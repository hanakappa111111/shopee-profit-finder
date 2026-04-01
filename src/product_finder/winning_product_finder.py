"""
Module to identify 'winning' products from scraped Shopee data.

This module provides functionality to analyze Shopee products and identify
those with high potential for arbitrage based on sales, ratings, pricing,
and trend data.
"""

from typing import Optional
from loguru import logger

from src.config.settings import settings
from src.database.models import ShopeeProduct, WinningProduct, TrendData
from src.database.database import db
from src.utils.logger import logger as app_logger


class WinningProductFinder:
    """
    Identifies winning products from Shopee data based on configurable criteria.

    A winning product is one that meets minimum thresholds for sales, rating, and price,
    and is scored based on a weighted composite of multiple factors.
    """

    def __init__(
        self,
        min_sales: Optional[int] = None,
        min_rating: Optional[float] = None,
        min_price: Optional[float] = None,
    ) -> None:
        """
        Initialize the WinningProductFinder with threshold values.

        Args:
            min_sales: Minimum sales count threshold. Defaults to settings.MIN_SALES_COUNT.
            min_rating: Minimum rating threshold (0-5). Defaults to settings.MIN_RATING.
            min_price: Minimum price threshold in PHP. Defaults to settings.MIN_PRICE_PHP.
        """
        self.min_sales = min_sales if min_sales is not None else settings.MIN_SALES_COUNT
        self.min_rating = min_rating if min_rating is not None else settings.MIN_RATING
        self.min_price = min_price if min_price is not None else settings.MIN_PRICE_PHP

        app_logger.info(
            f"WinningProductFinder initialized with thresholds: "
            f"min_sales={self.min_sales}, min_rating={self.min_rating}, min_price={self.min_price}"
        )

    def is_winner(
        self,
        product_dict: dict,
        trend: Optional[TrendData] = None,
    ) -> tuple[bool, list[str]]:
        """
        Determine if a product is a winner based on criteria.

        Args:
            product_dict: Dictionary containing product data with keys: sales, rating, price, seller_info.
            trend: Optional TrendData object for trend scoring.

        Returns:
            Tuple of (is_winner: bool, reasons: list[str]) where reasons lists why it's a winner
            or why it failed to qualify.
        """
        reasons = []

        # Check sales threshold
        sales = product_dict.get("sales", 0)
        if sales < self.min_sales:
            reasons.append(f"Insufficient sales: {sales} < {self.min_sales}")
            return False, reasons

        # Check rating threshold
        rating = product_dict.get("rating", 0)
        if rating < self.min_rating:
            reasons.append(f"Low rating: {rating} < {self.min_rating}")
            return False, reasons

        # Check price threshold
        price = product_dict.get("price", 0)
        if price < self.min_price:
            reasons.append(f"Price too low: {price} < {self.min_price}")
            return False, reasons

        # Product passed base criteria
        reasons.append(f"Sales qualified: {sales} >= {self.min_sales}")
        reasons.append(f"Rating qualified: {rating} >= {self.min_rating}")
        reasons.append(f"Price qualified: {price} >= {self.min_price}")

        # Bonus check: trend scoring
        if trend is not None and trend.trend_score > 60:
            reasons.append(f"Strong trend signal: {trend.trend_score}")

        # Malus check: empty seller info (slightly lower threshold applied in scoring)
        seller_info = product_dict.get("seller_info", {})
        if not seller_info or len(seller_info) == 0:
            reasons.append("Warning: Empty seller information")

        return True, reasons

    def compute_win_score(
        self,
        product_dict: dict,
        trend: Optional[TrendData] = None,
    ) -> float:
        """
        Compute a weighted composite win score for a product.

        Weights:
            - Sales score: 35%
            - Rating score: 25%
            - Price score: 20%
            - Trend score: 20%

        Args:
            product_dict: Dictionary containing product data.
            trend: Optional TrendData object for trend scoring.

        Returns:
            Composite win score (0-100).
        """
        # Sales score: min(sales / 1000, 1.0) * 100
        sales = product_dict.get("sales", 0)
        sales_score = min(sales / 1000, 1.0) * 100

        # Rating score: (rating / 5.0) * 100
        rating = product_dict.get("rating", 0)
        rating_score = (rating / 5.0) * 100

        # Price score: min(price / 5000, 1.0) * 100
        price = product_dict.get("price", 0)
        price_score = min(price / 5000, 1.0) * 100

        # Trend score: use trend.trend_score if provided, else 50.0
        trend_score = trend.trend_score if trend is not None else 50.0

        # Weighted composite
        win_score = (
            (sales_score * 0.35)
            + (rating_score * 0.25)
            + (price_score * 0.20)
            + (trend_score * 0.20)
        )

        return win_score

    def find_winners(
        self,
        products: list[dict],
        trends: Optional[list[TrendData]] = None,
    ) -> list[WinningProduct]:
        """
        Find winning products from a list of products.

        Args:
            products: List of product dictionaries.
            trends: Optional list of TrendData objects keyed by product ID.

        Returns:
            List of WinningProduct objects sorted by win_score in descending order.
        """
        winners = []
        trends_by_product = {trend.product_id: trend for trend in (trends or [])}

        for product_dict in products:
            product_id = product_dict.get("product_id")
            trend = trends_by_product.get(product_id)

            is_winner, reasons = self.is_winner(product_dict, trend)

            if is_winner:
                win_score = self.compute_win_score(product_dict, trend)

                # Create WinningProduct instance
                winning_product = WinningProduct(
                    product_id=product_id,
                    product=product_dict,
                    win_score=win_score,
                    reasons=reasons,
                )
                winners.append(winning_product)

        # Sort by win_score descending
        winners.sort(key=lambda w: w.win_score, reverse=True)

        app_logger.info(f"Found {len(winners)} winning products from {len(products)} total products")

        return winners

    def find_high_margin_candidates(
        self,
        min_score: float = 60.0,
    ) -> list[WinningProduct]:
        """
        Find high margin candidate products from the database.

        Retrieves products meeting base criteria, scores them, and returns top results.

        Args:
            min_score: Minimum win score threshold (default: 60.0).

        Returns:
            List of WinningProduct objects with score >= min_score, sorted by score descending.
        """
        # Get products from database meeting base criteria
        products = db.get_products(
            min_sales=self.min_sales,
            min_rating=self.min_rating,
            min_price=self.min_price,
        )

        # Find winners and filter by score
        all_winners = self.find_winners(products)
        high_margin_candidates = [w for w in all_winners if w.win_score >= min_score]

        app_logger.info(
            f"Found {len(high_margin_candidates)} high margin candidates "
            f"(score >= {min_score}) from {len(all_winners)} winners"
        )

        return high_margin_candidates


def find_winning_products() -> list[WinningProduct]:
    """
    Module-level function to find winning products using default settings.

    Convenience function that instantiates a WinningProductFinder and finds
    high margin candidates.

    Returns:
        List of WinningProduct objects sorted by win_score in descending order.
    """
    finder = WinningProductFinder()
    return finder.find_high_margin_candidates()
