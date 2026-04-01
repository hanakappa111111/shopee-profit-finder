"""
Trend detection module for Shopee product analytics.

This module implements statistical trend analysis for e-commerce products,
calculating trend scores based on sales velocity, price stability, and
sales volume to identify rising, stable, and falling trends.
"""

from typing import List
from datetime import datetime, timedelta
from statistics import mean, stdev

from src.config.settings import settings
from src.database.models import TrendData, TrendDirection, ShopeeProduct
from src.database.database import db
from src.utils.logger import logger


class TrendDetector:
    """
    Statistical trend detection for product market analysis.

    Computes trend scores and direction based on sales velocity, price stability,
    and sales volume. Provides batch processing and filtering capabilities.

    Attributes:
        velocity_days (int): Window size in days for velocity calculation.
    """

    def __init__(self, velocity_days: int | None = None) -> None:
        """
        Initialize trend detector with optional velocity window.

        Args:
            velocity_days (int | None): Days window for velocity calculation.
                Defaults to settings.TREND_VELOCITY_DAYS if not provided.
        """
        self.velocity_days = velocity_days or settings.TREND_VELOCITY_DAYS
        logger.info(f"TrendDetector initialized with velocity_days={self.velocity_days}")

    def compute_trend(
        self, product_row: dict, price_history: list
    ) -> TrendData:
        """
        Compute trend data for a single product.

        Calculates sales velocity (sales per day), price stability (coefficient of
        variation), and a composite trend score. Assigns direction based on thresholds.

        Trend score formula:
        - velocity_score: min(sales_velocity / avg_daily_sales, 100)
        - stability_score: 1.0 - (std/mean of prices, clamped 0-1)
        - sales_score: min(sales_count / 100, 100)
        - trend_score = velocity_score × 0.40 + stability_score × 0.30 + sales_score × 0.30

        Direction thresholds:
        - RISING: trend_score > 60
        - FALLING: trend_score < 30
        - STABLE: 30 <= trend_score <= 60

        Args:
            product_row (dict): Product data with keys:
                - 'id': Product ID
                - 'name': Product name
                - 'sales_count': Total sales count
                - 'created_at': Product creation datetime
                - 'price': Current price
            price_history (list): Historical price data (list of float values).
                Empty list is handled as perfect stability (1.0).

        Returns:
            TrendData: Computed trend information including score and direction.
        """
        product_id = product_row.get("id")
        name = product_row.get("name", "Unknown")
        sales_count = product_row.get("sales_count", 0)
        created_at = product_row.get("created_at", datetime.utcnow())
        current_price = product_row.get("price", 0)

        # Calculate days since product was created
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        days_since_created = (datetime.utcnow() - created_at).days
        days_since_created = max(days_since_created, 1)  # At least 1 day

        # Calculate sales velocity (sales per day)
        sales_velocity = sales_count / days_since_created

        # Calculate price stability
        if not price_history or len(price_history) < 2:
            price_stability = 1.0
        else:
            try:
                price_mean = mean(price_history)
                if price_mean == 0:
                    price_stability = 1.0
                else:
                    price_std = stdev(price_history)
                    coefficient_of_variation = price_std / price_mean
                    # Clamp between 0 and 1
                    price_stability = max(0, min(1.0, 1.0 - coefficient_of_variation))
            except (ValueError, ZeroDivisionError):
                price_stability = 1.0

        # Calculate component scores (0-100 scale)
        # Velocity score: normalize to 100 (assuming 10 sales/day is excellent)
        velocity_score = min((sales_velocity / 10) * 100, 100) if sales_velocity > 0 else 0

        # Stability score: already 0-1, convert to 0-100
        stability_score = price_stability * 100

        # Sales score: normalize sales count (assuming 100 sales is excellent)
        sales_score = min((sales_count / 100) * 100, 100) if sales_count > 0 else 0

        # Weighted composite trend score
        trend_score = (
            velocity_score * 0.40 +
            stability_score * 0.30 +
            sales_score * 0.30
        )

        # Determine trend direction
        if trend_score > 60:
            direction = TrendDirection.RISING
        elif trend_score < 30:
            direction = TrendDirection.FALLING
        else:
            direction = TrendDirection.STABLE

        trend_data = TrendData(
            product_id=product_id,
            product_name=name,
            trend_score=round(trend_score, 2),
            direction=direction,
            sales_velocity=round(sales_velocity, 2),
            price_stability=round(price_stability, 4),
            computed_at=datetime.utcnow(),
        )

        logger.debug(
            f"Computed trend for {name}: "
            f"score={trend_score:.2f}, direction={direction.value}"
        )

        return trend_data

    def compute_all_trends(
        self, products: list
    ) -> List[TrendData]:
        """
        Compute trends for multiple products in batch.

        Processes each product, saves trends to database, and returns
        results sorted by trend_score in descending order.

        Args:
            products (list): List of product dictionaries with required keys:
                - 'id': Product ID
                - 'name': Product name
                - 'sales_count': Total sales count
                - 'created_at': Product creation datetime
                - 'price': Current price

        Returns:
            List[TrendData]: Computed trends sorted by score (highest first).
        """
        trends: List[TrendData] = []

        logger.info(f"Computing trends for {len(products)} products...")

        for product in products:
            try:
                # In a real system, you would fetch price_history from database
                price_history = []
                trend = self.compute_trend(product, price_history)
                trends.append(trend)

                # Save to database
                db.save_trend(trend)

            except Exception as e:
                logger.error(
                    f"Failed to compute trend for product {product.get('id')}: {e}"
                )
                continue

        # Sort by trend_score descending
        trends.sort(key=lambda t: t.trend_score, reverse=True)

        logger.info(
            f"Computed {len(trends)} trends. "
            f"Top trend: {trends[0].product_name if trends else 'N/A'}"
        )

        return trends

    def get_trending_products(
        self, min_score: float = 50.0, limit: int = 50
    ) -> List[dict]:
        """
        Retrieve trending products from database.

        Fetches the most recent trend data for products, filters by minimum
        trend score, and returns up to the specified limit.

        Args:
            min_score (float): Minimum trend score threshold (0-100).
                Defaults to 50.0.
            limit (int): Maximum number of products to return.
                Defaults to 50.

        Returns:
            List[dict]: List of trending product dictionaries with trend data.
                Sorted by trend_score in descending order.
        """
        logger.info(f"Fetching trending products (min_score={min_score}, limit={limit})")

        try:
            trends = db.get_latest_trends(limit=limit * 2)  # Fetch extra to filter
            filtered = [
                trend for trend in trends
                if trend.trend_score >= min_score
            ][:limit]

            logger.info(
                f"Retrieved {len(filtered)} trending products "
                f"(from {len(trends)} total)"
            )

            return [
                {
                    "product_id": t.product_id,
                    "product_name": t.product_name,
                    "trend_score": t.trend_score,
                    "direction": t.direction.value,
                    "sales_velocity": t.sales_velocity,
                    "price_stability": t.price_stability,
                    "computed_at": t.computed_at.isoformat(),
                }
                for t in filtered
            ]

        except Exception as e:
            logger.error(f"Failed to fetch trending products: {e}")
            return []


def detect_trends(products: list) -> List[TrendData]:
    """
    Convenience function to detect trends for a product list.

    Creates a TrendDetector instance and computes trends for all products
    in one call.

    Args:
        products (list): List of product dictionaries.

    Returns:
        List[TrendData]: Computed trends sorted by score (highest first).
    """
    detector = TrendDetector()
    return detector.compute_all_trends(products)
