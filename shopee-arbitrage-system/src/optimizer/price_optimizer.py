"""
Price Optimizer Module

Handles price optimization for Shopee listings based on market competition analysis.
Uses competitor pricing data to suggest undercut prices while maintaining profit margins.
"""

import asyncio
import json
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

from src.config.settings import settings
from src.database.database import db
from src.database.models import (
    CompetitorPrice,
    ListingStatus,
    PriceOptimizationResult,
    ShopeeListing,
)
from src.utils.logger import logger

# Shared headers for HTTP requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def _scrape_shopee_prices(keyword: str, limit: int = 20) -> list[CompetitorPrice]:
    """
    Scrape competitor prices from Shopee for a given keyword.

    Attempts to extract price data from the Shopee search page using static scraping.
    Falls back to partial data extraction from Next.js __NEXT_DATA__ if available.

    Args:
        keyword: Search keyword for Shopee
        limit: Maximum number of competitors to scrape (default: 20)

    Returns:
        List of CompetitorPrice objects saved to database
    """
    competitor_prices = []

    try:
        url = f"{settings.SHOPEE_BASE_URL}/search?keyword={keyword}"
        logger.info(f"Scraping Shopee prices for keyword: {keyword}")

        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")

        # Attempt to extract data from __NEXT_DATA__ script tag
        next_data_script = soup.find("script", id="__NEXT_DATA__")

        if next_data_script and next_data_script.string:
            try:
                data = json.loads(next_data_script.string)

                # Navigate through typical Next.js data structure for Shopee
                # This structure may vary; adjust path based on actual Shopee response
                if "props" in data and "pageProps" in data["props"]:
                    items = data["props"]["pageProps"].get("items", [])

                    for idx, item in enumerate(items[:limit]):
                        if "price" in item and "name" in item:
                            price = float(item["price"]) / 100000  # Shopee prices in small units
                            competitor = CompetitorPrice(
                                keyword=keyword,
                                price_php=price,
                                source="shopee_search",
                                item_id=item.get("itemid", "unknown"),
                                shop_name=item.get("shop_name", "Unknown"),
                            )
                            db.save_competitor_price(competitor)
                            competitor_prices.append(competitor)

                if competitor_prices:
                    logger.info(
                        f"Found {len(competitor_prices)} competitor prices for '{keyword}'"
                    )
                    return competitor_prices

            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.debug(f"Error parsing __NEXT_DATA__: {e}")

        # Fallback warning if no data extracted
        if not competitor_prices:
            logger.warning(
                f"Full competitor scraping requires Playwright; returning partial data for '{keyword}'"
            )

        return competitor_prices

    except requests.RequestException as e:
        logger.error(f"Failed to scrape Shopee prices for '{keyword}': {e}")
        return []

    except Exception as e:
        logger.error(f"Unexpected error scraping Shopee prices: {e}")
        return []


class PriceOptimizer:
    """
    Optimizes Shopee listing prices based on competitor data and profit margins.

    Computes optimal prices that undercut competitors while maintaining minimum
    profit margins. Provides market analysis and price suggestions.
    """

    def __init__(
        self, undercut_percent: Optional[float] = None, min_margin: Optional[float] = None
    ):
        """
        Initialize the PriceOptimizer with settings.

        Args:
            undercut_percent: Percentage to undercut competitors (default from settings)
            min_margin: Minimum profit margin in PHP (default from settings)
        """
        self._undercut = undercut_percent or settings.PRICE_UNDERCUT_PERCENT
        self._min_margin = min_margin or settings.MIN_MARGIN_PHP
        logger.info(
            f"PriceOptimizer initialized: undercut={self._undercut}%, min_margin=₱{self._min_margin}"
        )

    def compute_optimized_price(self, listing: dict, competitor_price: float) -> float:
        """
        Compute an optimized price that undercuts competitors while maintaining margin.

        Formula: optimized = competitor_price * (1 - undercut_percent / 100)
        Never goes below: estimated_japan_cost + MIN_MARGIN_PHP

        Args:
            listing: Dictionary containing listing data with 'profit_jpy' and 'price'
            competitor_price: Lowest competitor price in PHP

        Returns:
            Optimized price rounded to nearest 0.50 PHP
        """
        # Estimate Japan cost from listing profit data
        # profit_jpy = price_jpy - cost_jpy
        # Assuming profit_jpy is stored and price relationship exists
        japan_cost_estimate = 0
        if "profit_jpy" in listing and "price_jpy" in listing:
            japan_cost_estimate = listing.get("price_jpy", 0) - listing.get(
                "profit_jpy", 0
            )

        # Convert estimate to PHP (rough estimate: 1 JPY = 0.35 PHP)
        japan_cost_php = japan_cost_estimate * 0.35

        # Compute undercut price
        optimized = competitor_price * (1 - self._undercut / 100)

        # Floor: never below cost + minimum margin
        floor_price = japan_cost_php + self._min_margin

        final_price = max(optimized, floor_price)

        return self._round_price(final_price)

    def optimize_listing(self, listing_id: int) -> Optional[PriceOptimizationResult]:
        """
        Optimize a single listing based on competitor prices.

        Extracts keyword from listing title, scrapes competitor prices,
        and computes optimized price if beneficial.

        Args:
            listing_id: ID of the listing to optimize

        Returns:
            PriceOptimizationResult if optimization applied, None otherwise
        """
        try:
            # Retrieve listing from database
            listing = db.get_listing(listing_id)
            if not listing:
                logger.warning(f"Listing {listing_id} not found")
                return None

            # Extract keyword from title
            keyword = self._extract_keyword_from_title(listing.get("title", ""))
            if not keyword:
                logger.warning(f"Could not extract keyword from listing {listing_id}")
                return None

            # Scrape competitor prices
            time.sleep(settings.REQUEST_DELAY_SECONDS)
            _scrape_shopee_prices(keyword)

            # Get lowest competitor price
            competitor_price = db.get_lowest_competitor_price(keyword)
            if not competitor_price:
                logger.debug(f"No competitor prices found for keyword: {keyword}")
                return None

            current_price = listing.get("price_php", 0)

            # Only optimize if competitor is cheaper
            if competitor_price >= current_price:
                logger.debug(
                    f"Listing {listing_id}: no optimization needed "
                    f"(current=₱{current_price}, competitor=₱{competitor_price})"
                )
                return None

            # Compute optimized price
            optimized_price = self.compute_optimized_price(listing, competitor_price)

            # Log optimization result
            logger.info(
                f"Listing {listing_id}: suggest price ₱{current_price} -> ₱{optimized_price} "
                f"(competitor=₱{competitor_price})"
            )

            # Save to database
            result = PriceOptimizationResult(
                listing_id=listing_id,
                keyword=keyword,
                current_price=current_price,
                competitor_price=competitor_price,
                optimized_price=optimized_price,
                margin_php=optimized_price - (listing.get("cost_estimate_php", 0)),
            )
            db.log_optimization(result)

            return result

        except Exception as e:
            logger.error(f"Error optimizing listing {listing_id}: {e}")
            return None

    def optimize_all_active_listings(self, apply: bool = False) -> list[PriceOptimizationResult]:
        """
        Optimize all active listings in the database.

        Args:
            apply: If True, apply prices to listings; if False, dry-run only

        Returns:
            List of optimization results
        """
        try:
            logger.info("Starting optimization of all active listings")
            results = []

            # Get all active listings
            active_listings = db.get_listings_by_status(ListingStatus.ACTIVE)
            logger.info(f"Found {len(active_listings)} active listings")

            for listing in active_listings:
                result = self.optimize_listing(listing["id"])
                if result:
                    results.append(result)

                    # Apply price if requested
                    if apply:
                        from src.listing_manager import listing_manager

                        listing_manager.update_price(listing["id"], result.optimized_price)
                        logger.info(f"Applied price ₱{result.optimized_price} to listing {listing['id']}")

            logger.info(
                f"Optimization complete: {len(results)} listings optimized "
                f"(apply={apply})"
            )

            return results

        except Exception as e:
            logger.error(f"Error optimizing all listings: {e}")
            return []

    def analyze_market_prices(self, keyword: str) -> dict:
        """
        Analyze market prices for a given keyword.

        Scrapes competitor prices and computes market statistics.

        Args:
            keyword: Search keyword to analyze

        Returns:
            Dictionary with price statistics (min, max, avg, median, count)
        """
        try:
            logger.info(f"Analyzing market prices for keyword: {keyword}")

            # Scrape competitor prices
            time.sleep(settings.REQUEST_DELAY_SECONDS)
            competitor_prices = _scrape_shopee_prices(keyword)

            if not competitor_prices:
                logger.warning(f"No competitor data for market analysis: {keyword}")
                return {
                    "keyword": keyword,
                    "min_price": None,
                    "max_price": None,
                    "avg_price": None,
                    "median_price": None,
                    "num_competitors": 0,
                }

            prices = [cp.price_php for cp in competitor_prices]
            prices_sorted = sorted(prices)

            analysis = {
                "keyword": keyword,
                "min_price": min(prices),
                "max_price": max(prices),
                "avg_price": sum(prices) / len(prices),
                "median_price": prices_sorted[len(prices_sorted) // 2],
                "num_competitors": len(competitor_prices),
            }

            logger.info(f"Market analysis for '{keyword}': {analysis}")
            return analysis

        except Exception as e:
            logger.error(f"Error analyzing market prices for '{keyword}': {e}")
            return {
                "keyword": keyword,
                "min_price": None,
                "max_price": None,
                "avg_price": None,
                "median_price": None,
                "num_competitors": 0,
            }

    def _extract_keyword_from_title(self, title: str) -> str:
        """
        Extract first 3 meaningful words from listing title.

        Skips common stopwords like 'the', 'a', 'is', etc.

        Args:
            title: Product title string

        Returns:
            Extracted keyword (first 3 meaningful words)
        """
        stopwords = {
            "the",
            "a",
            "an",
            "is",
            "are",
            "was",
            "were",
            "been",
            "be",
            "have",
            "has",
            "had",
            "do",
            "does",
            "did",
            "will",
            "would",
            "could",
            "should",
            "may",
            "might",
            "must",
            "can",
            "and",
            "or",
            "but",
            "in",
            "on",
            "at",
            "to",
            "for",
            "of",
            "with",
            "by",
            "from",
        }

        words = [w.lower() for w in title.split() if w.lower() not in stopwords]
        keyword = " ".join(words[:3])

        return keyword if keyword else title[:20]

    def _round_price(self, price: float) -> float:
        """
        Round price to nearest 0.50 PHP.

        Args:
            price: Price to round

        Returns:
            Price rounded to nearest 0.50
        """
        return round(price * 2) / 2
