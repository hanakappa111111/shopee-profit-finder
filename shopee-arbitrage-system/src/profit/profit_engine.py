"""Profit calculation engine for Shopee-Japan arbitrage system.

Handles currency conversion, fee calculation, profit analysis, and pricing
recommendations for cross-border e-commerce arbitrage.
"""

import time
from datetime import datetime, timedelta
from typing import Optional
import requests

from src.config.settings import settings
from src.database.models import MatchResult, ProfitResult
from src.utils.logger import logger


# Module-level cache for currency rates
_rate_cache = {"rate": None, "timestamp": None}


def get_php_to_jpy_rate() -> float:
    """Fetch PHP to JPY exchange rate with 1-hour caching.

    Calls exchangerate-api.com and caches the result. Falls back to 2.5
    if the API request fails.

    Returns:
        Exchange rate (PHP to JPY). Falls back to 2.5 if API unavailable.
    """
    global _rate_cache

    # Check if cache is still valid (within 1 hour)
    if _rate_cache["rate"] is not None and _rate_cache["timestamp"] is not None:
        if datetime.now() - _rate_cache["timestamp"] < timedelta(hours=1):
            return _rate_cache["rate"]

    try:
        response = requests.get(
            "https://api.exchangerate-api.com/v4/latest/PHP",
            timeout=5
        )
        response.raise_for_status()
        data = response.json()
        rate = data["rates"]["JPY"]
        _rate_cache["rate"] = rate
        _rate_cache["timestamp"] = datetime.now()
        logger.debug(f"Fetched PHP->JPY rate: {rate}")
        return rate
    except Exception as e:
        logger.warning(f"Failed to fetch exchange rate: {e}. Using fallback 2.5")
        return 2.5


class ProfitEngine:
    """Calculates profit and ROI for product arbitrage between markets.

    Attributes:
        fee_rate: Shopee fee percentage (0-1). Defaults to settings.SHOPEE_FEE_RATE.
        shipping_cost: Domestic shipping cost in JPY. Defaults to settings.DOMESTIC_SHIPPING_YEN.
        min_profit: Minimum profit threshold in JPY. Defaults to settings.MIN_PROFIT_YEN.
        min_roi: Minimum ROI threshold in percent. Defaults to settings.MIN_ROI_PERCENT.
    """

    def __init__(
        self,
        fee_rate: Optional[float] = None,
        shipping: Optional[float] = None,
        min_profit: Optional[float] = None,
        min_roi: Optional[float] = None
    ) -> None:
        """Initialize ProfitEngine with optional custom parameters.

        Args:
            fee_rate: Shopee platform fee rate (e.g., 0.05 for 5%). Defaults to settings.
            shipping: Domestic shipping cost in JPY. Defaults to settings.
            min_profit: Minimum profit in JPY. Defaults to settings.
            min_roi: Minimum ROI percentage. Defaults to settings.
        """
        self.fee_rate = fee_rate if fee_rate is not None else settings.SHOPEE_FEE_RATE
        self.shipping_cost = shipping if shipping is not None else settings.DOMESTIC_SHIPPING_YEN
        self.min_profit = min_profit if min_profit is not None else settings.MIN_PROFIT_YEN
        self.min_roi = min_roi if min_roi is not None else settings.MIN_ROI_PERCENT

    def calculate(self, match: MatchResult) -> ProfitResult:
        """Calculate profit and ROI for a matched product pair.

        Full formula:
        1. net_revenue_php = shopee_price * (1 - fee_rate)
        2. revenue_jpy = net_revenue_php * exchange_rate
        3. net_profit_jpy = revenue_jpy - japan_cost_jpy - shipping_cost_jpy
        4. roi_percent = (net_profit_jpy / japan_cost_jpy) * 100

        Args:
            match: MatchResult containing Shopee and Japan product prices.

        Returns:
            ProfitResult with full breakdown of costs and profit.
        """
        shopee_price = match.shopee_product.price_php
        japan_price = match.japan_product.price_jpy
        exchange_rate = get_php_to_jpy_rate()

        # Calculate net revenue after Shopee fees
        net_revenue_php = shopee_price * (1 - self.fee_rate)
        revenue_jpy = net_revenue_php * exchange_rate

        # Calculate net profit
        total_cost = japan_price + self.shipping_cost
        net_profit_jpy = revenue_jpy - total_cost

        # Calculate ROI
        roi_percent = (net_profit_jpy / japan_price * 100) if japan_price > 0 else 0

        breakdown = {
            "shopee_price_php": shopee_price,
            "fee_rate": self.fee_rate,
            "net_revenue_php": net_revenue_php,
            "exchange_rate": exchange_rate,
            "revenue_jpy": revenue_jpy,
            "japan_cost_jpy": japan_price,
            "shipping_cost_jpy": self.shipping_cost,
            "total_cost_jpy": total_cost,
            "net_profit_jpy": net_profit_jpy,
            "roi_percent": roi_percent
        }

        return ProfitResult(
            match_result=match,
            net_profit_jpy=net_profit_jpy,
            roi_percent=roi_percent,
            breakdown=breakdown
        )

    def calculate_many(
        self,
        matches: list[MatchResult]
    ) -> list[ProfitResult]:
        """Calculate profit for multiple matched products.

        Calculates profit for each match, sorts by profit descending,
        and logs summary of profitable vs non-profitable items.

        Args:
            matches: List of MatchResult objects to analyze.

        Returns:
            Sorted list of ProfitResult objects (descending by profit).
        """
        results = [self.calculate(match) for match in matches]
        results.sort(key=lambda x: x.net_profit_jpy, reverse=True)

        profitable = sum(1 for r in results if r.net_profit_jpy > 0)
        unprofitable = len(results) - profitable

        logger.info(
            f"Profit analysis: {profitable} profitable, "
            f"{unprofitable} non-profitable out of {len(results)} total"
        )

        return results

    def filter_profitable(self, results: list[ProfitResult]) -> list[ProfitResult]:
        """Filter results to only those meeting profit thresholds.

        Filters by both minimum profit (JPY) and minimum ROI (percent).

        Args:
            results: List of ProfitResult objects to filter.

        Returns:
            Filtered list of results meeting both thresholds.
        """
        filtered = [
            r for r in results
            if r.net_profit_jpy >= self.min_profit and
            r.roi_percent >= self.min_roi
        ]

        logger.info(
            f"Filtered to {len(filtered)} results meeting profit thresholds "
            f"(min {self.min_profit} JPY, min {self.min_roi}% ROI)"
        )

        return filtered

    def suggested_shopee_price(
        self,
        japan_price_jpy: float,
        target_roi: float = 30.0
    ) -> float:
        """Suggest minimum Shopee listing price to achieve target ROI.

        Reverse calculates the required PHP selling price to reach target ROI
        while covering Japan costs and shipping.

        Args:
            japan_price_jpy: Japan product cost in JPY.
            target_roi: Target ROI percentage. Defaults to 30.

        Returns:
            Suggested minimum Shopee listing price in PHP.
        """
        exchange_rate = get_php_to_jpy_rate()
        total_cost_jpy = japan_price_jpy + self.shipping_cost

        # Required revenue in JPY = cost * (1 + target_roi/100)
        required_revenue_jpy = total_cost_jpy * (1 + target_roi / 100)

        # Required revenue in PHP before fees
        required_revenue_php = required_revenue_jpy / exchange_rate

        # Required listing price (before fees)
        # net_revenue = listing_price * (1 - fee_rate)
        # so: listing_price = net_revenue / (1 - fee_rate)
        suggested_price_php = required_revenue_php / (1 - self.fee_rate)

        return round(suggested_price_php, 2)

    def format_report(self, result: ProfitResult) -> str:
        """Format a profit result as human-readable report string.

        Args:
            result: ProfitResult to format.

        Returns:
            Formatted report string with profit breakdown.
        """
        bd = result.breakdown
        report = (
            f"Profit Report: {result.match_result.shopee_product.title[:50]}...\n"
            f"  Shopee Price: ₱{bd['shopee_price_php']:.2f}\n"
            f"  Net Revenue (after fees): ₱{bd['net_revenue_php']:.2f}\n"
            f"  Revenue in JPY: ¥{bd['revenue_jpy']:.2f}\n"
            f"  Japan Cost: ¥{bd['japan_cost_jpy']:.2f}\n"
            f"  Shipping: ¥{bd['shipping_cost_jpy']:.2f}\n"
            f"  Total Cost: ¥{bd['total_cost_jpy']:.2f}\n"
            f"  Net Profit: ¥{result.net_profit_jpy:.2f}\n"
            f"  ROI: {result.roi_percent:.1f}%"
        )
        return report
