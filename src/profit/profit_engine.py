"""Profit calculation engine for Shopee-Japan arbitrage system.

Handles currency conversion, fee calculation, profit analysis, and pricing
recommendations for cross-border e-commerce arbitrage.

Formula
-------
    shopee_fee      = shopee_price × fee_rate
    net_revenue_php = shopee_price - shopee_fee
    net_revenue_jpy = net_revenue_php × exchange_rate
    cost_jpy        = supplier_price + domestic_shipping_cost
    profit          = net_revenue_jpy - cost_jpy - safety_margin   (JPY)
    roi             = profit / cost_jpy                            (decimal, e.g. 0.30)

A result is *profitable* when both conditions hold:
    profit >= min_profit_jpy   AND   roi >= min_roi  (decimal)
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, List, Optional

import requests

from src.config.settings import settings
from src.database.models import (
    MatchConfidence,
    MatchResult,
    ProfitAnalysis,
    ProfitResult,
)
from src.utils.logger import logger

if TYPE_CHECKING:
    from src.database.database import Database


# ── Multi-currency support ───────────────────────────────────────────────────
# Maps SHOPEE_MARKET codes to (currency_code, fallback_rate_to_JPY).
# The fallback rate is a conservative estimate used when the API is unreachable.

_MARKET_CURRENCY: dict[str, tuple[str, float]] = {
    "PH": ("PHP", 2.5),     # 1 PHP ≈ 2.5 JPY
    "SG": ("SGD", 113.0),   # 1 SGD ≈ 113 JPY
    "MY": ("MYR", 33.0),    # 1 MYR ≈ 33 JPY
}

# ── Module-level exchange-rate cache ─────────────────────────────────────────

_rate_cache: dict = {}  # "PHP" → {"rate": float, "timestamp": datetime}


def get_local_to_jpy_rate(currency: str | None = None) -> float:
    """Return *local currency* → JPY exchange rate.

    The currency defaults to the one implied by ``settings.SHOPEE_MARKET``.
    Rates are cached for 1 hour per currency code.

    Falls back to a hard-coded conservative rate when the API is unreachable.
    """
    if currency is None:
        market = settings.SHOPEE_MARKET.upper()
        currency, fallback = _MARKET_CURRENCY.get(market, ("PHP", 2.5))
    else:
        fallback = dict((v[0], v[1]) for v in _MARKET_CURRENCY.values()).get(currency, 2.5)

    cached = _rate_cache.get(currency)
    if (
        cached is not None
        and cached["rate"] is not None
        and datetime.now() - cached["timestamp"] < timedelta(hours=1)
    ):
        return cached["rate"]

    try:
        response = requests.get(
            f"https://api.exchangerate-api.com/v4/latest/{currency}",
            timeout=5,
        )
        response.raise_for_status()
        rate: float = response.json()["rates"]["JPY"]
        _rate_cache[currency] = {"rate": rate, "timestamp": datetime.now()}
        logger.debug(f"Fetched {currency}→JPY rate: {rate}")
        return rate
    except Exception as exc:
        logger.warning(
            f"Failed to fetch {currency}→JPY rate: {exc}. "
            f"Using fallback {fallback}"
        )
        return fallback


# Backward-compatible alias
def get_php_to_jpy_rate() -> float:
    """Legacy alias — returns the rate for the configured market currency."""
    return get_local_to_jpy_rate()


# ── ProfitEngine ──────────────────────────────────────────────────────────────

class ProfitEngine:
    """Calculates profit and ROI for Shopee ↔ Japan arbitrage pairs.

    Parameters
    ----------
    fee_rate:
        Shopee platform fee as a decimal fraction (e.g. 0.17 for 17 %).
        Defaults to ``settings.SHOPEE_FEE_RATE``.
    shipping:
        Japan domestic shipping cost in JPY.
        Defaults to ``settings.DOMESTIC_SHIPPING_YEN``.
    safety_margin:
        Additional JPY buffer deducted from gross profit before the
        profitability test.  Acts as a reserve for micro-costs.
        Defaults to ``settings.SAFETY_MARGIN_YEN``.
    min_profit:
        Minimum acceptable profit in JPY.
        Defaults to ``settings.MIN_PROFIT_YEN``.
    min_roi:
        Minimum acceptable ROI as a decimal fraction (e.g. 0.30 = 30 %).
        Defaults to ``settings.MIN_ROI``.
    """

    def __init__(
        self,
        fee_rate: Optional[float] = None,
        shipping: Optional[float] = None,
        safety_margin: Optional[float] = None,
        min_profit: Optional[float] = None,
        min_roi: Optional[float] = None,
    ) -> None:
        self.fee_rate = fee_rate if fee_rate is not None else settings.SHOPEE_FEE_RATE
        self.shipping_cost = (
            shipping if shipping is not None else settings.DOMESTIC_SHIPPING_YEN
        )
        self.safety_margin = (
            safety_margin if safety_margin is not None else settings.SAFETY_MARGIN_YEN
        )
        self.min_profit = (
            min_profit if min_profit is not None else settings.MIN_PROFIT_YEN
        )
        # min_roi stored as decimal (0–1) matching settings.MIN_ROI convention.
        self.min_roi = min_roi if min_roi is not None else settings.MIN_ROI

    # ── Core calculation ──────────────────────────────────────────────────────

    def calculate(self, match: MatchResult) -> ProfitResult:
        """Calculate profit and ROI for one matched product pair.

        Parameters
        ----------
        match:
            A validated ``MatchResult`` from the matching engine.

        Returns
        -------
        ProfitResult
            Full profit breakdown.  ``is_profitable`` is set when both
            the profit and ROI thresholds are met.
        """
        # Bug fix 1: field is `.price`, not `.price_php`
        shopee_price: float = match.shopee_product.price
        supplier_price: float = match.japan_product.price_jpy
        exchange_rate: float = get_php_to_jpy_rate()

        # Revenue
        shopee_fee = shopee_price * self.fee_rate
        net_revenue_php = shopee_price - shopee_fee
        net_revenue_jpy = net_revenue_php * exchange_rate

        # Cost
        cost_jpy = supplier_price + self.shipping_cost

        # Profit (after optional safety margin)
        profit = net_revenue_jpy - cost_jpy - self.safety_margin

        # Bug fix 2: ROI denominator must be total cost, not supplier price alone.
        roi = (profit / cost_jpy) if cost_jpy > 0 else 0.0

        is_profitable = (
            profit >= self.min_profit and roi >= self.min_roi
        )

        breakdown = {
            "shopee_price": shopee_price,
            "shopee_fee": shopee_fee,
            "fee_rate": self.fee_rate,
            "net_revenue_php": net_revenue_php,
            "exchange_rate": exchange_rate,
            "net_revenue_jpy": net_revenue_jpy,
            "supplier_price_jpy": supplier_price,
            "shipping_cost_jpy": self.shipping_cost,
            "safety_margin_jpy": self.safety_margin,
            "cost_jpy": cost_jpy,
            "profit_jpy": profit,
            "roi": roi,
            "roi_percent": roi * 100,
        }

        # Bug fix 3: ProfitResult fields are shopee_product / japan_product /
        # profit_jpy / roi_percent / is_profitable — no match_result, no net_profit_jpy.
        return ProfitResult(
            shopee_product=match.shopee_product,
            japan_product=match.japan_product,
            similarity_score=match.similarity_score,
            match_method=match.match_method,
            confidence_level=match.confidence_level,
            profit_jpy=profit,                # Bug fix 3b: correct field name
            roi_percent=roi * 100,
            is_profitable=is_profitable,      # Bug fix 9: populate is_profitable
            breakdown=breakdown,
        )

    # ── Batch helpers ─────────────────────────────────────────────────────────

    def calculate_many(self, matches: List[MatchResult]) -> List[ProfitResult]:
        """Calculate profit for a list of matches, sorted by profit descending."""
        results = [self.calculate(m) for m in matches]
        # Bug fix 4: field is profit_jpy, not net_profit_jpy
        results.sort(key=lambda x: x.profit_jpy, reverse=True)

        # Bug fix 5: field is profit_jpy
        profitable = sum(1 for r in results if r.profit_jpy > 0)
        logger.info(
            f"Profit analysis: {profitable} profitable, "
            f"{len(results) - profitable} non-profitable "
            f"out of {len(results)} total"
        )
        return results

    def filter_profitable(self, results: List[ProfitResult]) -> List[ProfitResult]:
        """Return only results that meet both profit and ROI thresholds."""
        filtered = [
            r for r in results
            # Bug fix 6: field is profit_jpy; compare roi_percent against min_roi*100
            if r.profit_jpy >= self.min_profit
            and (r.roi_percent / 100) >= self.min_roi
        ]
        logger.info(
            f"Filtered to {len(filtered)} profitable results "
            f"(min ¥{self.min_profit:,.0f}, min {self.min_roi * 100:.0f}% ROI)"
        )
        return filtered

    # ── Persistence ───────────────────────────────────────────────────────────

    def save_analysis(
        self,
        result: ProfitResult,
        shopee_product_id: int,
        japan_product_id: int,
        db: "Database",
    ) -> ProfitAnalysis:
        """Persist a ``ProfitResult`` to the ``profit_analysis`` table.

        The row is upserted (overwritten on re-run) so the table always holds
        the latest calculation for each (shopee_product_id, japan_product_id)
        pair.

        Parameters
        ----------
        result:
            Output from :meth:`calculate`.
        shopee_product_id:
            Primary key of the corresponding row in ``products``.
        japan_product_id:
            Primary key of the corresponding row in ``sources``.
        db:
            Open :class:`~src.database.database.Database` instance.

        Returns
        -------
        ProfitAnalysis
            The model that was written to the database.
        """
        bd = result.breakdown
        analysis = ProfitAnalysis(
            shopee_product_id=shopee_product_id,
            japan_product_id=japan_product_id,
            supplier_price=bd["supplier_price_jpy"],
            domestic_shipping_cost=bd["shipping_cost_jpy"],
            safety_margin=bd["safety_margin_jpy"],
            shopee_price=bd["shopee_price"],
            shopee_fee=bd["shopee_fee"],
            fee_rate=bd["fee_rate"],
            exchange_rate=bd["exchange_rate"],
            net_revenue_jpy=bd["net_revenue_jpy"],
            cost_jpy=bd["cost_jpy"],
            profit=bd["profit_jpy"],
            roi=bd["roi"],
            is_profitable=result.is_profitable,
            match_method=result.match_method,
            confidence_level=(
                result.confidence_level.value
                if isinstance(result.confidence_level, MatchConfidence)
                else str(result.confidence_level)
            ),
            similarity_score=result.similarity_score,
            analyzed_at=datetime.utcnow(),
        )
        db.save_profit_analysis(analysis)
        return analysis

    # ── Pricing helper ────────────────────────────────────────────────────────

    def suggested_shopee_price(
        self,
        japan_price_jpy: float,
        target_roi: float = 0.30,
    ) -> float:
        """Return the minimum PHP listing price to achieve *target_roi*.

        Parameters
        ----------
        japan_price_jpy:
            Supplier cost in JPY.
        target_roi:
            Target ROI as a decimal fraction (default 0.30 = 30 %).

        Returns
        -------
        float
            Minimum suggested Shopee listing price in PHP.
        """
        exchange_rate = get_php_to_jpy_rate()
        cost_jpy = japan_price_jpy + self.shipping_cost + self.safety_margin

        # Required net revenue in JPY to hit target ROI
        required_revenue_jpy = cost_jpy * (1 + target_roi)

        # Convert to PHP then gross up for platform fee
        required_revenue_php = required_revenue_jpy / exchange_rate
        suggested_price_php = required_revenue_php / (1 - self.fee_rate)

        return round(suggested_price_php, 2)

    # ── Reporting ─────────────────────────────────────────────────────────────

    def format_report(self, result: ProfitResult) -> str:
        """Return a human-readable profit report for *result*."""
        bd = result.breakdown
        # Bug fix 7+8: access shopee_product directly; use profit_jpy
        title = result.shopee_product.title  # Bug fix 7: no match_result wrapper
        currency_symbol = "₱"
        return (
            f"Profit Report: {title[:50]}...\n"
            f"  Shopee Price:          {currency_symbol}{bd['shopee_price']:.2f}\n"
            f"  Shopee Fee ({bd['fee_rate']*100:.0f}%):     "
            f"{currency_symbol}{bd['shopee_fee']:.2f}\n"
            f"  Net Revenue (PHP):     {currency_symbol}{bd['net_revenue_php']:.2f}\n"
            f"  Net Revenue (JPY):     ¥{bd['net_revenue_jpy']:.2f}  "
            f"(rate {bd['exchange_rate']:.4f})\n"
            f"  Supplier Cost:         ¥{bd['supplier_price_jpy']:.2f}\n"
            f"  Domestic Shipping:     ¥{bd['shipping_cost_jpy']:.2f}\n"
            f"  Safety Margin:         ¥{bd['safety_margin_jpy']:.2f}\n"
            f"  Total Cost:            ¥{bd['cost_jpy']:.2f}\n"
            f"  Net Profit:            ¥{result.profit_jpy:.2f}\n"  # Bug fix 8
            f"  ROI:                   {result.roi_percent:.1f}%\n"
            f"  Profitable:            {'✓ YES' if result.is_profitable else '✗ NO'}"
        )
