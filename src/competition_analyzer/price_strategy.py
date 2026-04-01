"""Competition Analyzer AI — Price Strategy Engine.

Computes an optimal Shopee listing price from:

1. A :class:`~src.competition_analyzer.price_analysis.PriceDistribution`
   describing the current market.
2. The product's cost structure (supplier_price, shipping, safety_margin,
   exchange_rate, fee_rate) — read from ``profit_analysis`` or supplied
   directly — which defines the **price floor**.

Strategy hierarchy
------------------
MEDIAN_MINUS_DISCOUNT (default)
    ``recommended = median_price − COMPETITION_MEDIAN_DISCOUNT_PHP``
    Applied when ``competitor_count >= COMPETITION_MIN_COMPETITORS``.
    Guarantees we sit just below the market middle, maximising conversions
    while maintaining healthy margins.

BELOW_MIN
    ``recommended = min_market_price − 1``  (smallest possible undercut)
    Applied when we have data but fewer than ``COMPETITION_MIN_COMPETITORS``
    results — a thin market where matching the lowest price makes sense.

FLOOR_ONLY
    ``recommended = min_viable_price × (1 + FLOOR_BUFFER_PCT)``
    Applied when no competitor data is available at all.  We set the price
    just above our cost floor to attract buyers while staying profitable.

In all cases the final recommendation is **clamped to the floor** so we
never generate a price that violates profit / ROI thresholds.

Price floor calculation
-----------------------
The floor is the minimum PHP price at which the listing is still profitable
given the current exchange rate, costs, and Shopee fee.

    net_revenue_required_jpy = cost_jpy + safety_margin + min_profit_jpy
    min_php_before_fee       = net_revenue_required_jpy / exchange_rate
    floor_php                = min_php_before_fee / (1 − fee_rate)

Additionally the floor is raised if the calculated price yields ROI < min_roi:

    min_php_for_roi         = cost_jpy × (1 + min_roi) + safety_margin
                             (converted to PHP the same way)
    floor_php               = max(floor_php, min_php_for_roi_php)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from src.config.settings import settings
from src.database.models import PriceRecommendation, PriceStrategy
from src.competition_analyzer.price_analysis import PriceDistribution
from src.utils.logger import logger


# Buffer above floor when no competitor data exists (5 %)
_FLOOR_BUFFER_PCT: float = 0.05

# Minimum price rounding granularity (PHP)
_PRICE_GRANULARITY: float = 1.0


@dataclass
class StrategyInput:
    """All inputs required to compute a price recommendation."""

    shopee_product_id: int
    product_key: Optional[str]

    # Cost structure (from profit_analysis or supplied directly)
    supplier_price_jpy: float         # Japan source price
    domestic_shipping_jpy: float      # Japan domestic shipping
    safety_margin_jpy: float          # Extra buffer
    exchange_rate: float              # PHP→JPY rate used for floor

    # Profit engine thresholds (default to settings if not provided)
    fee_rate: float
    min_profit_jpy: float
    min_roi: float                    # decimal (e.g. 0.30)

    # Median discount override (default to settings)
    median_discount_php: Optional[float] = None
    min_competitors: Optional[int] = None


class PriceStrategyEngine:
    """Compute an optimal listing price from market data and cost constraints.

    Parameters
    ----------
    median_discount_php:
        Override for the per-product discount below median.
    min_competitors:
        Minimum competitor count to apply the MEDIAN_MINUS_DISCOUNT strategy.
    """

    def __init__(
        self,
        median_discount_php: Optional[float] = None,
        min_competitors: Optional[int] = None,
    ) -> None:
        self._median_discount = (
            median_discount_php
            if median_discount_php is not None
            else settings.COMPETITION_MEDIAN_DISCOUNT_PHP
        )
        self._min_competitors = min_competitors or settings.COMPETITION_MIN_COMPETITORS

    # ── Public API ────────────────────────────────────────────────────────────

    def recommend(
        self,
        inp: StrategyInput,
        distribution: Optional[PriceDistribution],
    ) -> PriceRecommendation:
        """Compute a :class:`PriceRecommendation`.

        Parameters
        ----------
        inp:
            Cost / threshold inputs for this product.
        distribution:
            Price distribution from ``price_analysis.analyse_prices()``.
            Pass ``None`` when no competitor data is available.

        Returns
        -------
        PriceRecommendation
        """
        floor = self._compute_floor(inp)

        if distribution is None or distribution.count == 0:
            rec_price, strategy, note = self._strategy_floor_only(floor)
            dist_fields = self._empty_dist_fields()
        elif distribution.count >= self._min_competitors:
            rec_price, strategy, note = self._strategy_median_minus_discount(
                floor, distribution
            )
            dist_fields = self._dist_fields(distribution)
        else:
            rec_price, strategy, note = self._strategy_below_min(floor, distribution)
            dist_fields = self._dist_fields(distribution)

        # Always clamp to floor
        rec_price = max(rec_price, floor)
        rec_price = _round_price(rec_price)

        logger.debug(
            f"[PriceStrategy] product_id={inp.shopee_product_id} "
            f"strategy={strategy.value} floor={floor:.2f} rec={rec_price:.2f} "
            f"n_competitors={distribution.count if distribution else 0}"
        )

        return PriceRecommendation(
            shopee_product_id=inp.shopee_product_id,
            product_key=inp.product_key,
            competitor_count=distribution.count if distribution else 0,
            min_market_price=dist_fields["min"],
            median_market_price=dist_fields["median"],
            max_market_price=dist_fields["max"],
            recommended_price=rec_price,
            min_viable_price=round(floor, 4),
            strategy_used=strategy,
            strategy_note=note,
            calculated_at=datetime.utcnow(),
        )

    def compute_floor(self, inp: StrategyInput) -> float:
        """Expose floor calculation for external use (e.g. reporting)."""
        return self._compute_floor(inp)

    # ── Strategy implementations ──────────────────────────────────────────────

    def _strategy_median_minus_discount(
        self,
        floor: float,
        dist: PriceDistribution,
    ) -> tuple:
        candidate = dist.median_price - self._median_discount
        note = (
            f"Median ₱{dist.median_price:.2f} − discount ₱{self._median_discount:.2f}"
            f" = ₱{candidate:.2f}  (n={dist.count} competitors)"
        )
        return candidate, PriceStrategy.MEDIAN_MINUS_DISCOUNT, note

    def _strategy_below_min(
        self,
        floor: float,
        dist: PriceDistribution,
    ) -> tuple:
        # 1 PHP undercut of the current cheapest competitor
        candidate = dist.min_price - 1.0
        note = (
            f"Min market ₱{dist.min_price:.2f} − 1.00"
            f"  (only {dist.count} competitors; below threshold {self._min_competitors})"
        )
        return candidate, PriceStrategy.BELOW_MIN, note

    @staticmethod
    def _strategy_floor_only(floor: float) -> tuple:
        candidate = floor * (1 + _FLOOR_BUFFER_PCT)
        note = (
            f"No competitor data — floor ₱{floor:.2f} + {_FLOOR_BUFFER_PCT*100:.0f}% buffer"
            f" = ₱{candidate:.2f}"
        )
        return candidate, PriceStrategy.FLOOR_ONLY, note

    # ── Price floor ───────────────────────────────────────────────────────────

    @staticmethod
    def _compute_floor(inp: StrategyInput) -> float:
        """Return the minimum PHP price that still meets profit AND ROI thresholds."""
        # Cost and revenue pipeline (mirrors profit_engine formula):
        #   cost_jpy        = supplier_price + domestic_shipping
        #   net_rev_needed  = cost_jpy + safety_margin + min_profit_jpy
        #   min_php (before fee) = net_rev_needed / exchange_rate
        #   floor_php       = min_php / (1 - fee_rate)

        if inp.exchange_rate <= 0:
            return 0.0

        cost_jpy = inp.supplier_price_jpy + inp.domestic_shipping_jpy

        # Floor from minimum profit constraint
        net_rev_needed_jpy = cost_jpy + inp.safety_margin_jpy + inp.min_profit_jpy
        min_php_profit = net_rev_needed_jpy / inp.exchange_rate
        floor_from_profit = (
            min_php_profit / (1.0 - inp.fee_rate)
            if inp.fee_rate < 1.0 else min_php_profit
        )

        # Floor from minimum ROI constraint
        #   profit_needed = cost_jpy × min_roi
        #   net_rev_needed_roi = cost_jpy + safety_margin + cost_jpy × min_roi
        profit_needed_jpy = cost_jpy * inp.min_roi
        net_rev_needed_roi_jpy = cost_jpy + inp.safety_margin_jpy + profit_needed_jpy
        min_php_roi = net_rev_needed_roi_jpy / inp.exchange_rate
        floor_from_roi = (
            min_php_roi / (1.0 - inp.fee_rate)
            if inp.fee_rate < 1.0 else min_php_roi
        )

        return max(floor_from_profit, floor_from_roi, 0.0)

    # ── Distribution field helpers ────────────────────────────────────────────

    @staticmethod
    def _dist_fields(dist: PriceDistribution) -> Dict[str, float]:
        return {
            "min":    dist.min_price,
            "median": dist.median_price,
            "max":    dist.max_price,
        }

    @staticmethod
    def _empty_dist_fields() -> Dict[str, float]:
        return {"min": 0.0, "median": 0.0, "max": 0.0}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _round_price(price: float) -> float:
    """Round to _PRICE_GRANULARITY (1 PHP) towards ceiling."""
    return math.ceil(price / _PRICE_GRANULARITY) * _PRICE_GRANULARITY


def build_strategy_input_from_profit_row(
    profit_row: Dict[str, Any],
    shopee_product_id: int,
    product_key: Optional[str] = None,
) -> StrategyInput:
    """Build a :class:`StrategyInput` from a ``profit_analysis`` DB row dict.

    Parameters
    ----------
    profit_row:
        Row dict from ``Database.get_profit_analysis_for_product()`` or similar.
    shopee_product_id:
        ``products.id`` value.
    product_key:
        Optional product key for traceability.
    """
    return StrategyInput(
        shopee_product_id=shopee_product_id,
        product_key=product_key,
        supplier_price_jpy=float(profit_row.get("supplier_price", 0)),
        domestic_shipping_jpy=float(profit_row.get("domestic_shipping_cost", settings.DOMESTIC_SHIPPING_YEN)),
        safety_margin_jpy=float(profit_row.get("safety_margin", settings.SAFETY_MARGIN_YEN)),
        exchange_rate=float(profit_row.get("exchange_rate", 2.5)),
        fee_rate=float(profit_row.get("fee_rate", settings.SHOPEE_FEE_RATE)),
        min_profit_jpy=float(settings.MIN_PROFIT_YEN),
        min_roi=float(settings.MIN_ROI),
    )
