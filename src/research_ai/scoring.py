"""Research AI — scoring module.

Assigns a composite 0–100 ``research_score`` to each Shopee product by
combining five independent sub-scores, each capturing a different dimension
of arbitrage potential.

Sub-score definitions
---------------------
score_demand   (weight 0.35)
    Strength of proven market demand.  Derived from ``sales`` (cumulative
    Shopee sold count), ``rating`` (star rating 0–5), and ``review_count``.
    A product that sells well, is well-rated, and has many reviews signals
    genuine, sustainable demand — the most reliable predictor that a Japan
    equivalent will also sell.

score_velocity (weight 0.25)
    Rate of recent growth.  When trend data (computed by the existing
    ``TrendDetector``) is available, this re-uses ``trend_score`` directly.
    When trend data is absent (e.g. a newly scraped product), a static
    estimate is derived from ``sales`` divided by product age.

score_stability (weight 0.15)
    Price predictability.  Stable prices → predictable gross margin →
    lower risk of a profitable window closing unexpectedly.  Sourced from
    ``trends.price_stability`` (0–1 scale) when available, or from the
    coefficient of variation of Japan-side snapshot prices when the product
    is already in the pipeline, falling back to a neutral 0.50 otherwise.

score_price_gap (weight 0.15)
    Estimated headroom for arbitrage.  A higher Shopee listing price relative
    to the configured minimum threshold (``settings.MIN_PRICE_PHP``) implies
    more room to cover Japan sourcing costs and still hit the ROI target.
    A product_key with ``high`` or ``barcode`` confidence adds a small bonus
    because identifiable brand items typically have a clear Japan equivalent
    with a known price.

score_brand (weight 0.10)
    Brand recognition, proxied by the ``product_key_confidence`` field set
    by the product_key generator.  A barcode match (EAN-13) is the strongest
    signal; ``none`` means the key system could not identify a brand at all.

Composite score
---------------
    research_score = Σ( sub_score × weight )

Products with ``research_score >= settings.RESEARCH_MIN_SCORE`` (default 50)
are persisted to ``research_candidates``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from src.config.settings import settings
from src.utils.logger import logger


# ── Score weights ─────────────────────────────────────────────────────────────

WEIGHTS: Dict[str, float] = {
    "demand":    0.35,
    "velocity":  0.25,
    "stability": 0.15,
    "price_gap": 0.15,
    "brand":     0.10,
}

# ── Brand confidence → score mapping ─────────────────────────────────────────

_BRAND_SCORES: Dict[str, float] = {
    "barcode":      100.0,
    "high":          90.0,
    "medium_high":   70.0,
    "medium":        50.0,
    "low":           25.0,
    "none":           0.0,
}

# ── Reference ceilings used for normalization ─────────────────────────────────

# Sales count considered "excellent" (top of demand curve).
# Products at or above this value receive the maximum demand contribution
# from the sales component.
_EXCELLENT_SALES: float = 5_000.0

# Review count considered "excellent".
_EXCELLENT_REVIEWS: float = 500.0

# Sales count that maps to velocity score 100 when no trend data exists
# (used in the static fall-back estimator).
_EXCELLENT_STATIC_VELOCITY_SALES: float = 2_000.0


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class ScoreBreakdown:
    """Full scoring result for one Shopee product.

    Attributes
    ----------
    demand:    Raw demand sub-score (0–100).
    velocity:  Raw velocity sub-score (0–100).
    stability: Raw stability sub-score (0–100).
    price_gap: Raw price-gap sub-score (0–100).
    brand:     Raw brand-recognition sub-score (0–100).
    total:     Weighted composite score (0–100).
    reasons:   Human-readable list of top contributing factors, highest first.
    """
    demand:    float = 0.0
    velocity:  float = 0.0
    stability: float = 0.0
    price_gap: float = 0.0
    brand:     float = 0.0
    total:     float = 0.0
    reasons:   List[str] = field(default_factory=list)

    def reason_string(self, max_factors: int = 3) -> str:
        """Return the top *max_factors* reasons as a compact string."""
        return "; ".join(self.reasons[:max_factors]) if self.reasons else "no data"


# ── Scorer ────────────────────────────────────────────────────────────────────

class ResearchScorer:
    """Compute a research score for one Shopee product row.

    Designed to be stateless and side-effect-free — it only reads from the
    dicts passed to it, never touching the database directly.  The caller
    (``ResearchEngine``) is responsible for fetching the optional context rows.

    Usage
    -----
    ::

        scorer = ResearchScorer()
        breakdown = scorer.score(
            product_row=row,          # dict from db.get_products()
            trend_row=trend_row,      # dict from db.get_latest_trends()  or None
            snapshot_stats=stats,     # dict from SnapshotTrendAnalyzer  or None
        )
        if breakdown.total >= settings.RESEARCH_MIN_SCORE:
            ...
    """

    # Public weights reference so callers can inspect / override in tests.
    WEIGHTS = WEIGHTS

    def score(
        self,
        product_row: Dict[str, Any],
        trend_row: Optional[Dict[str, Any]] = None,
        snapshot_stats: Optional[Dict[str, Any]] = None,
    ) -> ScoreBreakdown:
        """Compute the full scoring breakdown for one product.

        Parameters
        ----------
        product_row:
            A dict from ``Database.get_products()`` (columns from ``products``
            table).  Required keys: ``id``, ``price``, ``sales``, ``rating``,
            ``review_count``, ``product_key_confidence``.
        trend_row:
            Optional dict from ``Database.get_latest_trends()``.  Expected
            keys: ``trend_score`` (0–100), ``price_stability`` (0–1),
            ``sales_velocity`` (sales/day).
        snapshot_stats:
            Optional dict from ``SnapshotTrendAnalyzer.get_snapshot_stats()``.
            Expected keys: ``price_cv`` (coefficient of variation, lower =
            more stable), ``sales_delta`` (Δsales over window),
            ``review_delta`` (Δreviews over window).

        Returns
        -------
        ScoreBreakdown
            All sub-scores and the weighted composite total.
        """
        demand,    reason_d = self._demand_score(product_row)
        velocity,  reason_v = self._velocity_score(product_row, trend_row, snapshot_stats)
        stability, reason_s = self._stability_score(trend_row, snapshot_stats)
        price_gap, reason_p = self._price_gap_score(product_row)
        brand,     reason_b = self._brand_score(product_row)

        total = (
            demand    * WEIGHTS["demand"]    +
            velocity  * WEIGHTS["velocity"]  +
            stability * WEIGHTS["stability"] +
            price_gap * WEIGHTS["price_gap"] +
            brand     * WEIGHTS["brand"]
        )
        total = round(min(max(total, 0.0), 100.0), 4)

        # Build reasons list ordered by weighted contribution (highest first)
        contributions = [
            (demand    * WEIGHTS["demand"],    reason_d),
            (velocity  * WEIGHTS["velocity"],  reason_v),
            (stability * WEIGHTS["stability"], reason_s),
            (price_gap * WEIGHTS["price_gap"], reason_p),
            (brand     * WEIGHTS["brand"],     reason_b),
        ]
        contributions.sort(key=lambda x: x[0], reverse=True)
        reasons = [msg for _, msg in contributions if msg]

        logger.debug(
            f"[Scorer] product_id={product_row.get('id')} "
            f"total={total:.2f} "
            f"(D={demand:.1f} V={velocity:.1f} S={stability:.1f} "
            f"P={price_gap:.1f} B={brand:.1f})"
        )

        return ScoreBreakdown(
            demand=round(demand,    4),
            velocity=round(velocity,  4),
            stability=round(stability, 4),
            price_gap=round(price_gap, 4),
            brand=round(brand,     4),
            total=total,
            reasons=reasons,
        )

    # ── Sub-score implementations ─────────────────────────────────────────────

    def _demand_score(
        self, row: Dict[str, Any]
    ) -> tuple[float, str]:
        """Normalize sales count, rating, and review count to 0–100.

        Weights within demand component:
            sales_count  45 %
            rating       30 %
            review_count 25 %
        """
        sales        = float(row.get("sales", 0) or 0)
        rating       = float(row.get("rating", 0.0) or 0.0)
        review_count = float(row.get("review_count", 0) or 0)

        sales_score  = min(sales  / _EXCELLENT_SALES,   1.0) * 100.0
        rating_score = min(rating / 5.0,                1.0) * 100.0
        review_score = min(review_count / _EXCELLENT_REVIEWS, 1.0) * 100.0

        demand = (
            sales_score  * 0.45 +
            rating_score * 0.30 +
            review_score * 0.25
        )

        # Build reason fragment
        if sales >= _EXCELLENT_SALES * 0.5:
            reason = f"high sales ({int(sales):,})"
        elif rating >= 4.5:
            reason = f"top-rated ({rating:.1f}★)"
        elif review_count >= _EXCELLENT_REVIEWS * 0.3:
            reason = f"many reviews ({int(review_count):,})"
        else:
            reason = f"moderate demand (sales={int(sales):,}, rating={rating:.1f})"

        return demand, reason

    def _velocity_score(
        self,
        row: Dict[str, Any],
        trend_row: Optional[Dict[str, Any]],
        snapshot_stats: Optional[Dict[str, Any]],
    ) -> tuple[float, str]:
        """Estimate rate-of-growth signal.

        Priority:
        1. ``trend_row.trend_score``       — already 0–100, direct re-use
        2. ``snapshot_stats.sales_delta``  — snapshot-derived Δsales over window
        3. Static estimate from sales / product age
        """
        if trend_row and trend_row.get("trend_score") is not None:
            score = float(trend_row["trend_score"])
            direction = trend_row.get("direction", "stable")
            reason = f"trend {direction} (score={score:.0f})"
            return score, reason

        if snapshot_stats and snapshot_stats.get("sales_delta") is not None:
            delta = float(snapshot_stats["sales_delta"])
            window = float(snapshot_stats.get("window_days", settings.RESEARCH_SCORE_WINDOW_DAYS))
            daily_delta = delta / max(window, 1)
            score = min(daily_delta / 10.0, 1.0) * 100.0  # 10 sales/day = 100pts
            reason = f"snapshot velocity ({daily_delta:.1f} sales/day)"
            return score, reason

        # Static fallback: treat cumulative sales as a rough proxy
        sales = float(row.get("sales", 0) or 0)
        score = min(sales / _EXCELLENT_STATIC_VELOCITY_SALES, 1.0) * 100.0
        reason = f"static velocity estimate (sales={int(sales):,})"
        return score, reason

    def _stability_score(
        self,
        trend_row: Optional[Dict[str, Any]],
        snapshot_stats: Optional[Dict[str, Any]],
    ) -> tuple[float, str]:
        """Measure price predictability.

        1 = perfectly stable, 0 = highly volatile.

        Priority:
        1. ``trend_row.price_stability``  — directly from TrendDetector
        2. ``snapshot_stats.price_cv``    — coefficient of variation from snapshots
        3. Neutral fallback = 0.50
        """
        if trend_row and trend_row.get("price_stability") is not None:
            stability_01 = float(trend_row["price_stability"])  # already 0–1
            score = stability_01 * 100.0
            level = "stable" if score >= 70 else ("volatile" if score < 40 else "moderate")
            reason = f"price {level} (stability={stability_01:.2f})"
            return score, reason

        if snapshot_stats and snapshot_stats.get("price_cv") is not None:
            cv = float(snapshot_stats["price_cv"])
            # CV close to 0 = stable; map [0, 1] → [100, 0]
            stability_01 = max(0.0, 1.0 - cv)
            score = stability_01 * 100.0
            reason = f"Japan price CV={cv:.2f} ({'stable' if cv < 0.05 else 'volatile'})"
            return score, reason

        # Neutral fallback
        return 50.0, "price stability unknown (no history)"

    def _price_gap_score(
        self, row: Dict[str, Any]
    ) -> tuple[float, str]:
        """Estimate potential arbitrage headroom from the Shopee listing price.

        Formula
        -------
        The higher the Shopee price relative to ``MIN_PRICE_PHP``, the more
        room there is to cover Japan sourcing costs and still achieve the
        target ROI.

        ``price_ratio = price / MIN_PRICE_PHP``
        ``base_score  = min(log2(price_ratio + 1) / log2(11), 1.0) * 100``
        (logarithmic so very high prices don't dominate)

        A ``product_key`` with ``high`` or ``barcode`` confidence adds up to
        10 bonus points because brand-identifiable products have a more
        predictable Japan equivalent price.
        """
        price     = float(row.get("price", 0.0) or 0.0)
        min_price = settings.MIN_PRICE_PHP

        if price < min_price:
            return 0.0, f"price ₱{price:.0f} below minimum ₱{min_price:.0f}"

        # Logarithmic scale: at 2× min_price → ~26pts; 5× → ~60pts; 10× → 100pts
        price_ratio = price / min_price
        base_score = min(math.log2(price_ratio + 1) / math.log2(11), 1.0) * 100.0

        # Brand bonus for easier Japan price discovery
        confidence = str(row.get("product_key_confidence", "none") or "none")
        brand_bonus = 10.0 if confidence in ("barcode", "high") else 0.0

        score = min(base_score + brand_bonus, 100.0)
        reason = (
            f"Shopee price ₱{price:.0f} ({price_ratio:.1f}× min)"
            + (" + brand bonus" if brand_bonus else "")
        )
        return score, reason

    def _brand_score(
        self, row: Dict[str, Any]
    ) -> tuple[float, str]:
        """Proxy brand recognition through ``product_key_confidence``.

        A barcode match proves the product has a globally unique identity
        (EAN-13).  A ``high`` confidence hash means brand + model + edition
        were all extracted — strongly indicative of a known branded item.

        Products with ``none`` confidence may still be profitable but are
        harder to match automatically against Japan catalogues.
        """
        confidence = str(row.get("product_key_confidence", "none") or "none")
        score = _BRAND_SCORES.get(confidence, 0.0)
        reason = f"brand confidence: {confidence} ({score:.0f}pts)"
        return score, reason
