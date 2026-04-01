"""OpportunityDiscoveryAI — pure in-memory product opportunity scorer.

Architecture
------------
Evaluates a batch of Shopee search results and assigns each product a
composite "opportunity score" (0–100) BEFORE the expensive Japan supplier
search is triggered.  Products below the configurable threshold (default 60)
are filtered out, significantly reducing unnecessary network calls.

Scoring model
-------------
::

    TotalScore = 0.35 * DemandScore
               + 0.25 * CompetitionScore
               + 0.25 * PriceSpreadScore
               + 0.15 * TrustScore

All sub-scores are in the range [0, 100].

Sub-score definitions
~~~~~~~~~~~~~~~~~~~~~

DemandScore (weight 0.35)
    Evidence that buyers actually want this product.
    • sales_count   — log-scaled, saturates at 10 000 sales → 70 pts
    • review_count  — log-scaled, saturates at 500 reviews  → 30 pts

CompetitionScore (weight 0.25)
    How saturated the keyword result set is, measured by **unique seller count**
    rather than raw listing count.  Shopee allows one seller to post dozens of
    nearly-identical listings, so ``len(results)`` systematically overcounts
    competitors; ``len({p.seller for p in results})`` is far more accurate.

    • shop_diversity_score (60%): fewer unique sellers → less commoditised market
    • price_dispersion_score (40%): high CV → sellers still experimenting on price

PriceSpreadScore (weight 0.25)
    Arbitrage likelihood based on the **interquartile range** (P25–P75) of
    prices in the result set, rather than the simple median.  Shopee's
    occasional $1/$1/$1/$100 outlier pattern skews the median badly; the IQR
    is unaffected by such extremes.

    Two components:
    • market_spread  (50%): wide IQR relative to P25 → spread-out market
    • price_position (50%): how far this product sits from the IQR midpoint

TrustScore (weight 0.15)
    Seller/product credibility.  New shops on Shopee routinely manufacture
    five-star ratings with zero reviews; we therefore **log-scale** the review
    count rather than using a binary presence bonus.
    • shop_rating_score: linear 0–5 → 0–60 pts
    • review_trust:      log10-scaled review_count → 0–40 pts

Performance
-----------
All scoring is pure in-memory arithmetic — no I/O, no OpenAI calls.
50 products are scored in well under 1 ms.

Usage
-----
::

    from src.opportunity_discovery.scorer import OpportunityDiscoveryAI

    ai = OpportunityDiscoveryAI(threshold=60.0)
    filtered, all_scores = ai.score_products(shopee_products, keyword="pokemon card")
    # filtered: products whose opportunity_score >= threshold, sorted DESC
    # all_scores: full scoring detail for every product (including dropped ones)
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Tuple

from src.database.models import ShopeeProduct
from src.utils.logger import logger


# ── Output dataclass ──────────────────────────────────────────────────────────


@dataclass
class OpportunityScore:
    """Opportunity score for a single Shopee product."""

    product: ShopeeProduct
    opportunity_score: float   # 0–100 composite score
    demand_score: float        # 0–100
    competition_score: float   # 0–100
    price_spread_score: float  # 0–100
    trust_score: float         # 0–100
    scored_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "product_url": self.product.product_url,
            "title": self.product.title,
            "shopee_price": self.product.price,
            "opportunity_score": round(self.opportunity_score, 1),
            "demand_score": round(self.demand_score, 1),
            "competition_score": round(self.competition_score, 1),
            "price_spread_score": round(self.price_spread_score, 1),
            "trust_score": round(self.trust_score, 1),
        }


# ── Score weights ─────────────────────────────────────────────────────────────

_W_DEMAND = 0.35
_W_COMPETITION = 0.25
_W_PRICE_SPREAD = 0.25
_W_TRUST = 0.15


# ── Sub-scorer functions ──────────────────────────────────────────────────────


def _demand_score(product: ShopeeProduct) -> float:
    """Score demand based on sales_count and review_count.

    Uses log scaling so that a product with 100 sales scores much better than
    one with 0, but a product with 100 000 sales is not astronomically better
    than one with 10 000 (both show strong demand).

    • sales_count component (70 pts max):
        score = min(70, 70 * log10(sales + 1) / log10(10001))
    • review_count component (30 pts max):
        score = min(30, 30 * log10(reviews + 1) / log10(501))
    """
    sales = max(0, product.sales_count)
    reviews = max(0, product.review_count)

    # log10(10001) ≈ 4.000; saturates at 10 000 sales
    sales_score = min(70.0, 70.0 * math.log10(sales + 1) / math.log10(10_001))

    # log10(501) ≈ 2.700; saturates at 500 reviews
    review_score = min(30.0, 30.0 * math.log10(reviews + 1) / math.log10(501))

    return sales_score + review_score


def _competition_score(
    unique_shop_count: int,
    price_cv: float,
) -> float:
    """Score based on true market saturation using unique seller count.

    Previously used raw listing count, which over-counted competition because
    a single Shopee seller often posts 10–30 almost-identical listings.
    Switching to unique seller count gives a far more accurate signal.

    Parameters
    ----------
    unique_shop_count:
        ``len({p.seller for p in batch if p.seller})`` — pre-computed.
    price_cv:
        Coefficient of variation of prices in the batch — pre-computed.

    Formula
    -------
    • shop_diversity_score  (weight 0.6):
        max(0, 100 - unique_shop_count * 2)
        → 0 pts at ≥50 unique sellers; 100 pts at 0 (edge case — solo listing)
    • price_dispersion_score (weight 0.4):
        min(100, cv * 150)
        → CV of 0.67 (67 % stdev/mean) hits the ceiling; rewards unsettled markets
    """
    shop_diversity = max(0.0, 100.0 - unique_shop_count * 2.0)
    price_dispersion = min(100.0, price_cv * 150.0)

    return shop_diversity * 0.6 + price_dispersion * 0.4


def _price_spread_score(
    product: ShopeeProduct,
    p25: float,
    p75: float,
) -> float:
    """Score arbitrage likelihood using the interquartile price range.

    The simple median is vulnerable to Shopee's price manipulation patterns
    (e.g. one listing at $100 among many at $1 skews the median significantly).
    The IQR (P75 – P25) is unaffected by such extremes.

    Parameters
    ----------
    p25 / p75:
        25th / 75th percentile of prices in the batch — pre-computed.

    Two components
    --------------
    market_spread (50 pts max)
        IQR relative to P25 = (P75 - P25) / P25.
        A wide spread means sellers in this market are heterogeneous, implying
        the market has not been commoditised — good for arbitrage.
        Saturates at IQR = 0.5 × P25 (i.e. 50 % spread).

    price_position (50 pts max)
        How far this specific product deviates from the IQR midpoint.
        Products far outside the IQR centre are priced unusually → opportunity.
        Saturates at 50 % deviation from (P25 + P75) / 2, normalised by P25.
    """
    if p25 <= 0 or product.price <= 0:
        return 0.0

    iqr = max(0.0, p75 - p25)

    # Signal 1: market-level spread — same for all products in the batch
    market_spread = iqr / p25
    market_score = min(50.0, market_spread * 100.0)

    # Signal 2: per-product position relative to IQR midpoint
    iqr_mid = (p25 + p75) / 2.0
    position_deviation = abs(product.price - iqr_mid) / max(p25, 0.01)
    position_score = min(50.0, position_deviation * 100.0)

    return market_score + position_score


def _trust_score(product: ShopeeProduct) -> float:
    """Score seller/product credibility with log-scaled review count.

    Shopee's new-seller farming pattern (high star rating, almost no reviews)
    means we cannot trust the star rating alone.  We log-scale review_count
    to distinguish "1 review" from "100 reviews" properly.

    • shop_rating_score (60 pts max): linear scale 0–5 → 0–60 pts
    • review_trust      (40 pts max): log10(review_count + 1) × 20

      review_count  →  review_trust
           0              0 pts
           1             ~6 pts
          10             ~20 pts
         100             ~40 pts  (ceiling)
         500+            40 pts
    """
    rating = max(0.0, min(5.0, product.rating))
    shop_rating_score = (rating / 5.0) * 60.0

    reviews = max(0, product.review_count)
    # log10(101) ≈ 2.0  →  2.0 × 20 = 40 pts (practical ceiling at ~100 reviews)
    review_trust = min(40.0, math.log10(reviews + 1) * 20.0)

    return shop_rating_score + review_trust


# ── Main class ────────────────────────────────────────────────────────────────


class OpportunityDiscoveryAI:
    """Score and filter Shopee products by opportunity before supplier search.

    Parameters
    ----------
    threshold:
        Minimum opportunity_score (0–100) required to pass a product through
        to the Japan supplier search stage.  Products below this threshold are
        dropped silently.  Default: 60.
    """

    def __init__(self, threshold: float = 60.0) -> None:
        self.threshold = threshold

    # ── Public API ────────────────────────────────────────────────────────────

    def score_products(
        self,
        products: List[ShopeeProduct],
        keyword: str = "",
    ) -> Tuple[List[ShopeeProduct], List[OpportunityScore]]:
        """Score all *products* and return those that pass the threshold.

        Parameters
        ----------
        products:
            Shopee products from the scraper stage.
        keyword:
            The search keyword (used for logging only).

        Returns
        -------
        filtered_products : List[ShopeeProduct]
            Products whose opportunity_score >= self.threshold, sorted by
            score descending.
        all_scores : List[OpportunityScore]
            Full scoring detail for every product (including filtered ones),
            sorted by opportunity_score descending.
        """
        if not products:
            return [], []

        # ── Pre-compute batch-level statistics (done once, O(n)) ───────────
        prices = [p.price for p in products if p.price > 0]

        # IQR via P25/P75 — robust against outlier prices (② PriceSpreadScore)
        if len(prices) >= 4:
            q = statistics.quantiles(prices, n=4)  # [P25, P50, P75]
            p25, p75 = q[0], q[2]
        elif len(prices) >= 2:
            p25, p75 = min(prices), max(prices)
        elif len(prices) == 1:
            p25 = p75 = prices[0]
        else:
            p25 = p75 = 0.0

        # Unique seller count — true competitor count (① CompetitionScore)
        valid_sellers = [p.seller for p in products if p.seller]
        unique_shop_count = (
            len(set(valid_sellers)) if valid_sellers else len(products)
        )

        # Price coefficient of variation (① CompetitionScore)
        if len(prices) >= 2:
            mean_price = statistics.mean(prices)
            price_cv = statistics.stdev(prices) / mean_price if mean_price > 0 else 0.0
        else:
            price_cv = 0.0

        # ── Pre-compute batch-level competition score (same for all products)
        batch_competition = _competition_score(unique_shop_count, price_cv)

        # ── Score every product ────────────────────────────────────────────
        all_scores: List[OpportunityScore] = []

        for product in products:
            demand = _demand_score(product)
            competition = batch_competition  # batch-level signal
            price_spread = _price_spread_score(product, p25, p75)
            trust = _trust_score(product)

            total = (
                _W_DEMAND * demand
                + _W_COMPETITION * competition
                + _W_PRICE_SPREAD * price_spread
                + _W_TRUST * trust
            )

            all_scores.append(
                OpportunityScore(
                    product=product,
                    opportunity_score=total,
                    demand_score=demand,
                    competition_score=competition,
                    price_spread_score=price_spread,
                    trust_score=trust,
                )
            )

        # Sort descending by total score
        all_scores.sort(key=lambda s: s.opportunity_score, reverse=True)

        # Filter by threshold
        passed = [s for s in all_scores if s.opportunity_score >= self.threshold]
        filtered_products = [s.product for s in passed]

        logger.info(
            f"[OpportunityDiscoveryAI] keyword={keyword!r} | "
            f"scored={len(all_scores)} | "
            f"passed(>={self.threshold:.0f})={len(passed)} | "
            f"dropped={len(all_scores) - len(passed)} | "
            f"unique_shops={unique_shop_count} | "
            f"price_cv={price_cv:.2f} | "
            f"IQR=[{p25:.1f}, {p75:.1f}]"
        )

        if passed:
            top = passed[0]
            logger.debug(
                f"[OpportunityDiscoveryAI] Top product: "
                f"{top.product.title[:50]!r} "
                f"score={top.opportunity_score:.1f} "
                f"(D={top.demand_score:.1f} "
                f"C={top.competition_score:.1f} "
                f"P={top.price_spread_score:.1f} "
                f"T={top.trust_score:.1f})"
            )

        return filtered_products, all_scores
