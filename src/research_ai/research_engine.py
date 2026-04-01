"""Research AI — main orchestration engine.

``ResearchEngine`` is the single entry point for the discovery layer of the
arbitrage pipeline.  It connects the Shopee product catalogue with the scoring
and trend-analysis sub-modules and persists results to ``research_candidates``.

Pipeline position
-----------------
::

    Shopee Scraper
        ↓
    ShopeeProducts  (products table)
        ↓
    ResearchEngine.scan()          ← THIS MODULE
        ↓
    ResearchCandidates             (research_candidates table)
        ↓
    Japan marketplace search
        ↓
    ProductMatches / ProfitAnalysis / ListingCandidates

``ResearchEngine`` is deliberately read-only with respect to the upstream
tables (``products``, ``matches``, ``product_snapshots``, ``trends``).
It only writes to ``research_candidates``.  No existing modules are modified.

Usage
-----
::

    from src.database.database import db
    from src.research_ai.research_engine import ResearchEngine

    db.initialize()
    engine = ResearchEngine(db=db)
    candidates = engine.scan()          # returns List[ResearchCandidate]
    pending    = engine.get_candidates()  # returns List[dict] for downstream
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.config.settings import settings
from src.database.database import db as _default_db
from src.database.models import ResearchCandidate, ResearchCandidateStatus
from src.research_ai.scoring import ResearchScorer
from src.research_ai.trend_detection import SnapshotTrendAnalyzer
from src.utils.logger import logger


class ResearchEngine:
    """Discover and score profitable product candidates from the Shopee catalogue.

    Parameters
    ----------
    db:
        Database instance.  Defaults to the module-level singleton ``db``
        from ``src.database.database``.  Inject an alternative in tests.
    scorer:
        ``ResearchScorer`` instance.  Defaults to a fresh ``ResearchScorer()``.
        Override to use custom weights or normalisation ceilings.
    trend_analyzer:
        ``SnapshotTrendAnalyzer`` instance.  Defaults to a new one bound to
        *db*.  Override for testing.
    min_score:
        Composite score threshold.  Products below this value are not
        persisted.  Defaults to ``settings.RESEARCH_MIN_SCORE``.
    max_candidates:
        Hard cap on new/updated candidates per ``scan()`` call.  Defaults to
        ``settings.RESEARCH_MAX_CANDIDATES``.
    """

    def __init__(
        self,
        db=None,
        scorer: Optional[ResearchScorer] = None,
        trend_analyzer: Optional[SnapshotTrendAnalyzer] = None,
        min_score: Optional[float] = None,
        max_candidates: Optional[int] = None,
    ) -> None:
        self._db             = db or _default_db
        self._scorer         = scorer or ResearchScorer()
        self._trend_analyzer = trend_analyzer or SnapshotTrendAnalyzer(self._db)
        self.min_score       = min_score if min_score is not None else settings.RESEARCH_MIN_SCORE
        self.max_candidates  = max_candidates or settings.RESEARCH_MAX_CANDIDATES

    # ── Public API ────────────────────────────────────────────────────────────

    def scan(
        self,
        limit: Optional[int] = None,
        min_sales: Optional[int] = None,
        market: Optional[str] = None,
    ) -> List[ResearchCandidate]:
        """Scan the Shopee product catalogue and update ``research_candidates``.

        Algorithm
        ---------
        1. Fetch up to ``limit`` Shopee products that meet the minimum sales
           and price filters from the ``products`` table.
        2. For each product:
           a. Fetch the latest trend row (from ``trends`` table) if available.
           b. Fetch snapshot-derived stats via ``SnapshotTrendAnalyzer``
              (requires prior matching; returns ``None`` for new products).
           c. Call ``ResearchScorer.score()`` to get a ``ScoreBreakdown``.
           d. Skip products whose total score < ``self.min_score``.
           e. Build a ``ResearchCandidate`` and upsert it to the database.
        3. Stop early once ``self.max_candidates`` upserts have been written.
        4. Return the list of all persisted candidates (sorted by score desc).

        Parameters
        ----------
        limit:
            Maximum products to read from the catalogue per run.  Defaults to
            ``self.max_candidates * 5`` to ensure the cap doesn't produce an
            artificially small evaluation pool.
        min_sales:
            Override the minimum sales count filter.  Defaults to
            ``settings.RESEARCH_MIN_SALES``.
        market:
            Filter to a specific Shopee market code (e.g. ``'PH'``).  Defaults
            to ``None`` (all markets).

        Returns
        -------
        List[ResearchCandidate]
            Candidates persisted in this run, sorted by ``research_score`` desc.
        """
        start_time = time.time()
        fetch_limit = limit or (self.max_candidates * 5)
        floor_sales = min_sales if min_sales is not None else settings.RESEARCH_MIN_SALES

        logger.info(
            f"[ResearchEngine] Starting scan — "
            f"min_score={self.min_score}, max_candidates={self.max_candidates}, "
            f"min_sales={floor_sales}, market={market or 'all'}"
        )

        # ── 1. Fetch product rows ─────────────────────────────────────────────
        product_rows: List[Dict[str, Any]] = self._db.get_products(
            min_sales=floor_sales,
            min_price=settings.MIN_PRICE_PHP,
            market=market,
            limit=fetch_limit,
        )
        logger.info(f"[ResearchEngine] {len(product_rows)} products loaded for evaluation")

        # ── 2. Pre-fetch trend rows into a lookup dict ─────────────────────────
        # Fetching all trends once is cheaper than N individual queries.
        trend_lookup = self._build_trend_lookup()

        # ── 3. Score each product ─────────────────────────────────────────────
        scored: List[ResearchCandidate] = []
        total_evaluated  = 0
        total_above_threshold = 0

        for row in product_rows:
            if len(scored) >= self.max_candidates:
                logger.debug(
                    "[ResearchEngine] max_candidates cap reached, stopping early"
                )
                break

            total_evaluated += 1
            product_id  = row.get("id")
            product_url = row.get("url", "")

            try:
                trend_row      = trend_lookup.get(product_url)
                snapshot_stats = self._trend_analyzer.get_snapshot_stats_for_shopee(product_id)

                breakdown = self._scorer.score(
                    product_row=row,
                    trend_row=trend_row,
                    snapshot_stats=snapshot_stats,
                )

                if breakdown.total < self.min_score:
                    logger.debug(
                        f"[ResearchEngine] skip product_id={product_id} "
                        f"score={breakdown.total:.2f} < threshold"
                    )
                    continue

                total_above_threshold += 1

                candidate = ResearchCandidate(
                    shopee_product_id=product_id,
                    research_score=breakdown.total,
                    score_demand=breakdown.demand,
                    score_velocity=breakdown.velocity,
                    score_stability=breakdown.stability,
                    score_price_gap=breakdown.price_gap,
                    score_brand=breakdown.brand,
                    reason=breakdown.reason_string(max_factors=3),
                    status=ResearchCandidateStatus.PENDING,
                    created_at=datetime.utcnow(),
                )

                self._db.upsert_research_candidate(candidate)
                scored.append(candidate)

            except Exception as exc:
                logger.error(
                    f"[ResearchEngine] Error scoring product_id={product_id}: {exc}",
                    exc_info=True,
                )
                continue

        # Sort by score descending for the return value
        scored.sort(key=lambda c: c.research_score, reverse=True)

        elapsed = time.time() - start_time
        logger.info(
            f"[ResearchEngine] Scan complete in {elapsed:.2f}s — "
            f"evaluated={total_evaluated}, "
            f"above_threshold={total_above_threshold}, "
            f"persisted={len(scored)}"
        )

        return scored

    def get_candidates(
        self,
        status: str = "pending",
        min_score: float = 0.0,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """Return research candidates for downstream pipeline consumption.

        This is the read-side of the research → Japan search handoff.
        The Japan search pipeline calls this to get its work queue.

        Parameters
        ----------
        status:
            Lifecycle filter.  Pass ``None`` to return all statuses.
            Defaults to ``'pending'`` (not yet searched on Japan marketplaces).
        min_score:
            Only return candidates above this threshold.
        limit:
            Maximum rows.

        Returns
        -------
        List[dict]
            Rows from ``research_candidates`` joined with ``products`` columns
            (``shopee_title``, ``shopee_price``, ``shopee_url``,
            ``shopee_keyword``, etc.).  Sorted by ``research_score DESC``.
        """
        return self._db.get_research_candidates(
            status=status,
            min_score=min_score,
            limit=limit,
        )

    def mark_matched(self, shopee_product_id: int) -> None:
        """Advance candidate status to ``'matched'``.

        Called by the Japan search pipeline after a Japan product has been
        found and stored in ``matches``.

        Parameters
        ----------
        shopee_product_id: ``products.id`` of the matched Shopee product.
        """
        self._db.update_candidate_status(
            shopee_product_id, ResearchCandidateStatus.MATCHED
        )
        logger.debug(
            f"[ResearchEngine] shopee_id={shopee_product_id} → matched"
        )

    def mark_rejected(self, shopee_product_id: int) -> None:
        """Advance candidate status to ``'rejected'``.

        Called by the profit engine when all analyses for this product fall
        below profit thresholds, or when no Japan match can be found.

        Parameters
        ----------
        shopee_product_id: ``products.id`` of the Shopee product.
        """
        self._db.update_candidate_status(
            shopee_product_id, ResearchCandidateStatus.REJECTED
        )
        logger.debug(
            f"[ResearchEngine] shopee_id={shopee_product_id} → rejected"
        )

    def get_summary_stats(self) -> Dict[str, Any]:
        """Return a dict of research pipeline health metrics.

        Useful for logging and monitoring dashboards.
        """
        db_stats = self._db.get_stats()
        return {
            "research_pending":  db_stats.get("research_pending",  0),
            "research_matched":  db_stats.get("research_matched",  0),
            "research_rejected": db_stats.get("research_rejected", 0),
        }

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _build_trend_lookup(self) -> Dict[str, Dict[str, Any]]:
        """Fetch all latest trend rows and return a URL → row dict.

        Fetching all trends in a single query (rather than one per product) is
        O(1) database round-trips instead of O(N).
        """
        try:
            rows = self._db.get_latest_trends(limit=10_000)
            return {r["product_url"]: r for r in rows if r.get("product_url")}
        except Exception as exc:
            logger.warning(
                f"[ResearchEngine] Could not load trend data: {exc}. "
                "Proceeding without trend scores."
            )
            return {}


# ── Module-level singleton ────────────────────────────────────────────────────

research_engine = ResearchEngine()
