"""Competition Analyzer AI — Analyzer Engine (orchestrator).

``AnalyzerEngine`` ties together all three components:

* :class:`~src.competition_analyzer.competitor_scraper.CompetitorScraper`
* :func:`~src.competition_analyzer.price_analysis.analyse_prices_from_listings`
* :class:`~src.competition_analyzer.price_strategy.PriceStrategyEngine`

Pipeline
--------
1. Fetch **profitable** ``profit_analysis`` rows from the DB as the product
   pool (these are products that have been matched, priced, and are ready
   to be listed).
2. For each product:
   a. Delete stale competitor listings (older than ``COMPETITION_FRESHNESS_HOURS``).
   b. Scrape fresh competitor data using the product title / keyword.
   c. Persist competitor listings via ``upsert_competitor_listing()``.
   d. Run price analysis on the scraped prices.
   e. Build a :class:`StrategyInput` from the product's profit_analysis row.
   f. Compute the price recommendation.
   g. Persist the recommendation via ``upsert_price_recommendation()``.

Design constraints
------------------
* Does **not** modify ``profit_engine``, ``matching``, ``listing_manager``
  or any other existing module.
* Reads from ``profit_analysis`` and ``products``; writes only to
  ``competitor_listings`` and ``price_recommendations``.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from src.config.settings import settings
from src.competition_analyzer.competitor_scraper import CompetitorScraper
from src.competition_analyzer.price_analysis import analyse_prices_from_listings
from src.competition_analyzer.price_strategy import (
    PriceStrategyEngine,
    StrategyInput,
    build_strategy_input_from_profit_row,
)
from src.utils.logger import logger

if TYPE_CHECKING:
    from src.database.database import Database


class AnalyzerEngine:
    """Orchestrate competitor scraping, price analysis, and recommendation.

    Parameters
    ----------
    db:
        Open :class:`~src.database.database.Database` instance.
    max_products:
        Max products to analyse per run.
        Defaults to ``settings.COMPETITION_MAX_PRODUCTS``.
    freshness_hours:
        Stale competitor data older than this is deleted before re-scraping.
        Defaults to ``settings.COMPETITION_FRESHNESS_HOURS``.
    """

    def __init__(
        self,
        db: "Database",
        max_products: Optional[int] = None,
        freshness_hours: Optional[int] = None,
    ) -> None:
        self._db               = db
        self._max_products     = max_products or settings.COMPETITION_MAX_PRODUCTS
        self._freshness_hours  = freshness_hours if freshness_hours is not None else settings.COMPETITION_FRESHNESS_HOURS
        self._scraper          = CompetitorScraper()
        self._strategy_engine  = PriceStrategyEngine()

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        """Execute one full competition-analysis run.

        Returns
        -------
        dict
            Summary statistics of the run.
        """
        pool = self._load_product_pool()
        logger.info(
            f"[AnalyzerEngine] Starting analysis — {len(pool)} products in pool"
        )

        analysed   = 0
        skipped    = 0
        total_comp = 0
        total_rec  = 0

        for row in pool:
            if analysed >= self._max_products:
                break
            try:
                comp_count, wrote_rec = self._analyse_product(row)
                total_comp += comp_count
                if wrote_rec:
                    total_rec += 1
                analysed += 1
            except Exception as exc:
                pid = row.get("shopee_product_id", "?")
                logger.warning(
                    f"[AnalyzerEngine] product_id={pid} analysis failed: {exc}"
                )
                skipped += 1

        summary = {
            "products_analysed":      analysed,
            "products_skipped":       skipped,
            "competitor_rows_stored": total_comp,
            "recommendations_stored": total_rec,
        }
        logger.info(
            f"[AnalyzerEngine] Run complete — "
            f"analysed={analysed} recommendations={total_rec} competitors={total_comp}"
        )
        return summary

    def analyse_product(
        self,
        shopee_product_id: int,
        keyword: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Analyse one specific product (useful for ad-hoc/testing).

        Returns a dict with ``competitor_count``, ``recommendation``, and
        ``distribution`` keys.
        """
        # Get the latest profitable profit_analysis row for this product
        row = self._get_best_profit_row(shopee_product_id)
        if row is None:
            logger.warning(
                f"[AnalyzerEngine] No profitable analysis found for product_id={shopee_product_id}"
            )
            return {"competitor_count": 0, "recommendation": None, "distribution": None}

        if keyword:
            row["_override_keyword"] = keyword
        comp_count, _ = self._analyse_product(row)
        rec = self._db.get_price_recommendation(shopee_product_id)
        return {
            "competitor_count": comp_count,
            "recommendation":   rec,
        }

    def get_recommendations(
        self,
        limit: int = 200,
        min_competitor_count: int = 1,
    ) -> List[Dict[str, Any]]:
        """Return persisted price recommendations (read-only helper)."""
        return self._db.get_price_recommendations(
            limit=limit,
            min_competitor_count=min_competitor_count,
        )

    def get_summary_stats(self) -> Dict[str, Any]:
        """Return stats from the DB for reporting."""
        stats = self._db.get_stats()
        return {
            "total_competitor_listings":  stats.get("total_competitor_listings", 0),
            "total_price_recommendations": stats.get("total_price_recommendations", 0),
        }

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _load_product_pool(self) -> List[Dict[str, Any]]:
        """Return profitable matched products with their profit_analysis data.

        Uses a JOIN so each row has both product metadata (title, keyword,
        product_key) and cost structure (supplier_price, fee_rate, etc.).
        """
        sql = """
            SELECT
                pa.*,
                p.title     AS shopee_title,
                p.url       AS shopee_url,
                p.keyword   AS shopee_keyword,
                p.product_key  AS product_key
            FROM profit_analysis pa
            JOIN products p ON p.id = pa.shopee_product_id
            WHERE pa.is_profitable = 1
            ORDER BY pa.profit DESC
            LIMIT :limit
        """
        with self._db.connection() as conn:
            return [dict(r) for r in conn.execute(sql, {"limit": self._max_products * 2}).fetchall()]

    def _get_best_profit_row(
        self,
        shopee_product_id: int,
    ) -> Optional[Dict[str, Any]]:
        """Return the most-profitable profit_analysis row for one product."""
        sql = """
            SELECT pa.*, p.title AS shopee_title, p.keyword AS shopee_keyword,
                   p.product_key AS product_key, p.url AS shopee_url
            FROM profit_analysis pa
            JOIN products p ON p.id = pa.shopee_product_id
            WHERE pa.shopee_product_id = ? AND pa.is_profitable = 1
            ORDER BY pa.profit DESC
            LIMIT 1
        """
        with self._db.connection() as conn:
            row = conn.execute(sql, [shopee_product_id]).fetchone()
        return dict(row) if row else None

    def _analyse_product(self, row: Dict[str, Any]) -> tuple:
        """Analyse one product row.

        Returns (competitor_count_stored, wrote_recommendation: bool).
        """
        shopee_product_id = row["shopee_product_id"]
        product_key       = row.get("product_key")
        title             = row.get("_override_keyword") or row.get("shopee_title", "")
        keyword           = row.get("shopee_keyword") or title
        shopee_url        = row.get("shopee_url", "")

        # ── Step 1: purge stale competitor data ───────────────────────────────
        cutoff = (datetime.utcnow() - timedelta(hours=self._freshness_hours)).isoformat()
        self._db.delete_stale_competitor_listings(shopee_product_id, cutoff)

        # ── Step 2: scrape fresh competitor listings ──────────────────────────
        competitors = self._scraper.scrape(
            shopee_product_id=shopee_product_id,
            keyword=keyword or title,
            product_key=product_key,
            own_url=shopee_url,
        )
        comp_count = 0
        for comp in competitors:
            try:
                self._db.upsert_competitor_listing(comp)
                comp_count += 1
            except Exception as exc:
                logger.debug(
                    f"[AnalyzerEngine] Failed to store competitor listing: {exc}"
                )

        # ── Step 3: fetch stored listings and analyse ─────────────────────────
        stored = self._db.get_competitor_listings(shopee_product_id)
        distribution = analyse_prices_from_listings(stored)

        # ── Step 4: build strategy input from profit_analysis row ─────────────
        strategy_input = build_strategy_input_from_profit_row(
            profit_row=row,
            shopee_product_id=shopee_product_id,
            product_key=product_key,
        )

        # ── Step 5: compute recommendation ────────────────────────────────────
        recommendation = self._strategy_engine.recommend(strategy_input, distribution)

        # ── Step 6: persist ───────────────────────────────────────────────────
        try:
            self._db.upsert_price_recommendation(recommendation)
            wrote = True
        except Exception as exc:
            logger.warning(
                f"[AnalyzerEngine] Failed to store recommendation "
                f"product_id={shopee_product_id}: {exc}"
            )
            wrote = False

        logger.debug(
            f"[AnalyzerEngine] product_id={shopee_product_id} "
            f"competitors={comp_count} "
            f"rec_price=₱{recommendation.recommended_price:.2f} "
            f"floor=₱{recommendation.min_viable_price:.2f} "
            f"strategy={recommendation.strategy_used}"
        )
        return comp_count, wrote


# ── Module-level singleton ────────────────────────────────────────────────────

_engine_instance: Optional[AnalyzerEngine] = None


def get_analyzer_engine(
    db: Optional["Database"] = None,
) -> AnalyzerEngine:
    """Return the module-level singleton, creating it on first call."""
    global _engine_instance
    if _engine_instance is None:
        if db is None:
            from src.database.database import db as _db
            db = _db
        _engine_instance = AnalyzerEngine(db=db)
    return _engine_instance
