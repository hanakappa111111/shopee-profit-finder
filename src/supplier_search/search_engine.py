"""Japan Supplier Search AI — Search Engine orchestrator.

``SupplierSearchEngine`` is the top-level coordinator that:

1. Reads seeds from ``research_candidates`` and ``related_product_candidates``.
2. Generates optimised search queries via :class:`QueryBuilder`.
3. Dispatches queries to marketplace adapters (Amazon JP, Rakuten, Yahoo, Mercari).
4. Enriches results with ``product_key`` (done inside each adapter).
5. Deduplicates by ``product_url``.
6. Persists results to the ``sources`` table (``JapanProducts``) via
   ``Database.upsert_source()``.

Design constraints
------------------
* **No writes to existing modules** — only reads from ``research_candidates``,
  ``related_product_candidates``, and ``products``; writes only to ``sources``.
* Marketplace adapters are pluggable — controlled by
  ``settings.SUPPLIER_MARKETPLACES``.
* Rate-limiting: configurable delay between marketplace requests.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from src.config.settings import settings
from src.database.models import JapanProduct, JapanSource
from src.supplier_search.query_builder import QueryBuilder, SearchQuery
from src.utils.logger import logger

if TYPE_CHECKING:
    from src.database.database import Database
    from src.supplier_search.base_scraper import BaseMarketplaceAdapter


# ── Marketplace registry ──────────────────────────────────────────────────────
# Maps JapanSource enum values to adapter classes (lazy imports).

_MARKETPLACE_MAP: Dict[str, type] = {}


def _ensure_registry() -> None:
    """Populate the marketplace registry on first use."""
    global _MARKETPLACE_MAP
    if _MARKETPLACE_MAP:
        return

    from src.supplier_search.amazon_scraper import AmazonAdapter
    from src.supplier_search.rakuten_scraper import RakutenAdapter
    from src.supplier_search.yahoo_scraper import YahooAdapter
    from src.supplier_search.mercari_scraper import MercariAdapter

    _MARKETPLACE_MAP = {
        JapanSource.AMAZON_JP.value:       AmazonAdapter,
        JapanSource.RAKUTEN.value:         RakutenAdapter,
        JapanSource.YAHOO_SHOPPING.value:  YahooAdapter,
        JapanSource.MERCARI.value:         MercariAdapter,
    }


class SupplierSearchEngine:
    """Orchestrate supplier searches across Japanese marketplaces.

    Parameters
    ----------
    db:
        Open :class:`~src.database.database.Database` instance.
    marketplaces:
        List of marketplace slugs to search (subset of
        ``JapanSource`` values).  Defaults to
        ``settings.SUPPLIER_MARKETPLACES``.
    max_queries_per_seed:
        Passed to :class:`QueryBuilder`.
        Defaults to ``settings.SUPPLIER_MAX_QUERIES_PER_SEED``.
    request_delay:
        Seconds to sleep between marketplace HTTP requests.
        Defaults to ``settings.SUPPLIER_REQUEST_DELAY``.
    """

    def __init__(
        self,
        db: "Database",
        marketplaces: Optional[List[str]] = None,
        max_queries_per_seed: Optional[int] = None,
        request_delay: Optional[float] = None,
    ) -> None:
        self._db     = db
        self._delay  = (
            request_delay if request_delay is not None
            else settings.SUPPLIER_REQUEST_DELAY
        )
        self._qb     = QueryBuilder(max_queries=max_queries_per_seed)

        # Instantiate requested marketplace adapters
        _ensure_registry()
        mp_list = marketplaces or settings.SUPPLIER_MARKETPLACES
        self._adapters: Dict[str, "BaseMarketplaceAdapter"] = {}
        for mp in mp_list:
            cls = _MARKETPLACE_MAP.get(mp)
            if cls is not None:
                self._adapters[mp] = cls()
            else:
                logger.warning(
                    f"[SupplierSearch] Unknown marketplace {mp!r} — skipping"
                )

    # ── Public API ────────────────────────────────────────────────────────────

    def run(
        self,
        seed_min_score: Optional[float] = None,
        related_min_confidence: Optional[float] = None,
        max_seeds: Optional[int] = None,
        search_related: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Execute a full supplier-search run.

        Parameters
        ----------
        seed_min_score:
            Min ``research_score`` for ResearchCandidate seeds.
        related_min_confidence:
            Min ``confidence_score`` for RelatedProductCandidate seeds.
        max_seeds:
            Hard cap on total seeds processed.
        search_related:
            Whether to include RelatedProductCandidates.

        Returns
        -------
        dict
            Summary statistics of the run.
        """
        seed_min   = seed_min_score if seed_min_score is not None else settings.SUPPLIER_SEED_MIN_SCORE
        rel_min    = related_min_confidence if related_min_confidence is not None else settings.SUPPLIER_RELATED_MIN_CONFIDENCE
        limit      = max_seeds or settings.SUPPLIER_MAX_SEEDS
        do_related = search_related if search_related is not None else settings.SUPPLIER_SEARCH_RELATED

        total_queries   = 0
        total_results   = 0
        total_persisted = 0
        seeds_processed = 0

        # ── Phase 1: ResearchCandidates ───────────────────────────────────────
        research_seeds = self._db.get_research_candidates(
            status="pending",
            min_score=seed_min,
            limit=limit,
        )
        logger.info(
            f"[SupplierSearch] Phase 1: {len(research_seeds)} research seeds "
            f"(min_score={seed_min})"
        )

        for seed in research_seeds:
            if seeds_processed >= limit:
                break
            queries = self._qb.build_from_research_candidate(seed)
            qc, rc, pc = self._execute_queries(queries)
            total_queries   += qc
            total_results   += rc
            total_persisted += pc
            seeds_processed += 1

        # ── Phase 2: RelatedProductCandidates ─────────────────────────────────
        if do_related and seeds_processed < limit:
            related_seeds = self._db.get_related_candidates(
                min_confidence=rel_min,
                limit=limit - seeds_processed,
            )
            logger.info(
                f"[SupplierSearch] Phase 2: {len(related_seeds)} related seeds "
                f"(min_confidence={rel_min})"
            )

            for seed in related_seeds:
                if seeds_processed >= limit:
                    break
                queries = self._qb.build_from_related_candidate(seed)
                qc, rc, pc = self._execute_queries(queries)
                total_queries   += qc
                total_results   += rc
                total_persisted += pc
                seeds_processed += 1

        summary = {
            "seeds_processed":     seeds_processed,
            "total_queries":       total_queries,
            "total_results":       total_results,
            "total_persisted":     total_persisted,
            "marketplaces_active": list(self._adapters.keys()),
        }

        logger.info(
            f"[SupplierSearch] Run complete — "
            f"seeds={seeds_processed} queries={total_queries} "
            f"results={total_results} persisted={total_persisted}"
        )
        return summary

    def search_single(
        self,
        query: str,
        marketplace: Optional[str] = None,
    ) -> List[JapanProduct]:
        """Run a single search query (useful for ad-hoc testing).

        Parameters
        ----------
        query:
            Raw search string.
        marketplace:
            Specific marketplace to search.  If ``None``, searches all
            active marketplaces.

        Returns
        -------
        List[JapanProduct]
            Merged, deduplicated results (not persisted).
        """
        adapters = (
            {marketplace: self._adapters[marketplace]}
            if marketplace and marketplace in self._adapters
            else self._adapters
        )

        seen_urls: set[str] = set()
        results: List[JapanProduct] = []

        for mp_name, adapter in adapters.items():
            products = adapter.search(query)
            for p in products:
                if p.product_url and p.product_url not in seen_urls:
                    results.append(p)
                    seen_urls.add(p.product_url)
            if len(adapters) > 1:
                time.sleep(self._delay)

        results.sort(key=lambda p: p.price_jpy)
        return results

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _execute_queries(
        self,
        queries: List[SearchQuery],
    ) -> tuple:
        """Execute a list of queries across all active marketplaces.

        Returns (queries_executed, results_found, results_persisted).
        """
        query_count    = 0
        result_count   = 0
        persist_count  = 0
        seen_urls: set[str] = set()

        for sq in queries:
            for mp_name, adapter in self._adapters.items():
                products = adapter.search(sq.query)
                query_count += 1

                for product in products:
                    result_count += 1
                    if product.product_url and product.product_url in seen_urls:
                        continue
                    if product.product_url:
                        seen_urls.add(product.product_url)

                    # Persist to sources table
                    try:
                        self._db.upsert_source(product)
                        persist_count += 1
                    except Exception as exc:
                        logger.warning(
                            f"[SupplierSearch] Failed to persist product "
                            f"url={product.product_url!r}: {exc}"
                        )

                # Rate limit between marketplace requests
                time.sleep(self._delay)

        return query_count, result_count, persist_count

    def get_summary_stats(self) -> Dict[str, Any]:
        """Return basic statistics from the DB for reporting."""
        stats = self._db.get_stats()
        return {
            "total_sources":             stats.get("total_sources", 0),
            "total_research_candidates": stats.get("total_research_candidates", 0),
            "total_related_candidates":  stats.get("total_related_candidates", 0),
            "marketplaces_active":       list(self._adapters.keys()),
        }


# ── Module-level singleton ────────────────────────────────────────────────────

_engine_instance: Optional[SupplierSearchEngine] = None


def get_supplier_search_engine(
    db: Optional["Database"] = None,
) -> SupplierSearchEngine:
    """Return the module-level singleton, creating it on first call."""
    global _engine_instance
    if _engine_instance is None:
        if db is None:
            from src.database.database import db as _db
            db = _db
        _engine_instance = SupplierSearchEngine(db=db)
    return _engine_instance
