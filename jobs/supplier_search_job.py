"""Japan Supplier Search AI — automation job.

Standalone entry point that reads high-scoring seeds from
``research_candidates`` and ``related_product_candidates``, generates
search queries, scrapes Japanese marketplaces, and persists results
to the ``sources`` table (JapanProducts).

This job is designed to run **after** ``related_discovery_job.py`` in the
nightly pipeline (default schedule ``02:00``).

Scheduling options
------------------
1. **Direct execution** (development / manual trigger)::

       python -m jobs.supplier_search_job

2. **Cron** (Linux / macOS)::

       0 2 * * * /path/to/.venv/bin/python -m jobs.supplier_search_job >> /logs/supplier_search.log 2>&1

3. **Plugging into an existing JobScheduler**::

   .. code-block:: python

       import schedule
       from jobs.supplier_search_job import run_supplier_search_job
       from src.config.settings import settings

       schedule.every().day.at(settings.SUPPLIER_SEARCH_JOB_TIME).do(
           run_supplier_search_job
       )

Exit codes
----------
0   Job completed successfully (even if zero products were found).
1   Fatal error (database initialisation failure, unhandled exception).
"""

from __future__ import annotations

import sys
import time
from datetime import datetime

from src.config.settings import settings
from src.database.database import db
from src.supplier_search.search_engine import SupplierSearchEngine
from src.utils.logger import logger


# ── Job function ──────────────────────────────────────────────────────────────

def run_supplier_search_job(
    seed_min_score: float | None = None,
    related_min_confidence: float | None = None,
    max_seeds: int | None = None,
    marketplaces: list[str] | None = None,
    search_related: bool | None = None,
) -> dict:
    """Execute one full Supplier Search run.

    Parameters
    ----------
    seed_min_score:
        Override minimum ``research_score`` for ResearchCandidate seeds.
    related_min_confidence:
        Override minimum ``confidence_score`` for RelatedProductCandidate seeds.
    max_seeds:
        Hard cap on total seeds processed in this run.
    marketplaces:
        Override which marketplaces to search.
    search_related:
        Whether to search RelatedProductCandidates.

    Returns
    -------
    dict
        Summary statistics of the run.
    """
    run_start = time.time()
    run_date  = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    effective_min_score = seed_min_score if seed_min_score is not None else settings.SUPPLIER_SEED_MIN_SCORE
    effective_rel_conf  = related_min_confidence if related_min_confidence is not None else settings.SUPPLIER_RELATED_MIN_CONFIDENCE
    effective_max_seeds = max_seeds or settings.SUPPLIER_MAX_SEEDS
    effective_mp        = marketplaces or settings.SUPPLIER_MARKETPLACES

    logger.info("=" * 60)
    logger.info(f"[SupplierSearchJob] Starting supplier search — {run_date}")
    logger.info(
        f"[SupplierSearchJob] Config: "
        f"seed_min_score={effective_min_score}, "
        f"related_min_conf={effective_rel_conf}, "
        f"max_seeds={effective_max_seeds}, "
        f"marketplaces={effective_mp}, "
        f"search_related={search_related if search_related is not None else settings.SUPPLIER_SEARCH_RELATED}"
    )
    logger.info("=" * 60)

    # ── 1. Ensure the database schema is current ──────────────────────────────
    try:
        db.initialize()
        logger.info("[SupplierSearchJob] Database initialized")
    except Exception as exc:
        logger.error(
            f"[SupplierSearchJob] Database initialisation failed: {exc}",
            exc_info=True,
        )
        return {"error": str(exc)}

    # ── 2. Run the supplier search engine ─────────────────────────────────────
    engine = SupplierSearchEngine(
        db=db,
        marketplaces=marketplaces,
        max_queries_per_seed=settings.SUPPLIER_MAX_QUERIES_PER_SEED,
        request_delay=settings.SUPPLIER_REQUEST_DELAY,
    )

    summary = engine.run(
        seed_min_score=seed_min_score,
        related_min_confidence=related_min_confidence,
        max_seeds=max_seeds,
        search_related=search_related,
    )

    # ── 3. Log summary statistics ─────────────────────────────────────────────
    elapsed = time.time() - run_start
    stats   = engine.get_summary_stats()

    logger.info("─" * 60)
    logger.info(f"[SupplierSearchJob] Run complete in {elapsed:.2f}s")
    logger.info(f"[SupplierSearchJob] Seeds processed:      {summary.get('seeds_processed', 0)}")
    logger.info(f"[SupplierSearchJob] Queries executed:      {summary.get('total_queries', 0)}")
    logger.info(f"[SupplierSearchJob] Results found:         {summary.get('total_results', 0)}")
    logger.info(f"[SupplierSearchJob] Products persisted:    {summary.get('total_persisted', 0)}")
    logger.info(f"[SupplierSearchJob] Total sources in DB:   {stats.get('total_sources', 0)}")
    logger.info(f"[SupplierSearchJob] Marketplaces:          {summary.get('marketplaces_active', [])}")
    logger.info("─" * 60)

    return summary


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    """Run as ``python -m jobs.supplier_search_job [options]``."""
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Japan Supplier Search AI — discover Japanese marketplace suppliers "
            "for research candidates and related products"
        )
    )
    parser.add_argument(
        "--seed-min-score",
        type=float,
        default=None,
        help=(
            f"Minimum research_score for seeds 0-100  "
            f"(default: {settings.SUPPLIER_SEED_MIN_SCORE})"
        ),
    )
    parser.add_argument(
        "--related-min-confidence",
        type=float,
        default=None,
        help=(
            f"Minimum confidence for related keywords 0-100  "
            f"(default: {settings.SUPPLIER_RELATED_MIN_CONFIDENCE})"
        ),
    )
    parser.add_argument(
        "--max-seeds",
        type=int,
        default=None,
        help=(
            f"Maximum seeds to process  "
            f"(default: {settings.SUPPLIER_MAX_SEEDS})"
        ),
    )
    parser.add_argument(
        "--marketplaces",
        nargs="+",
        default=None,
        choices=["amazon_jp", "rakuten", "yahoo_shopping", "mercari"],
        help=(
            f"Marketplaces to search  "
            f"(default: {' '.join(settings.SUPPLIER_MARKETPLACES)})"
        ),
    )
    parser.add_argument(
        "--no-related",
        action="store_true",
        default=False,
        help="Skip searching RelatedProductCandidates (research seeds only)",
    )
    args = parser.parse_args()

    search_related = not args.no_related

    try:
        summary = run_supplier_search_job(
            seed_min_score=args.seed_min_score,
            related_min_confidence=args.related_min_confidence,
            max_seeds=args.max_seeds,
            marketplaces=args.marketplaces,
            search_related=search_related,
        )
        sys.exit(0)
    except Exception as exc:
        logger.error(f"[SupplierSearchJob] Fatal error: {exc}", exc_info=True)
        sys.exit(1)
