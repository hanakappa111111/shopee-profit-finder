"""Competition Analyzer AI — automation job.

Standalone entry point that analyses Shopee competition for all profitable
matched products and generates price recommendations.

This job sits at the end of the nightly pipeline (default schedule ``02:30``),
running after the Japan Supplier Search and giving the listing pipeline
market-aware pricing.

Full nightly pipeline order
---------------------------
01:00  research_job.py          — score & discover research candidates
01:30  related_discovery_job.py — expand seeds into related keywords
02:00  supplier_search_job.py   — search Japanese marketplaces
02:30  competition_analysis_job.py  ← this file
(then the listing pipeline consumes price_recommendations)

Scheduling options
------------------
1. **Direct execution**::

       python -m jobs.competition_analysis_job

2. **Cron** (Linux / macOS)::

       30 2 * * * /path/to/.venv/bin/python -m jobs.competition_analysis_job >> /logs/competition.log 2>&1

3. **Plugging into an existing JobScheduler**::

   .. code-block:: python

       import schedule
       from jobs.competition_analysis_job import run_competition_analysis_job
       from src.config.settings import settings

       schedule.every().day.at(settings.COMPETITION_JOB_TIME).do(
           run_competition_analysis_job
       )

Exit codes
----------
0   Job completed successfully.
1   Fatal error (database initialisation failure, unhandled exception).
"""

from __future__ import annotations

import sys
import time
from datetime import datetime

from src.config.settings import settings
from src.database.database import db
from src.competition_analyzer.analyzer_engine import AnalyzerEngine
from src.utils.logger import logger


# ── Job function ──────────────────────────────────────────────────────────────

def run_competition_analysis_job(
    max_products: int | None = None,
    freshness_hours: int | None = None,
) -> dict:
    """Execute one full competition-analysis run.

    Parameters
    ----------
    max_products:
        Override the maximum number of products to analyse.
    freshness_hours:
        Override the competitor-data freshness window.

    Returns
    -------
    dict
        Summary statistics of the run.
    """
    run_start = time.time()
    run_date  = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    effective_max       = max_products    or settings.COMPETITION_MAX_PRODUCTS
    effective_freshness = freshness_hours or settings.COMPETITION_FRESHNESS_HOURS

    logger.info("=" * 60)
    logger.info(f"[CompetitionJob] Starting competition analysis — {run_date}")
    logger.info(
        f"[CompetitionJob] Config: "
        f"max_products={effective_max}, "
        f"freshness_hours={effective_freshness}, "
        f"min_competitors={settings.COMPETITION_MIN_COMPETITORS}, "
        f"median_discount=₱{settings.COMPETITION_MEDIAN_DISCOUNT_PHP:.2f}"
    )
    logger.info("=" * 60)

    # ── 1. Ensure the database schema is current ──────────────────────────────
    try:
        db.initialize()
        logger.info("[CompetitionJob] Database initialized")
    except Exception as exc:
        logger.error(
            f"[CompetitionJob] Database initialisation failed: {exc}",
            exc_info=True,
        )
        return {"error": str(exc)}

    # ── 2. Run the analysis engine ────────────────────────────────────────────
    engine = AnalyzerEngine(
        db=db,
        max_products=max_products,
        freshness_hours=freshness_hours,
    )

    summary = engine.run()
    elapsed = time.time() - run_start
    stats   = engine.get_summary_stats()

    # ── 3. Log summary ────────────────────────────────────────────────────────
    logger.info("─" * 60)
    logger.info(f"[CompetitionJob] Run complete in {elapsed:.2f}s")
    logger.info(f"[CompetitionJob] Products analysed:           {summary.get('products_analysed', 0)}")
    logger.info(f"[CompetitionJob] Products skipped (errors):   {summary.get('products_skipped', 0)}")
    logger.info(f"[CompetitionJob] Competitor rows stored:      {summary.get('competitor_rows_stored', 0)}")
    logger.info(f"[CompetitionJob] Recommendations stored:      {summary.get('recommendations_stored', 0)}")
    logger.info(f"[CompetitionJob] Total competitor_listings:   {stats.get('total_competitor_listings', 0)}")
    logger.info(f"[CompetitionJob] Total price_recommendations: {stats.get('total_price_recommendations', 0)}")

    # ── 4. Log top 3 recommendations ─────────────────────────────────────────
    top = engine.get_recommendations(limit=3)
    if top:
        logger.info("[CompetitionJob] Top recommendations:")
        for rank, rec in enumerate(top, 1):
            logger.info(
                f"  #{rank} product_id={rec.get('shopee_product_id')} "
                f"rec=₱{rec.get('recommended_price', 0):.2f} "
                f"median=₱{rec.get('median_market_price', 0):.2f} "
                f"floor=₱{rec.get('min_viable_price', 0):.2f} "
                f"n={rec.get('competitor_count', 0)}"
            )

    logger.info("─" * 60)
    return summary


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    """Run as ``python -m jobs.competition_analysis_job [options]``."""
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Competition Analyzer AI — analyse Shopee competition and generate "
            "optimal price recommendations"
        )
    )
    parser.add_argument(
        "--max-products",
        type=int,
        default=None,
        help=(
            f"Maximum products to analyse  "
            f"(default: {settings.COMPETITION_MAX_PRODUCTS})"
        ),
    )
    parser.add_argument(
        "--freshness-hours",
        type=int,
        default=None,
        help=(
            f"Competitor data freshness window in hours  "
            f"(default: {settings.COMPETITION_FRESHNESS_HOURS})"
        ),
    )
    args = parser.parse_args()

    try:
        summary = run_competition_analysis_job(
            max_products=args.max_products,
            freshness_hours=args.freshness_hours,
        )
        sys.exit(0)
    except Exception as exc:
        logger.error(f"[CompetitionJob] Fatal error: {exc}", exc_info=True)
        sys.exit(1)
