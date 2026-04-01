"""Related Product Discovery AI — automation job.

Standalone entry point that reads high-scoring ``ResearchCandidates`` as seeds
and expands them into related search keywords persisted in
``related_product_candidates``.

This job is designed to run **after** ``research_job.py`` in the nightly
pipeline (default schedule ``01:30``).

Scheduling options
------------------
1. **Direct execution** (development / manual trigger)::

       python -m jobs.related_discovery_job

2. **Cron** (Linux / macOS)::

       30 1 * * * /path/to/.venv/bin/python -m jobs.related_discovery_job >> /logs/discovery_job.log 2>&1

3. **Plugging into an existing JobScheduler** (no-source-modification pattern)::

   .. code-block:: python

       import schedule
       from jobs.related_discovery_job import run_discovery_job
       from src.config.settings import settings

       schedule.every().day.at(settings.DISCOVERY_JOB_TIME).do(run_discovery_job)

Exit codes
----------
0   Job completed successfully (even if zero candidates were found).
1   Fatal error (database initialisation failure, unhandled exception).
"""

from __future__ import annotations

import sys
import time
from datetime import datetime

from src.config.settings import settings
from src.database.database import db
from src.related_discovery.discovery_engine import DiscoveryEngine
from src.utils.logger import logger


# ── Job function ──────────────────────────────────────────────────────────────

def run_discovery_job(
    seed_min_score: float | None = None,
    min_confidence: float | None = None,
    max_keywords_per_seed: int | None = None,
    seed_status: str | None = "pending",
    seed_limit: int = 200,
) -> int:
    """Execute one full Related Product Discovery run.

    Parameters
    ----------
    seed_min_score:
        Override minimum ``research_score`` for seeds.  Defaults to
        ``settings.DISCOVERY_SEED_MIN_SCORE``.
    min_confidence:
        Override minimum confidence for generated keywords.  Defaults to
        ``settings.DISCOVERY_MIN_CONFIDENCE``.
    max_keywords_per_seed:
        Override per-seed keyword cap.  Defaults to
        ``settings.DISCOVERY_MAX_KEYWORDS_PER_SEED``.
    seed_status:
        Filter seeds by ``research_candidates.status``.  Pass ``None`` for all.
        Defaults to ``'pending'``.
    seed_limit:
        Maximum number of seeds to process.  Defaults to 200.

    Returns
    -------
    int
        Total number of ``related_product_candidates`` rows upserted.
    """
    run_start = time.time()
    run_date  = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    effective_min_score = seed_min_score if seed_min_score is not None else settings.DISCOVERY_SEED_MIN_SCORE
    effective_min_conf  = min_confidence if min_confidence is not None else settings.DISCOVERY_MIN_CONFIDENCE
    effective_max_kw    = max_keywords_per_seed or settings.DISCOVERY_MAX_KEYWORDS_PER_SEED

    logger.info("=" * 60)
    logger.info(f"[DiscoveryJob] Starting related-product discovery — {run_date}")
    logger.info(
        f"[DiscoveryJob] Config: "
        f"seed_min_score={effective_min_score}, "
        f"min_confidence={effective_min_conf}, "
        f"max_keywords_per_seed={effective_max_kw}, "
        f"seed_status={seed_status!r}, "
        f"seed_limit={seed_limit}"
    )
    logger.info("=" * 60)

    # ── 1. Ensure the database schema is current ──────────────────────────────
    try:
        db.initialize()
        logger.info("[DiscoveryJob] Database initialized")
    except Exception as exc:
        logger.error(
            f"[DiscoveryJob] Database initialisation failed: {exc}",
            exc_info=True,
        )
        return 0

    # ── 2. Run the discovery engine ───────────────────────────────────────────
    engine = DiscoveryEngine(
        db=db,
        seed_min_score=seed_min_score,
        min_confidence=min_confidence,
        max_keywords_per_seed=max_keywords_per_seed,
    )

    total_upserted = engine.run(
        seed_status=seed_status,
        seed_limit=seed_limit,
    )

    # ── 3. Log summary statistics ─────────────────────────────────────────────
    stats   = engine.get_summary_stats()
    elapsed = time.time() - run_start

    logger.info("─" * 60)
    logger.info(f"[DiscoveryJob] Run complete in {elapsed:.2f}s")
    logger.info(f"[DiscoveryJob] Keywords upserted this run:  {total_upserted}")
    logger.info(
        f"[DiscoveryJob] Total related_product_candidates in DB: "
        f"{stats['total_related_candidates']}"
    )
    logger.info(
        f"[DiscoveryJob] Total research_candidates in DB: "
        f"{stats['total_research_candidates']}"
    )

    # ── 4. Log top keywords (first 5) for quick inspection ───────────────────
    if total_upserted > 0:
        top_rows = engine.get_candidates(min_confidence=effective_min_conf, limit=5)
        if top_rows:
            logger.info("[DiscoveryJob] Sample keywords persisted:")
            for row in top_rows:
                logger.info(
                    f"  seed_id={row.get('seed_product_id')} "
                    f"method={row.get('discovery_method')} "
                    f"conf={row.get('confidence_score', 0):.0f} "
                    f"kw={row.get('related_keyword', '')!r}"
                )

    logger.info("─" * 60)
    return total_upserted


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    """Run as ``python -m jobs.related_discovery_job [options]``."""
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Related Product Discovery AI — expand research candidates into "
            "Japan-side search keywords"
        )
    )
    parser.add_argument(
        "--seed-min-score",
        type=float,
        default=None,
        help=(
            f"Minimum research_score for seeds 0-100  "
            f"(default: {settings.DISCOVERY_SEED_MIN_SCORE})"
        ),
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=None,
        help=(
            f"Minimum keyword confidence 0-100  "
            f"(default: {settings.DISCOVERY_MIN_CONFIDENCE})"
        ),
    )
    parser.add_argument(
        "--max-keywords",
        type=int,
        default=None,
        dest="max_keywords_per_seed",
        help=(
            f"Max keywords per seed  "
            f"(default: {settings.DISCOVERY_MAX_KEYWORDS_PER_SEED})"
        ),
    )
    parser.add_argument(
        "--seed-status",
        default="pending",
        choices=["pending", "matched", "rejected", "all"],
        help="Filter seeds by research_candidates.status  (default: pending)",
    )
    parser.add_argument(
        "--seed-limit",
        type=int,
        default=200,
        help="Maximum number of seeds to process  (default: 200)",
    )
    args = parser.parse_args()

    # Map "all" CLI value to None (no status filter)
    seed_status = None if args.seed_status == "all" else args.seed_status

    try:
        count = run_discovery_job(
            seed_min_score=args.seed_min_score,
            min_confidence=args.min_confidence,
            max_keywords_per_seed=args.max_keywords_per_seed,
            seed_status=seed_status,
            seed_limit=args.seed_limit,
        )
        sys.exit(0)
    except Exception as exc:
        logger.error(f"[DiscoveryJob] Fatal error: {exc}", exc_info=True)
        sys.exit(1)
