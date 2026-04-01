"""Daily Research AI automation job.

Standalone entry point that scans the Shopee product catalogue, scores each
product for arbitrage potential, and persists high-scoring candidates to the
``research_candidates`` table.

Scheduling options
------------------
1. **Direct execution** (development / manual trigger)::

       python -m jobs.research_job

2. **Cron** (Linux / macOS) — add to ``crontab -e``::

       0 1 * * * /path/to/.venv/bin/python -m jobs.research_job >> /logs/research_job.log 2>&1

3. **Plugging into the existing JobScheduler** (no-source-modification pattern).
   The scheduler is designed for easy extension.  In ``main.py`` or your
   application bootstrap, after the scheduler is constructed:

   .. code-block:: python

       import schedule
       from jobs.research_job import run_research_job
       from src.config.settings import settings

       schedule.every().day.at(settings.RESEARCH_JOB_TIME).do(run_research_job)

   This wires the job at ``settings.RESEARCH_JOB_TIME`` (default ``"01:00"``)
   without touching the ``JobScheduler`` source.

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
from src.research_ai.research_engine import ResearchEngine
from src.utils.logger import logger


# ── Job function ──────────────────────────────────────────────────────────────

def run_research_job(
    min_score: float | None = None,
    max_candidates: int | None = None,
    market: str | None = None,
) -> int:
    """Execute one full Research AI scan and return the number of candidates found.

    Parameters
    ----------
    min_score:
        Override the minimum score threshold.  Defaults to
        ``settings.RESEARCH_MIN_SCORE``.
    max_candidates:
        Override the per-run candidate cap.  Defaults to
        ``settings.RESEARCH_MAX_CANDIDATES``.
    market:
        Restrict to one Shopee market code (e.g. ``'PH'``, ``'SG'``).
        Defaults to all markets.

    Returns
    -------
    int
        Number of research candidates upserted in this run.
    """
    run_start = time.time()
    run_date  = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    logger.info("=" * 60)
    logger.info(f"[ResearchJob] Starting daily research scan — {run_date}")
    logger.info(
        f"[ResearchJob] Config: min_score={min_score or settings.RESEARCH_MIN_SCORE}, "
        f"max_candidates={max_candidates or settings.RESEARCH_MAX_CANDIDATES}, "
        f"market={market or 'all'}, "
        f"min_sales={settings.RESEARCH_MIN_SALES}, "
        f"window_days={settings.RESEARCH_SCORE_WINDOW_DAYS}"
    )
    logger.info("=" * 60)

    # ── 1. Ensure the database schema is current ──────────────────────────────
    try:
        db.initialize()
        logger.info("[ResearchJob] Database initialized")
    except Exception as exc:
        logger.error(f"[ResearchJob] Database initialisation failed: {exc}", exc_info=True)
        return 0

    # ── 2. Run the Research AI scan ───────────────────────────────────────────
    engine = ResearchEngine(
        db=db,
        min_score=min_score,
        max_candidates=max_candidates,
    )

    candidates = engine.scan(market=market)

    # ── 3. Log summary statistics ─────────────────────────────────────────────
    stats = engine.get_summary_stats()
    elapsed = time.time() - run_start

    logger.info("─" * 60)
    logger.info(f"[ResearchJob] Scan complete in {elapsed:.2f}s")
    logger.info(f"[ResearchJob] Candidates this run:  {len(candidates)}")
    logger.info(f"[ResearchJob] Total pending:        {stats['research_pending']}")
    logger.info(f"[ResearchJob] Total matched:        {stats['research_matched']}")
    logger.info(f"[ResearchJob] Total rejected:       {stats['research_rejected']}")

    if candidates:
        top3 = candidates[:3]
        logger.info("[ResearchJob] Top candidates this run:")
        for rank, c in enumerate(top3, 1):
            logger.info(
                f"  #{rank} product_id={c.shopee_product_id} "
                f"score={c.research_score:.1f} "
                f"| {c.reason}"
            )

    logger.info("─" * 60)

    return len(candidates)


# ── Score distribution helper ─────────────────────────────────────────────────

def _log_score_distribution(candidates: list) -> None:
    """Log a simple histogram of research scores for monitoring."""
    if not candidates:
        return
    buckets = {"90-100": 0, "70-89": 0, "50-69": 0, "<50": 0}
    for c in candidates:
        s = c.research_score
        if s >= 90:
            buckets["90-100"] += 1
        elif s >= 70:
            buckets["70-89"] += 1
        elif s >= 50:
            buckets["50-69"] += 1
        else:
            buckets["<50"] += 1
    logger.info(
        "[ResearchJob] Score distribution: "
        + " | ".join(f"{k}: {v}" for k, v in buckets.items())
    )


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    """Run as ``python -m jobs.research_job [--market PH] [--min-score 60]``."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Daily Research AI scan — discover arbitrage candidates"
    )
    parser.add_argument(
        "--market",
        default=None,
        help="Restrict to one Shopee market code: PH | SG | MY  (default: all)",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=None,
        help=f"Minimum research score 0-100  (default: {settings.RESEARCH_MIN_SCORE})",
    )
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=None,
        help=f"Per-run candidate cap  (default: {settings.RESEARCH_MAX_CANDIDATES})",
    )
    args = parser.parse_args()

    try:
        count = run_research_job(
            min_score=args.min_score,
            max_candidates=args.max_candidates,
            market=args.market,
        )
        sys.exit(0)
    except Exception as exc:
        logger.error(f"[ResearchJob] Fatal error: {exc}", exc_info=True)
        sys.exit(1)
