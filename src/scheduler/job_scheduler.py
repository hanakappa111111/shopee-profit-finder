"""
Job Scheduler Module

Central job scheduler for the Shopee arbitrage system.
Handles scheduling and execution of market analysis, inventory checks,
price optimization, and full pipeline runs.
"""

import asyncio
import schedule
import threading
import time
from typing import Optional

from src.config.settings import settings
from src.utils.logger import logger


class JobScheduler:
    """
    Central scheduler for arbitrage system jobs.

    Manages scheduling of recurring jobs including market analysis, inventory checks,
    price optimization, and full pipeline execution. Uses lazy imports to avoid
    circular dependencies.
    """

    def __init__(self):
        """Initialize the JobScheduler with empty job queue and stopped state."""
        self._jobs = []
        self._running = False
        self._thread: Optional[threading.Thread] = None
        logger.info("JobScheduler initialized")

    def _run_market_analysis(self) -> None:
        """
        Execute market analysis and trend detection.

        Lazy imports pipeline functions to avoid circular imports.
        Scrapes market data and detects trends.
        """
        try:
            logger.info("Starting market analysis job")
            start_time = time.time()

            # Lazy imports
            from src.market_analyzer.shopee_market_scraper import run_market_scraper
            from src.market_analyzer.trend_detector import detect_trends

            # Run market scraper
            market_data = run_market_scraper()
            logger.info(f"Market scraper completed: {len(market_data)} items analyzed")

            # Detect trends
            trends = detect_trends(market_data)
            logger.info(f"Trend detection completed: {len(trends)} trends identified")

            elapsed = time.time() - start_time
            logger.info(f"Market analysis job completed in {elapsed:.2f}s")

        except Exception as e:
            logger.error(f"Market analysis job failed: {e}", exc_info=True)

    def _run_inventory_check(self) -> None:
        """
        Execute inventory monitoring and alerting.

        Lazy imports inventory monitor to check all listings for stock issues.
        Logs alerts for critical inventory levels.
        """
        try:
            logger.info("Starting inventory check job")
            start_time = time.time()

            # Lazy import
            from src.monitoring.inventory_monitor import InventoryMonitor

            monitor = InventoryMonitor()
            alerts = monitor.check_all()
            logger.info(f"Inventory check completed: {len(alerts)} alerts generated")

            for alert in alerts:
                logger.warning(f"Inventory alert: {alert}")

            elapsed = time.time() - start_time
            logger.info(f"Inventory check job completed in {elapsed:.2f}s")

        except Exception as e:
            logger.error(f"Inventory check job failed: {e}", exc_info=True)

    def _run_price_optimization(self) -> None:
        """
        Execute price optimization for all active listings.

        Lazy imports price optimizer and runs optimization in dry-run mode
        (suggestions only, not applied to listings).
        """
        try:
            logger.info("Starting price optimization job (dry-run)")
            start_time = time.time()

            # Lazy import
            from src.optimizer.price_optimizer import PriceOptimizer

            optimizer = PriceOptimizer()
            results = optimizer.optimize_all_active_listings(apply=False)
            logger.info(f"Price optimization completed: {len(results)} recommendations")

            for result in results:
                logger.info(
                    f"  Listing {result.listing_id}: "
                    f"₱{result.current_price} -> ₱{result.suggested_price} "
                    f"(competitor: ₱{result.competitor_price})"
                )

            elapsed = time.time() - start_time
            logger.info(f"Price optimization job completed in {elapsed:.2f}s")

        except Exception as e:
            logger.error(f"Price optimization job failed: {e}", exc_info=True)

    def _run_full_pipeline(self) -> None:
        """
        Execute the complete arbitrage pipeline.

        Runs all pipeline stages in sequence:
        1. Market analysis
        2. Winner finding
        3. Japan sourcing
        4. Listing matching
        5. Profit calculation
        6. Listing generation

        Logs timing for full execution.
        """
        try:
            logger.info("Starting full pipeline execution")
            start_time = time.time()

            # Lazy imports for all pipeline stages
            from src.market_analyzer.shopee_market_scraper import run_market_scraper
            from src.market_analyzer.trend_detector import detect_trends
            from src.database.database import db
            from src.research_ai.research_engine import ResearchEngine
            from src.related_discovery.discovery_engine import DiscoveryEngine
            from src.supplier_search.search_engine import SupplierSearchEngine
            from src.matching.product_matcher import ProductMatcher
            from src.profit.profit_engine import ProfitEngine
            from src.competition_analyzer.analyzer_engine import AnalyzerEngine

            # Stage 1: Market Analysis
            logger.info("Pipeline Stage 1: Market analysis")
            market_data = run_market_scraper()
            trends = detect_trends(market_data)
            logger.info(f"  Found {len(trends)} trends")

            # Stage 2: Research AI — identify promising candidates
            logger.info("Pipeline Stage 2: Research AI")
            research_engine = ResearchEngine(db=db)
            candidate_count = research_engine.scan()
            logger.info(f"  Identified {candidate_count} research candidates")

            # Stage 3: Related Discovery — expand keywords
            logger.info("Pipeline Stage 3: Related Discovery")
            discovery_engine = DiscoveryEngine(db=db)
            keyword_count = discovery_engine.run()
            logger.info(f"  Generated {keyword_count} related keywords")

            # Stage 4: Supplier Search — find Japan sources
            logger.info("Pipeline Stage 4: Supplier Search")
            search_engine = SupplierSearchEngine(db=db)
            search_summary = search_engine.run()
            logger.info(f"  Persisted {search_summary.get('total_persisted', 0)} sources")

            # Stage 5: Matching — match Shopee products to Japan sources
            logger.info("Pipeline Stage 5: Product Matching")
            matcher = ProductMatcher(db=db)
            match_results = matcher.match_all()
            logger.info(f"  Found {len(match_results)} matches")

            # Stage 6: Profit Calculation
            logger.info("Pipeline Stage 6: Profit Calculation")
            profit_engine = ProfitEngine(db=db)
            profit_count = profit_engine.analyze_all()
            logger.info(f"  Analyzed {profit_count} profitable opportunities")

            # Stage 7: Competition Analysis
            logger.info("Pipeline Stage 7: Competition Analysis")
            analyzer = AnalyzerEngine(db=db)
            analysis_count = analyzer.run()
            logger.info(f"  Analyzed competition for {analysis_count} products")

            elapsed = time.time() - start_time
            logger.info(
                f"Full pipeline completed successfully in {elapsed:.2f}s "
                f"(matches={len(match_results)}, profitable={profit_count})"
            )

        except Exception as e:
            logger.error(f"Full pipeline execution failed: {e}", exc_info=True)

    # ── AI Pipeline Jobs ──────────────────────────────────────────────────────

    def _run_research_ai(self) -> None:
        """Execute Research AI scan to identify promising candidates."""
        try:
            logger.info("Starting Research AI job")
            start_time = time.time()

            from src.database.database import db
            from src.research_ai.research_engine import ResearchEngine

            engine = ResearchEngine(db=db)
            count = engine.scan()

            elapsed = time.time() - start_time
            logger.info(f"Research AI job completed in {elapsed:.2f}s — {count} candidates")

        except Exception as e:
            logger.error(f"Research AI job failed: {e}", exc_info=True)

    def _run_discovery_ai(self) -> None:
        """Execute Related Product Discovery AI."""
        try:
            logger.info("Starting Discovery AI job")
            start_time = time.time()

            from src.database.database import db
            from src.related_discovery.discovery_engine import DiscoveryEngine

            engine = DiscoveryEngine(db=db)
            count = engine.run()

            elapsed = time.time() - start_time
            logger.info(f"Discovery AI job completed in {elapsed:.2f}s — {count} keywords")

        except Exception as e:
            logger.error(f"Discovery AI job failed: {e}", exc_info=True)

    def _run_supplier_search(self) -> None:
        """Execute Japan Supplier Search AI."""
        try:
            logger.info("Starting Supplier Search job")
            start_time = time.time()

            from src.database.database import db
            from src.supplier_search.search_engine import SupplierSearchEngine

            engine = SupplierSearchEngine(db=db)
            summary = engine.run()

            elapsed = time.time() - start_time
            logger.info(
                f"Supplier Search job completed in {elapsed:.2f}s — "
                f"persisted={summary.get('total_persisted', 0)}"
            )

        except Exception as e:
            logger.error(f"Supplier Search job failed: {e}", exc_info=True)

    def _run_competition_analysis(self) -> None:
        """Execute Competition Analyzer AI."""
        try:
            logger.info("Starting Competition Analysis job")
            start_time = time.time()

            from src.database.database import db
            from src.competition_analyzer.analyzer_engine import AnalyzerEngine

            engine = AnalyzerEngine(db=db)
            count = engine.run()

            elapsed = time.time() - start_time
            logger.info(f"Competition Analysis job completed in {elapsed:.2f}s — {count} products")

        except Exception as e:
            logger.error(f"Competition Analysis job failed: {e}", exc_info=True)

    def _run_snapshot_cleanup(self) -> None:
        """Purge old product snapshots beyond the retention window."""
        try:
            logger.info("Starting snapshot cleanup job")

            from src.database.database import db

            deleted = db.purge_old_snapshots(
                retention_days=settings.SNAPSHOT_RETENTION_DAYS
            )
            logger.info(f"Snapshot cleanup complete — {deleted} rows purged")

        except Exception as e:
            logger.error(f"Snapshot cleanup job failed: {e}", exc_info=True)

    # ── Supplier Monitor Jobs ──────────────────────────────────────────────────

    def _run_supplier_price_monitor(self) -> None:
        """Execute one supplier price monitoring cycle.

        Fetches current Japan-side prices for all active ProductMatches,
        records snapshots, recalculates profit, and pauses or reprices
        Shopee listings whose profit fell below the minimum threshold.

        Runs every ``settings.SUPPLIER_PRICE_MONITOR_HOURS`` hours.
        """
        try:
            logger.info("Starting supplier price monitor job")
            start_time = time.time()

            from src.supplier_monitor.monitor_engine import get_monitor_engine

            engine = get_monitor_engine()
            alerts = engine.run_price_check()

            elapsed = time.time() - start_time
            logger.info(
                f"Supplier price monitor job completed in {elapsed:.2f}s "
                f"— {len(alerts)} alert(s)"
            )

        except Exception as e:
            logger.error(f"Supplier price monitor job failed: {e}", exc_info=True)

    def _run_supplier_inventory_monitor(self) -> None:
        """Execute one supplier inventory monitoring cycle.

        Fetches current Japan-side stock status for all active ProductMatches,
        records snapshots, and pauses Shopee listings whose supplier went
        out of stock.  Restock events are logged for manual review but do
        NOT auto-resume listings.

        Runs every ``settings.SUPPLIER_INVENTORY_MONITOR_HOURS`` hours.
        """
        try:
            logger.info("Starting supplier inventory monitor job")
            start_time = time.time()

            from src.supplier_monitor.monitor_engine import get_monitor_engine

            engine = get_monitor_engine()
            alerts = engine.run_inventory_check()

            elapsed = time.time() - start_time
            logger.info(
                f"Supplier inventory monitor job completed in {elapsed:.2f}s "
                f"— {len(alerts)} alert(s)"
            )

        except Exception as e:
            logger.error(f"Supplier inventory monitor job failed: {e}", exc_info=True)

    # ── Job Setup ──────────────────────────────────────────────────────────────

    def setup_jobs(self) -> None:
        """
        Configure all scheduled jobs based on settings.

        Sets up recurring jobs for:
        - AI pipeline: research, discovery, supplier search, competition analysis
        - Market analysis (daily at configured time)
        - Inventory checks (multiple times daily)
        - Price optimization (multiple times daily)
        - Snapshot cleanup (daily)

        When ``settings.AUTOMATION_ENABLED`` is False (the default in
        on-demand mode), this method is a no-op.  All individual job
        methods remain callable via :meth:`run_job_now` for manual
        one-shot execution.
        """
        if not settings.AUTOMATION_ENABLED:
            logger.info(
                "Continuous automation is DISABLED (AUTOMATION_ENABLED=False). "
                "The system is running in on-demand research mode. "
                "Set AUTOMATION_ENABLED=True to re-enable scheduled jobs."
            )
            return

        try:
            logger.info("Setting up scheduled jobs")

            # ── AI pipeline jobs (nightly sequence) ────────────────────────────
            schedule.every().day.at(settings.RESEARCH_JOB_TIME).do(
                self._run_research_ai
            )
            logger.info(f"  Scheduled Research AI at {settings.RESEARCH_JOB_TIME}")

            schedule.every().day.at(settings.DISCOVERY_JOB_TIME).do(
                self._run_discovery_ai
            )
            logger.info(f"  Scheduled Discovery AI at {settings.DISCOVERY_JOB_TIME}")

            schedule.every().day.at(settings.SUPPLIER_SEARCH_JOB_TIME).do(
                self._run_supplier_search
            )
            logger.info(f"  Scheduled Supplier Search at {settings.SUPPLIER_SEARCH_JOB_TIME}")

            schedule.every().day.at(settings.COMPETITION_JOB_TIME).do(
                self._run_competition_analysis
            )
            logger.info(f"  Scheduled Competition Analysis at {settings.COMPETITION_JOB_TIME}")

            # ── Snapshot cleanup (daily at 04:00) ──────────────────────────────
            schedule.every().day.at("04:00").do(self._run_snapshot_cleanup)
            logger.info("  Scheduled snapshot cleanup at 04:00")

            # ── Supplier monitoring ─────────────────────────────────────────────
            schedule.every(settings.SUPPLIER_PRICE_MONITOR_HOURS).hours.do(
                self._run_supplier_price_monitor
            )
            logger.info(
                f"  Scheduled supplier price monitor every "
                f"{settings.SUPPLIER_PRICE_MONITOR_HOURS}h"
            )

            schedule.every(settings.SUPPLIER_INVENTORY_MONITOR_HOURS).hours.do(
                self._run_supplier_inventory_monitor
            )
            logger.info(
                f"  Scheduled supplier inventory monitor every "
                f"{settings.SUPPLIER_INVENTORY_MONITOR_HOURS}h"
            )

            # ── Legacy jobs ────────────────────────────────────────────────────
            # Market analysis - once daily
            schedule.every().day.at(settings.MARKET_ANALYSIS_TIME).do(
                self._run_market_analysis
            )
            logger.info(f"  Scheduled market analysis at {settings.MARKET_ANALYSIS_TIME}")

            # Inventory checks - multiple times daily
            for check_time in settings.INVENTORY_CHECK_TIMES:
                schedule.every().day.at(check_time).do(self._run_inventory_check)
                logger.info(f"  Scheduled inventory check at {check_time}")

            # Price optimization - multiple times daily
            for opt_time in settings.PRICE_OPTIMIZE_TIMES:
                schedule.every().day.at(opt_time).do(self._run_price_optimization)
                logger.info(f"  Scheduled price optimization at {opt_time}")

            logger.info(
                f"Job setup complete: "
                f"4 AI pipeline jobs, 1 snapshot cleanup, "
                f"1 supplier price monitor (every {settings.SUPPLIER_PRICE_MONITOR_HOURS}h), "
                f"1 supplier inventory monitor (every {settings.SUPPLIER_INVENTORY_MONITOR_HOURS}h), "
                f"1 market analysis, "
                f"{len(settings.INVENTORY_CHECK_TIMES)} inventory checks, "
                f"{len(settings.PRICE_OPTIMIZE_TIMES)} price optimizations"
            )

        except Exception as e:
            logger.error(f"Error setting up jobs: {e}", exc_info=True)

    def start(self, run_immediately: bool = False) -> None:
        """
        Start the scheduler with configured jobs.

        Runs an infinite loop that executes pending jobs every minute.
        Can optionally run full pipeline immediately before scheduling.

        Args:
            run_immediately: If True, run full pipeline before entering schedule loop
        """
        if not settings.AUTOMATION_ENABLED:
            logger.info(
                "Scheduler.start() called but AUTOMATION_ENABLED=False. "
                "No jobs will run.  Use run_research_pipeline(keyword) for "
                "on-demand research, or set AUTOMATION_ENABLED=True to "
                "restore the continuous pipeline."
            )
            return

        try:
            self._running = True
            self.setup_jobs()

            # Run full pipeline immediately if requested
            if run_immediately:
                logger.info("Running full pipeline immediately")
                self._run_full_pipeline()

            logger.info("Scheduler started - entering main loop")

            # Main scheduler loop
            while self._running:
                schedule.run_pending()
                time.sleep(60)  # Check for pending jobs every minute

        except KeyboardInterrupt:
            logger.info("Scheduler interrupted by user")
        except Exception as e:
            logger.error(f"Scheduler error: {e}", exc_info=True)
        finally:
            self._running = False
            logger.info("Scheduler stopped")

    def start_in_background(self) -> threading.Thread:
        """
        Start the scheduler in a background daemon thread.

        Useful for running the scheduler alongside other application components.

        Returns:
            Reference to the running daemon thread
        """
        try:
            logger.info("Starting scheduler in background")

            self._thread = threading.Thread(
                target=self.start,
                daemon=True,
                name="JobSchedulerThread",
            )
            self._thread.start()

            logger.info("Scheduler running in background thread")
            return self._thread

        except Exception as e:
            logger.error(f"Error starting background scheduler: {e}", exc_info=True)
            return None

    def run_job_now(self, job_name: str) -> None:
        """
        Manually trigger a specific job immediately.

        Args:
            job_name: Name of job to run.  One of:
                "market_analysis" | "inventory" | "price_optimize" | "full_pipeline" |
                "research_ai" | "discovery_ai" | "supplier_search" |
                "competition_analysis" | "snapshot_cleanup" |
                "supplier_price_monitor" | "supplier_inventory_monitor"
        """
        try:
            job_mapping = {
                "market_analysis": self._run_market_analysis,
                "inventory": self._run_inventory_check,
                "price_optimize": self._run_price_optimization,
                "full_pipeline": self._run_full_pipeline,
                "research_ai": self._run_research_ai,
                "discovery_ai": self._run_discovery_ai,
                "supplier_search": self._run_supplier_search,
                "competition_analysis": self._run_competition_analysis,
                "snapshot_cleanup": self._run_snapshot_cleanup,
                "supplier_price_monitor": self._run_supplier_price_monitor,
                "supplier_inventory_monitor": self._run_supplier_inventory_monitor,
            }

            if job_name not in job_mapping:
                logger.error(
                    f"Unknown job name: {job_name}. "
                    f"Valid options: {list(job_mapping.keys())}"
                )
                return

            logger.info(f"Manually triggering job: {job_name}")
            job_mapping[job_name]()

        except Exception as e:
            logger.error(f"Error running job '{job_name}': {e}", exc_info=True)


# Module-level singleton instance
scheduler = JobScheduler()
