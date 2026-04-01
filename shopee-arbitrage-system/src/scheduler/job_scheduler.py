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
                    f"₱{result.current_price} -> ₱{result.optimized_price} "
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
            from src.winner_finder.winner_finder import WinnerFinder
            from src.japan_sourcing.japan_sourcer import JapanSourcer
            from src.matcher.listing_matcher import ListingMatcher
            from src.profit_calculator.profit_calc import ProfitCalculator
            from src.listing_generator.generator import ListingGenerator

            # Stage 1: Market Analysis
            logger.info("Pipeline Stage 1: Market analysis")
            market_data = run_market_scraper()
            trends = detect_trends(market_data)
            logger.info(f"  Found {len(trends)} trends")

            # Stage 2: Winner Finding
            logger.info("Pipeline Stage 2: Winner finding")
            winner_finder = WinnerFinder()
            winners = winner_finder.find_winners(market_data)
            logger.info(f"  Found {len(winners)} potential winners")

            # Stage 3: Japan Sourcing
            logger.info("Pipeline Stage 3: Japan sourcing")
            sourcer = JapanSourcer()
            sourced_items = sourcer.source_items(winners)
            logger.info(f"  Sourced {len(sourced_items)} items from Japan")

            # Stage 4: Listing Matching
            logger.info("Pipeline Stage 4: Listing matching")
            matcher = ListingMatcher()
            matched_items = matcher.match_listings(sourced_items, market_data)
            logger.info(f"  Matched {len(matched_items)} items to market")

            # Stage 5: Profit Calculation
            logger.info("Pipeline Stage 5: Profit calculation")
            calculator = ProfitCalculator()
            profitable_items = calculator.calculate_profits(matched_items)
            logger.info(f"  Found {len(profitable_items)} profitable opportunities")

            # Stage 6: Listing Generation
            logger.info("Pipeline Stage 6: Listing generation")
            generator = ListingGenerator()
            generated_count = generator.generate_listings(profitable_items)
            logger.info(f"  Generated {generated_count} Shopee listings")

            elapsed = time.time() - start_time
            logger.info(
                f"Full pipeline completed successfully in {elapsed:.2f}s "
                f"({generated_count} listings generated)"
            )

        except Exception as e:
            logger.error(f"Full pipeline execution failed: {e}", exc_info=True)

    def setup_jobs(self) -> None:
        """
        Configure all scheduled jobs based on settings.

        Sets up recurring jobs for:
        - Market analysis (daily at configured time)
        - Inventory checks (multiple times daily)
        - Price optimization (multiple times daily)
        """
        try:
            logger.info("Setting up scheduled jobs")

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
            job_name: Name of job to run
                     ("market_analysis" | "inventory" | "price_optimize" | "full_pipeline")
        """
        try:
            job_mapping = {
                "market_analysis": self._run_market_analysis,
                "inventory": self._run_inventory_check,
                "price_optimize": self._run_price_optimization,
                "full_pipeline": self._run_full_pipeline,
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
