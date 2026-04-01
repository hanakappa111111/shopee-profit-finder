"""
Shopee Arbitrage System — Main Entry Point
==========================================
Full automation pipeline:

  1. Scrape Shopee market data
  2. Detect trending products
  3. Identify winning products (high-margin candidates)
  4. Search Japan e-commerce (Amazon JP, Rakuten, Yahoo, Mercari)
  5. Match products using RapidFuzz similarity
  6. Calculate profit & ROI
  7. Generate AI-optimised Shopee listings
  8. Save profitable candidates to database
  9. Schedule recurring jobs (monitors, price optimisation)

Run: python main.py [command]
"""

from __future__ import annotations

import asyncio
import sys
import time
from typing import List

from src.config.settings import settings
from src.database.database import db
from src.database.models import (
    JapanProduct,
    ProfitResult,
    ShopeeListing,
    ShopeeProduct,
    WinningProduct,
)
from src.utils.logger import logger


# ══════════════════════════════════════════════════════════════════════════════
# Individual pipeline steps
# ══════════════════════════════════════════════════════════════════════════════

async def step1_scrape_market() -> List[ShopeeProduct]:
    """Step 1 — Scrape Shopee for current market data."""
    from src.market_analyzer.shopee_market_scraper import ShopeeMarketScraper

    logger.info("━━━ Step 1: Shopee Market Scrape ━━━")
    async with ShopeeMarketScraper() as scraper:
        products = await scraper.scrape_all_keywords()
        scraper.save_products(products)
    logger.success(f"  ✅ {len(products)} products scraped")
    return products


def step2_detect_trends(products: List[ShopeeProduct]) -> list:
    """Step 2 — Compute trend scores from scraped data."""
    from src.market_analyzer.trend_detector import detect_trends

    logger.info("━━━ Step 2: Trend Detection ━━━")
    product_rows = db.get_products(
        min_sales=50, limit=len(products) * 2
    )
    trends = detect_trends(product_rows)
    rising = [t for t in trends if t.trend_direction == "rising"]
    logger.success(
        f"  ✅ {len(trends)} trend scores computed | {len(rising)} rising"
    )
    return trends


def step3_find_winners(trends: list) -> List[WinningProduct]:
    """Step 3 — Identify high-margin winning products."""
    from src.product_finder.winning_product_finder import WinningProductFinder

    logger.info("━━━ Step 3: Winning Product Detection ━━━")
    finder = WinningProductFinder()
    product_rows = db.get_products(
        min_sales=settings.MIN_SALES_COUNT,
        min_rating=settings.MIN_RATING,
        min_price=settings.MIN_PRICE_PHP,
    )
    # Build trend lookup
    trend_map = {t.product_url: t for t in trends}
    winners = finder.find_winners(
        product_rows,
        trends=list(trend_map.values()),
    )
    logger.success(f"  ✅ {len(winners)} winning products identified")
    for w in winners[:5]:
        logger.info(
            f"    🏆 [{w.win_score:.0f}] "
            f"{w.product.title[:55]} | ₱{w.product.price:,.0f} | "
            f"{w.product.sales_count} sold"
        )
    return winners


def step4_source_japan(winners: List[WinningProduct]) -> List[JapanProduct]:
    """Step 4 — Search Japanese platforms for winner products."""
    from src.japan_source.mercari_scraper import JapanSourceSearcher
    from src.product_finder.related_product_engine import RelatedProductEngine

    logger.info("━━━ Step 4: Japan Sourcing ━━━")
    searcher = JapanSourceSearcher()
    engine = RelatedProductEngine()
    all_jp: List[JapanProduct] = []
    seen_urls: set[str] = set()

    for winner in winners:
        queries = engine.generate_japan_search_queries(winner)
        for query in queries[:2]:  # Limit queries per winner
            try:
                results = searcher.search(query, limit=settings.JAPAN_RESULTS_LIMIT)
                for p in results:
                    if p.product_url not in seen_urls:
                        seen_urls.add(p.product_url)
                        all_jp.append(p)
                        db.upsert_source(p)
            except Exception as exc:
                logger.error(f"Japan search error '{query[:40]}': {exc}")

    logger.success(f"  ✅ {len(all_jp)} Japan products found & saved")
    return all_jp


def step5_match_products(
    winners: List[WinningProduct], japan_products: List[JapanProduct]
) -> list:
    """Step 5 — Match Shopee winners with Japan sourcing products."""
    from src.matching.product_matcher import ProductMatcher

    logger.info("━━━ Step 5: Product Matching ━━━")
    matcher = ProductMatcher(threshold=settings.MIN_MATCH_SIMILARITY)
    shopee_products = [w.product for w in winners]
    matches = matcher.find_matches(shopee_products, japan_products)
    logger.success(
        f"  ✅ {len(matches)} matches "
        f"(threshold={settings.MIN_MATCH_SIMILARITY})"
    )
    return matches


def step6_calculate_profit(matches: list) -> List[ProfitResult]:
    """Step 6 — Calculate profit & ROI, filter by thresholds."""
    from src.profit.profit_engine import ProfitEngine

    logger.info("━━━ Step 6: Profit Analysis ━━━")
    engine = ProfitEngine()
    all_results = engine.calculate_many(matches)
    profitable = engine.filter_profitable(all_results)

    for r in profitable[:5]:
        logger.info(
            f"    💰 ¥{r.profit_jpy:,.0f} | ROI {r.roi_percent:.0f}% | "
            f"{r.shopee_product.title[:50]}"
        )

    for r in profitable:
        try:
            db.upsert_match(r)
        except Exception as exc:
            logger.debug(f"Match DB save: {exc}")

    logger.success(
        f"  ✅ {len(profitable)}/{len(all_results)} results above "
        f"¥{settings.MIN_PROFIT_YEN:,.0f} & {settings.MIN_ROI_PERCENT}% ROI"
    )
    return profitable


def step7_generate_listings(
    profitable: List[ProfitResult],
) -> List[ShopeeListing]:
    """Step 7 — Build AI-optimised Shopee listings."""
    from src.listing.listing_builder import ListingBuilder

    logger.info("━━━ Step 7: Listing Generation ━━━")
    builder = ListingBuilder()
    listings = builder.build_many(profitable)
    logger.success(f"  ✅ {len(listings)} listings generated")
    return listings


def step8_save_candidates(listings: List[ShopeeListing]) -> int:
    """Step 8 — Persist listing candidates to database."""
    logger.info("━━━ Step 8: Saving Candidates ━━━")
    saved = 0
    for listing in listings:
        try:
            db.save_listing(listing)
            saved += 1
        except Exception as exc:
            logger.error(f"Listing save error: {exc}")
    logger.success(f"  ✅ {saved}/{len(listings)} candidates saved")
    return saved


# ══════════════════════════════════════════════════════════════════════════════
# Full pipeline orchestrator
# ══════════════════════════════════════════════════════════════════════════════

async def run_full_pipeline() -> None:
    """Execute the complete arbitrage pipeline end-to-end."""
    logger.info("=" * 65)
    logger.info("🚀  SHOPEE ARBITRAGE SYSTEM — Pipeline Starting")
    logger.info("=" * 65)
    start = time.perf_counter()

    try:
        # ── Phase A: Market Intelligence ──────────────────────────────────────
        shopee_products = await step1_scrape_market()
        if not shopee_products:
            logger.warning("No Shopee products found — aborting.")
            return

        trends = step2_detect_trends(shopee_products)
        winners = step3_find_winners(trends)
        if not winners:
            logger.warning("No winning products found — aborting.")
            return

        # ── Phase B: Sourcing & Matching ──────────────────────────────────────
        japan_products = step4_source_japan(winners)
        if not japan_products:
            logger.warning("No Japan products found — aborting.")
            return

        matches = step5_match_products(winners, japan_products)
        if not matches:
            logger.warning("No product matches found — aborting.")
            return

        # ── Phase C: Monetisation ─────────────────────────────────────────────
        profitable = step6_calculate_profit(matches)
        if not profitable:
            logger.warning("No profitable matches found — pipeline complete.")
            return

        listings = step7_generate_listings(profitable)
        saved = step8_save_candidates(listings)

        # ── Summary ───────────────────────────────────────────────────────────
        elapsed = time.perf_counter() - start
        stats = db.get_stats()
        logger.info("=" * 65)
        logger.success(
            f"✅  Pipeline COMPLETE in {elapsed:.1f}s\n"
            f"    Products scraped :  {stats['products']}\n"
            f"    Japan sources    :  {stats['sources']}\n"
            f"    Total matches    :  {stats['matches']}\n"
            f"    Profitable       :  {stats['profitable_matches']}\n"
            f"    New candidates   :  {saved}\n"
            f"    Total listings   :  {stats['listings']}"
        )
        logger.info("=" * 65)

    except Exception as exc:
        logger.exception(f"Pipeline FAILED: {exc}")
        raise


# ══════════════════════════════════════════════════════════════════════════════
# CLI helpers
# ══════════════════════════════════════════════════════════════════════════════

def show_stats() -> None:
    db.initialize()
    stats = db.get_stats()
    print("\n📊  Database Statistics")
    print("=" * 45)
    for k, v in stats.items():
        print(f"  {k.replace('_', ' ').title():28s}: {v}")

    print()
    listings = db.get_listings(status="draft", limit=10)
    if listings:
        print("🏆  Top Draft Listings (by profit)")
        print("=" * 45)
        for i, lst in enumerate(listings, 1):
            print(
                f"  {i:2}. ¥{lst['profit_jpy']:,.0f} | "
                f"ROI {lst['roi_percent']:.0f}% | "
                f"{lst['title'][:55]}"
            )
    print()


def run_monitor() -> None:
    from src.monitoring.inventory_monitor import InventoryMonitor
    from src.monitoring.price_monitor import PriceMonitor

    logger.info("🔍  Running inventory & price monitors…")
    InventoryMonitor().check_all()
    PriceMonitor().check_all()
    logger.success("  Monitors complete")


def run_optimize(apply: bool = False) -> None:
    from src.optimizer.price_optimizer import PriceOptimizer

    logger.info(f"💹  Running price optimizer (apply={apply})…")
    optimizer = PriceOptimizer()
    results = optimizer.optimize_all_active_listings(apply=apply)
    logger.success(f"  {len(results)} optimization(s) computed")


def print_help() -> None:
    print("""
Shopee Arbitrage System
=======================

Usage:  python main.py [command]

Commands:
  run           Run the full pipeline once (default)
  schedule      Start the daily scheduler (runs indefinitely)
  monitor       Run inventory + price monitors once
  optimize      Run price optimizer (dry-run, no changes applied)
  optimize-apply Run price optimizer and apply changes
  stats         Show database statistics & top candidates
  help          Show this message

Examples:
  python main.py run
  python main.py schedule
  python main.py optimize
  python main.py stats
""")


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "run"
    db.initialize()

    if command == "run":
        asyncio.run(run_full_pipeline())

    elif command == "schedule":
        from src.scheduler.job_scheduler import scheduler

        logger.info("Starting scheduler (running pipeline now then scheduling)…")
        asyncio.run(run_full_pipeline())
        scheduler.start(run_immediately=False)

    elif command == "monitor":
        run_monitor()

    elif command == "optimize":
        run_optimize(apply=False)

    elif command == "optimize-apply":
        run_optimize(apply=True)

    elif command == "stats":
        show_stats()

    elif command in ("-h", "--help", "help"):
        print_help()

    else:
        logger.error(f"Unknown command: '{command}'")
        print_help()
        sys.exit(1)
