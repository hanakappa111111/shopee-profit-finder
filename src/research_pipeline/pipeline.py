"""On-demand Shopee Profit Research Pipeline.

Architecture
------------
This module is the **replacement entry-point** for the old always-on
automation pipeline.  Instead of continuous monitoring and scheduled jobs,
the system now executes a single research cycle per user-provided keyword.

The pipeline reuses every existing intelligence component without
modification.  It calls the same scraper, matcher, profit engine,
supplier search, and competition analyser — but wires them together
in a single synchronous call scoped to one keyword.

Data flow
---------
::

    keyword
      │
      ▼
    ┌──────────────────────┐
    │ 1. Shopee Scraper    │  scrape 20-50 products for keyword
    └──────────┬───────────┘
               ▼
    ┌──────────────────────┐
    │ 2. Product Key Gen   │  generate universal product keys
    └──────────┬───────────┘
               ▼
    ┌──────────────────────┐
    │ 3. Japan Supplier    │  search_single() per product title
    │    Search            │  across all marketplaces
    └──────────┬───────────┘
               ▼
    ┌──────────────────────┐
    │ 4. Product Matcher   │  find_matches() using title fuzzy
    │                      │  + product_key exact matching
    └──────────┬───────────┘
               ▼
    ┌──────────────────────┐
    │ 5. Profit Engine     │  calculate() + filter_profitable()
    └──────────┬───────────┘
               ▼
    ┌──────────────────────┐
    │ 6. Competition       │  analyse_product() per profitable match
    │    Analyser          │
    └──────────┬───────────┘
               ▼
    ┌──────────────────────┐
    │ 7. Compile Results   │  sort by ROI DESC, return top N
    └──────────────────────┘

No snapshots are recorded.  No monitoring jobs are triggered.
No listings are created.  Pure research output only.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from src.config.settings import settings
from src.database.database import db
from src.database.models import (
    JapanProduct,
    MatchResult,
    ProfitResult,
    ShopeeProduct,
)
from src.utils.cache import make_cache_key, scrape_cache, supplier_cache
from src.utils.logger import logger


# ── Output dataclass ──────────────────────────────────────────────────────────


@dataclass
class ResearchResult:
    """One profitable arbitrage opportunity returned by the pipeline."""

    product_name: str
    shopee_price: float
    japan_supplier_price: float
    estimated_profit_jpy: float
    roi_percent: float
    supplier_url: str
    competition_price: Optional[float] = None
    match_confidence: str = ""
    match_method: str = ""
    japan_source: str = ""
    shopee_url: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "product_name": self.product_name,
            "shopee_price": self.shopee_price,
            "japan_supplier_price": self.japan_supplier_price,
            "estimated_profit_jpy": self.estimated_profit_jpy,
            "roi_percent": self.roi_percent,
            "supplier_url": self.supplier_url,
            "competition_price": self.competition_price,
            "match_confidence": self.match_confidence,
            "match_method": self.match_method,
            "japan_source": self.japan_source,
            "shopee_url": self.shopee_url,
        }


@dataclass
class PipelineReport:
    """Complete output of one research pipeline execution."""

    keyword: str
    results: List[ResearchResult] = field(default_factory=list)
    products_scraped: int = 0
    japan_sources_found: int = 0
    matches_found: int = 0
    profitable_count: int = 0
    elapsed_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "keyword": self.keyword,
            "products_scraped": self.products_scraped,
            "japan_sources_found": self.japan_sources_found,
            "matches_found": self.matches_found,
            "profitable_count": self.profitable_count,
            "elapsed_seconds": round(self.elapsed_seconds, 2),
            "results": [r.to_dict() for r in self.results],
        }


# ── Internal async scraper helper ─────────────────────────────────────────────


async def _scrape_keyword(keyword: str, max_pages: int) -> List[ShopeeProduct]:
    """Scrape Shopee search results for a single keyword.

    Uses the existing :class:`ShopeeMarketScraper` but calls its private
    ``_scrape_keyword`` method directly to avoid triggering a full
    all-keywords sweep.
    """
    from src.market_analyzer.shopee_market_scraper import ShopeeMarketScraper

    async with ShopeeMarketScraper() as scraper:
        products = await scraper._scrape_keyword(keyword, max_pages)

    # Stamp the keyword onto each product so DB inserts record it.
    for p in products:
        p.keyword = keyword

    return products


# ── Product key generation ────────────────────────────────────────────────────


def _generate_product_keys(products: List[ShopeeProduct]) -> None:
    """Assign a universal product key to each product (in-place mutation).

    If the product_key generator module is available it is used for high-
    quality keys.  Otherwise we fall back to a no-op (the matcher still
    works via title fuzzy matching).
    """
    try:
        from src.product_key.generator import product_key_generator

        for p in products:
            if p.product_key:
                continue  # already has a key from a previous run
            result = product_key_generator.generate(p.title)
            if result.product_key:
                p.product_key = result.product_key
                p.product_key_confidence = result.confidence
        logger.info(
            f"[Pipeline] Product keys generated: "
            f"{sum(1 for p in products if p.product_key)}/{len(products)} assigned"
        )

    except ImportError:
        logger.warning(
            "[Pipeline] product_key module not available — "
            "skipping key generation (title matching will still work)"
        )


# ── Main pipeline ─────────────────────────────────────────────────────────────


def run_research_pipeline(
    keyword: str,
    *,
    max_pages: int = 2,
    max_products: int = 50,
    top_n: int = 20,
) -> PipelineReport:
    """Execute a single on-demand research cycle for *keyword*.

    Parameters
    ----------
    keyword:
        Shopee search keyword (e.g. ``"pokemon card"``).
    max_pages:
        Maximum Shopee search result pages to scrape (default 2 ≈ 40-50
        products).
    max_products:
        Hard cap on products entering the pipeline.
    top_n:
        Maximum number of results to return, sorted by ROI descending.

    Returns
    -------
    PipelineReport
        Structured report with the top profitable opportunities and
        pipeline execution statistics.
    """
    report = PipelineReport(keyword=keyword)
    t0 = time.time()

    logger.info(f"[Pipeline] ════ Starting research for keyword: {keyword!r} ════")

    # Ensure DB schema is ready
    db.initialize()

    # ── Stage 1: Scrape Shopee ────────────────────────────────────────────

    logger.info("[Pipeline] Stage 1/6 — Scraping Shopee search results …")

    # Check scrape cache first
    cache_key = make_cache_key("shopee", keyword, str(max_pages))
    cached_products = scrape_cache.get(cache_key)
    if cached_products is not None:
        shopee_products = cached_products
        logger.info(f"[Pipeline] Using cached scrape results ({len(shopee_products)} products)")
    else:
        try:
            shopee_products = asyncio.run(_scrape_keyword(keyword, max_pages))
        except RuntimeError:
            # Already inside an event loop (e.g. Jupyter)
            loop = asyncio.new_event_loop()
            shopee_products = loop.run_until_complete(
                _scrape_keyword(keyword, max_pages)
            )
            loop.close()
        # Cache the scrape results (10 min TTL)
        scrape_cache.put(cache_key, shopee_products)

    # Cap products
    shopee_products = shopee_products[:max_products]
    report.products_scraped = len(shopee_products)
    logger.info(f"[Pipeline] Scraped {len(shopee_products)} products")

    if not shopee_products:
        logger.warning("[Pipeline] No products found — aborting pipeline")
        report.elapsed_seconds = time.time() - t0
        return report

    # Persist to DB so downstream engines can reference them via FK.
    for p in shopee_products:
        db.upsert_product(p)

    # ── Stage 2: Product key generation ───────────────────────────────────

    logger.info("[Pipeline] Stage 2/6 — Generating universal product keys …")
    _generate_product_keys(shopee_products)

    # Persist updated product keys back to DB.
    for p in shopee_products:
        if p.product_key:
            db.set_product_key(p.product_url, p.product_key, p.product_key_confidence)

    # ── Stage 2b: OpportunityDiscoveryAI pre-filter ────────────────────────

    logger.info(
        "[Pipeline] Stage 2b — OpportunityDiscoveryAI: scoring & filtering products …"
    )
    try:
        from src.opportunity_discovery.scorer import OpportunityDiscoveryAI

        opp_ai = OpportunityDiscoveryAI(threshold=60.0)
        shopee_products, all_scores = opp_ai.score_products(shopee_products, keyword)

        # Persist all scores to DB (background, errors are non-fatal)
        try:
            db.upsert_opportunity_scores(all_scores, keyword)
        except Exception as exc:
            logger.debug(f"[Pipeline] OpportunityDiscoveryAI score persist failed: {exc}")

        logger.info(
            f"[Pipeline] OpportunityDiscoveryAI: "
            f"{len(shopee_products)}/{report.products_scraped} products passed filter"
        )

        if not shopee_products:
            logger.warning(
                "[Pipeline] OpportunityDiscoveryAI filtered out all products "
                "— no products above threshold 60. Aborting pipeline."
            )
            report.elapsed_seconds = time.time() - t0
            return report

    except ImportError:
        logger.warning(
            "[Pipeline] opportunity_discovery module not available — "
            "skipping pre-filter (all products will be passed to supplier search)"
        )

    # ── Stage 3: Japan supplier search ────────────────────────────────────

    logger.info("[Pipeline] Stage 3/6 — Searching Japan supplier sources …")
    all_japan_products: List[JapanProduct] = []

    try:
        from src.supplier_search.search_engine import SupplierSearchEngine

        search_engine = SupplierSearchEngine(db=db)

        # Build deduplicated search queries from the Shopee product titles.
        # Use the keyword itself PLUS the top product titles (truncated to
        # avoid excessively long queries).
        search_queries: List[str] = [keyword]
        seen_queries = {keyword.lower()}

        for p in shopee_products[:15]:  # limit to avoid over-scraping
            short_title = " ".join(p.title.split()[:8])
            if short_title.lower() not in seen_queries:
                search_queries.append(short_title)
                seen_queries.add(short_title.lower())

        for query in search_queries:
            try:
                # Check supplier cache
                sq_key = make_cache_key("supplier", query)
                cached_jp = supplier_cache.get(sq_key)
                if cached_jp is not None:
                    results = cached_jp
                else:
                    results = search_engine.search_single(query)
                    supplier_cache.put(sq_key, results)
                for jp in results:
                    all_japan_products.append(jp)
            except Exception as exc:
                logger.warning(f"[Pipeline] Supplier search failed for {query!r}: {exc}")

        # Deduplicate by URL
        seen_urls: set = set()
        unique_japan: List[JapanProduct] = []
        for jp in all_japan_products:
            if jp.product_url not in seen_urls:
                unique_japan.append(jp)
                seen_urls.add(jp.product_url)
        all_japan_products = unique_japan

        # Persist Japan sources to DB for profit analysis FK references.
        for jp in all_japan_products:
            db.upsert_source(jp)

        report.japan_sources_found = len(all_japan_products)
        logger.info(f"[Pipeline] Found {len(all_japan_products)} Japan sources")

    except ImportError:
        logger.error("[Pipeline] supplier_search module not available — cannot proceed")
        report.elapsed_seconds = time.time() - t0
        return report

    if not all_japan_products:
        logger.warning("[Pipeline] No Japan sources found — aborting pipeline")
        report.elapsed_seconds = time.time() - t0
        return report

    # ── Stage 4: Product matching ─────────────────────────────────────────

    logger.info("[Pipeline] Stage 4/6 — Matching Shopee ↔ Japan products …")
    raw_matches: List[MatchResult] = []
    try:
        from src.matching.product_matcher import ProductMatcher

        matcher = ProductMatcher()
        raw_matches = matcher.find_matches(shopee_products, all_japan_products)
    except ImportError:
        logger.error("[Pipeline] matching module not available — cannot proceed")
        report.elapsed_seconds = time.time() - t0
        return report
    except Exception as exc:
        logger.error(f"[Pipeline] Product matching failed: {exc}")
        report.elapsed_seconds = time.time() - t0
        return report
    logger.info(f"[Pipeline] Stage 4a: structural matcher found {len(raw_matches)} candidates")

    # ── Stage 4b: ProductMatchingAI second-pass filter ────────────────────

    logger.info("[Pipeline] Stage 4b — ProductMatchingAI: scoring & filtering matches …")
    ai_score_map: dict = {}   # (shopee_url, japan_url) → MatchAIScore
    try:
        from src.product_matching.matcher import ProductMatchingAI

        match_ai = ProductMatchingAI(threshold=0.8)
        matches, ai_scores = match_ai.filter_matches(raw_matches)

        # Build lookup for DB persistence
        ai_score_map = {
            (s.shopee_url, s.japan_url): s for s in ai_scores
        }

        logger.info(
            f"[Pipeline] Stage 4b: {len(matches)}/{len(raw_matches)} "
            f"matches accepted by ProductMatchingAI"
        )
    except ImportError:
        logger.warning(
            "[Pipeline] product_matching module not available — "
            "using all structural matches without AI validation"
        )
        matches = raw_matches

    report.matches_found = len(matches)

    if not matches:
        logger.warning("[Pipeline] No matches after AI filter — aborting pipeline")
        report.elapsed_seconds = time.time() - t0
        return report

    # Persist matches to DB so profit analysis / competition analyser
    # can reference them.
    from src.profit.profit_engine import ProfitEngine

    profit_engine = ProfitEngine()

    for match in matches:
        try:
            ai = ai_score_map.get(
                (match.shopee_product.product_url, match.japan_product.product_url)
            )
            db.upsert_match(
                profit=ProfitResult(
                    shopee_product=match.shopee_product,
                    japan_product=match.japan_product,
                    similarity_score=match.similarity_score,
                    match_method=match.match_method,
                    confidence_level=match.confidence_level,
                    profit_jpy=0,
                    roi_percent=0,
                    is_profitable=False,
                    breakdown={},
                ),
                match_method=match.match_method,
                confidence_level=match.confidence_level,
                match_score=ai.match_score if ai else 0.0,
                matching_method=ai.matching_method if ai else "keyword",
            )
        except Exception as exc:
            logger.debug(f"[Pipeline] Match upsert failed: {exc}")

    # ── Stage 5: Profit calculation ───────────────────────────────────────

    logger.info("[Pipeline] Stage 5/6 — Calculating profit margins …")
    try:
        all_profit_results: List[ProfitResult] = profit_engine.calculate_many(matches)
        profitable: List[ProfitResult] = profit_engine.filter_profitable(all_profit_results)
    except Exception as exc:
        logger.error(f"[Pipeline] Profit calculation failed: {exc}")
        report.elapsed_seconds = time.time() - t0
        return report
    report.profitable_count = len(profitable)
    logger.info(
        f"[Pipeline] {len(profitable)}/{len(all_profit_results)} "
        f"matches are profitable"
    )

    if not profitable:
        logger.info("[Pipeline] No profitable opportunities found for this keyword")
        report.elapsed_seconds = time.time() - t0
        return report

    # Sort by ROI descending, cap at top_n.
    profitable.sort(key=lambda r: r.roi_percent, reverse=True)
    profitable = profitable[:top_n]

    # ── Stage 6: Competition analysis ─────────────────────────────────────

    logger.info("[Pipeline] Stage 6/6 — Analysing competition …")
    competition_data: Dict[str, Optional[float]] = {}  # shopee_url → lowest competitor price

    try:
        from src.competition_analyzer.analyzer_engine import AnalyzerEngine

        analyzer = AnalyzerEngine(db=db)

        for result in profitable:
            shopee_url = result.shopee_product.product_url
            shopee_id = db.get_product_id(shopee_url)
            if shopee_id:
                try:
                    analysis = analyzer.analyse_product(shopee_id, keyword=keyword)
                    rec = analysis.get("recommendation")
                    if rec and rec.get("min_market_price"):
                        competition_data[shopee_url] = rec["min_market_price"]
                except Exception as exc:
                    logger.debug(f"[Pipeline] Competition analysis failed for {shopee_url}: {exc}")

    except ImportError:
        logger.warning("[Pipeline] competition_analyzer not available — skipping")

    # ── Compile results ───────────────────────────────────────────────────

    for result in profitable:
        shopee_url = result.shopee_product.product_url
        comp_price = competition_data.get(shopee_url)

        report.results.append(
            ResearchResult(
                product_name=result.shopee_product.title,
                shopee_price=result.shopee_product.price,
                japan_supplier_price=result.japan_product.price_jpy,
                estimated_profit_jpy=result.profit_jpy,
                roi_percent=result.roi_percent,
                supplier_url=result.japan_product.product_url,
                competition_price=comp_price,
                match_confidence=str(result.confidence_level),
                match_method=result.match_method,
                japan_source=str(result.japan_product.source),
                shopee_url=shopee_url,
            )
        )

    report.elapsed_seconds = time.time() - t0

    logger.info(
        f"[Pipeline] ════ Research complete: {len(report.results)} profitable "
        f"opportunities in {report.elapsed_seconds:.1f}s ════"
    )

    # ── Send notifications (if configured) ───────────────────────────────
    try:
        from src.utils.notifications import notify_profitable_results
        notify_profitable_results(report)
    except Exception as exc:
        logger.debug(f"[Pipeline] Notification skipped: {exc}")

    return report
