"""
SQLite database layer — thread-safe connection pool and CRUD operations
for all application entities.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Generator, List, Optional

from src.config.settings import settings
from src.database.models import (
    CompetitorListing,
    DiscoveryMethod,
    JapanProduct,
    ListingStatus,
    MatchConfidence,
    MatchResult,
    PriceDelta,
    PriceOptimizationResult,
    PriceRecommendation,
    ProfitAnalysis,
    ProfitResult,
    ProductSnapshot,
    RelatedProductCandidate,
    ResearchCandidate,
    ResearchCandidateStatus,
    ShopeeListing,
    ShopeeProduct,
    TrendData,
)
from src.utils.logger import logger


# ── DDL ───────────────────────────────────────────────────────────────────────

_DDL = """
-- Shopee scraped products
CREATE TABLE IF NOT EXISTS products (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    title                   TEXT    NOT NULL,
    price                   REAL    NOT NULL DEFAULT 0,
    sales                   INTEGER NOT NULL DEFAULT 0,
    rating                  REAL    NOT NULL DEFAULT 0,
    review_count            INTEGER NOT NULL DEFAULT 0,
    seller                  TEXT    NOT NULL DEFAULT '',
    image                   TEXT    NOT NULL DEFAULT '',
    url                     TEXT    NOT NULL UNIQUE,
    keyword                 TEXT    NOT NULL DEFAULT '',
    market                  TEXT    NOT NULL DEFAULT 'PH',
    product_key             TEXT,
    product_key_confidence  TEXT    NOT NULL DEFAULT 'none',
    created_at              TEXT    NOT NULL,
    updated_at              TEXT    NOT NULL
);

-- Trend snapshots per product
CREATE TABLE IF NOT EXISTS trends (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    product_url     TEXT    NOT NULL,
    sales_velocity  REAL    NOT NULL DEFAULT 0,
    review_growth   REAL    NOT NULL DEFAULT 0,
    price_stability REAL    NOT NULL DEFAULT 1,
    direction       TEXT    NOT NULL DEFAULT 'stable',
    trend_score     REAL    NOT NULL DEFAULT 0,
    computed_at     TEXT    NOT NULL
);

-- Japanese sourcing products
CREATE TABLE IF NOT EXISTS sources (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    title                   TEXT    NOT NULL,
    price_jpy               REAL    NOT NULL,
    stock                   TEXT    NOT NULL DEFAULT 'unknown',
    image                   TEXT    NOT NULL DEFAULT '',
    url                     TEXT    NOT NULL UNIQUE,
    source                  TEXT    NOT NULL,
    seller                  TEXT    NOT NULL DEFAULT '',
    condition               TEXT    NOT NULL DEFAULT 'new',
    product_key             TEXT,
    product_key_confidence  TEXT    NOT NULL DEFAULT 'none',
    created_at              TEXT    NOT NULL,
    updated_at              TEXT    NOT NULL
);

-- Product matches
CREATE TABLE IF NOT EXISTS matches (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    shopee_product_id   INTEGER NOT NULL REFERENCES products(id),
    japan_product_id    INTEGER NOT NULL REFERENCES sources(id),
    similarity          REAL    NOT NULL,
    profit_jpy          REAL    NOT NULL DEFAULT 0,
    roi_percent         REAL    NOT NULL DEFAULT 0,
    match_method        TEXT    NOT NULL DEFAULT 'title_fuzzy',
    confidence_level    TEXT    NOT NULL DEFAULT 'medium_fuzzy',
    created_at          TEXT    NOT NULL,
    UNIQUE(shopee_product_id, japan_product_id)
);

-- Shopee listings (managed by this system)
CREATE TABLE IF NOT EXISTS listings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    title           TEXT    NOT NULL,
    description     TEXT    NOT NULL DEFAULT '',
    price           REAL    NOT NULL,
    stock           INTEGER NOT NULL DEFAULT 10,
    images          TEXT    NOT NULL DEFAULT '[]',
    category_id     INTEGER NOT NULL DEFAULT 0,
    brand           TEXT    NOT NULL DEFAULT '',
    status          TEXT    NOT NULL DEFAULT 'draft',
    keywords        TEXT    NOT NULL DEFAULT '[]',
    source_url      TEXT    NOT NULL DEFAULT '',
    profit_jpy      REAL    NOT NULL DEFAULT 0,
    roi_percent     REAL    NOT NULL DEFAULT 0,
    shopee_item_id  INTEGER,
    payload_json    TEXT    NOT NULL DEFAULT '{}',
    created_at      TEXT    NOT NULL,
    updated_at      TEXT    NOT NULL
);

-- Order tracking
CREATE TABLE IF NOT EXISTS orders (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id      INTEGER NOT NULL REFERENCES listings(id),
    order_status    TEXT    NOT NULL DEFAULT 'pending',
    order_amount    REAL    NOT NULL DEFAULT 0,
    created_at      TEXT    NOT NULL
);

-- Price history for Japan products
CREATE TABLE IF NOT EXISTS price_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    japan_url   TEXT    NOT NULL,
    price_jpy   REAL    NOT NULL,
    recorded_at TEXT    NOT NULL
);

-- Competitor price snapshots (for price optimisation)
CREATE TABLE IF NOT EXISTS competitor_prices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword         TEXT    NOT NULL,
    comp_title      TEXT    NOT NULL,
    comp_price      REAL    NOT NULL,
    comp_url        TEXT    NOT NULL,
    scraped_at      TEXT    NOT NULL
);

-- ─────────────────────────────────────────────────────────────────────────────
-- product_snapshots
-- ─────────────────────────────────────────────────────────────────────────────
-- One row per monitoring run per Japan source product.  Every time the price
-- monitor or inventory monitor visits a Japan product URL it inserts a row
-- here so that callers can query the full time-series without touching the
-- live scrapers.
--
-- Integration points
-- ──────────────────
-- 1. Price monitoring (src/monitoring/price_monitor.py)
--    Calls record_snapshot() after each platform fetch.
--    Calls get_price_delta() to decide whether to raise a PriceAlert.
--    Threshold: settings.PRICE_CHANGE_ALERT_PCT (default 5 %).
--
-- 2. Inventory monitoring (src/monitoring/inventory_monitor.py)
--    Calls record_snapshot() after each platform fetch.
--    Calls get_stock_changes() to detect in_stock → out_of_stock transitions.
--    On transition: calls update_source_stock() + raises a StockAlert.
--
-- 3. Profit recalculation (src/profit/profit_engine.py)
--    Calls get_products_needing_profit_recalc() to find Japan products whose
--    price_jpy changed by > settings.PROFIT_RECALC_THRESHOLD_PCT in the last
--    24 h.  For each match linked to such a product it recomputes
--    profit_jpy / roi_percent and calls upsert_match() to persist the new
--    values, then calls update_listing() if the listing is active.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS product_snapshots (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    -- FK to the Japan source product being monitored
    product_id          INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    -- Japan source price at capture time (JPY)
    price_jpy           REAL    NOT NULL DEFAULT 0,
    -- Lowest Shopee competitor price in local currency at the same moment.
    -- NULL when no competitor scrape was performed during this run.
    competitor_price    REAL,
    -- Stock status reported by the Japan platform
    stock_status        TEXT    NOT NULL DEFAULT 'unknown',
    -- Cumulative Shopee sales count for the matched product (for velocity calc)
    sales_count         INTEGER NOT NULL DEFAULT 0,
    -- Review count at capture time (for review_growth_rate)
    review_count        INTEGER NOT NULL DEFAULT 0,
    -- Exchange rate (local_currency / JPY) stored at capture time so historical
    -- profit recalculation can replay ROI without hitting the rates API.
    exchange_rate       REAL,
    captured_at         TEXT    NOT NULL
);

-- Composite index: product_id + time — the primary access pattern for all
-- time-series queries (latest snapshot, window delta, stock change detection).
CREATE INDEX IF NOT EXISTS idx_snapshots_product_time
    ON product_snapshots(product_id, captured_at DESC);

-- Standalone time index — used by the scheduler to purge rows older than
-- settings.SNAPSHOT_RETENTION_DAYS.
CREATE INDEX IF NOT EXISTS idx_snapshots_captured
    ON product_snapshots(captured_at DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- profit_analysis
-- ─────────────────────────────────────────────────────────────────────────────
-- One auditable profit-calculation record per (shopee_product_id,
-- japan_product_id) pair.  The UNIQUE constraint means the row is
-- overwritten on re-calculation so the table always holds the latest result.
--
-- ROI is stored as a DECIMAL (0–1) — 0.30 = 30 % — matching the formula
--   roi = profit / cost_jpy
-- All cost and revenue fields are in JPY except shopee_price and shopee_fee
-- which are in the market's local currency (PHP / SGD / MYR).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS profit_analysis (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    shopee_product_id       INTEGER NOT NULL REFERENCES products(id),
    japan_product_id        INTEGER NOT NULL REFERENCES sources(id),
    -- cost components (JPY)
    supplier_price          REAL    NOT NULL DEFAULT 0,
    domestic_shipping_cost  REAL    NOT NULL DEFAULT 0,
    safety_margin           REAL    NOT NULL DEFAULT 0,
    -- revenue components (local currency for shopee_price/fee; JPY otherwise)
    shopee_price            REAL    NOT NULL DEFAULT 0,
    shopee_fee              REAL    NOT NULL DEFAULT 0,
    fee_rate                REAL    NOT NULL DEFAULT 0,
    exchange_rate           REAL    NOT NULL DEFAULT 0,
    net_revenue_jpy         REAL    NOT NULL DEFAULT 0,
    cost_jpy                REAL    NOT NULL DEFAULT 0,
    -- derived results
    profit                  REAL    NOT NULL DEFAULT 0,
    roi                     REAL    NOT NULL DEFAULT 0,    -- decimal 0–1
    is_profitable           INTEGER NOT NULL DEFAULT 0,
    -- match provenance
    match_method            TEXT    NOT NULL DEFAULT 'title_fuzzy',
    confidence_level        TEXT    NOT NULL DEFAULT 'medium_fuzzy',
    similarity_score        REAL    NOT NULL DEFAULT 0,
    analyzed_at             TEXT    NOT NULL,
    UNIQUE(shopee_product_id, japan_product_id)
);

CREATE INDEX IF NOT EXISTS idx_profit_analysis_profitable
    ON profit_analysis(is_profitable, profit DESC);
CREATE INDEX IF NOT EXISTS idx_profit_analysis_roi
    ON profit_analysis(roi DESC);
CREATE INDEX IF NOT EXISTS idx_profit_analysis_analyzed
    ON profit_analysis(analyzed_at DESC);

-- Price optimisation log
CREATE TABLE IF NOT EXISTS price_optimizations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id          INTEGER NOT NULL REFERENCES listings(id),
    old_price           REAL    NOT NULL,
    suggested_price     REAL    NOT NULL,
    competitor_price    REAL    NOT NULL,
    reason              TEXT    NOT NULL DEFAULT '',
    applied             INTEGER NOT NULL DEFAULT 0,
    optimized_at        TEXT    NOT NULL
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_products_keyword      ON products(keyword);
CREATE INDEX IF NOT EXISTS idx_products_sales        ON products(sales DESC);
CREATE INDEX IF NOT EXISTS idx_products_product_key  ON products(product_key);
CREATE INDEX IF NOT EXISTS idx_products_market       ON products(market);
CREATE INDEX IF NOT EXISTS idx_sources_product_key   ON sources(product_key);
CREATE INDEX IF NOT EXISTS idx_matches_profit        ON matches(profit_jpy DESC);
CREATE INDEX IF NOT EXISTS idx_matches_confidence    ON matches(confidence_level);
CREATE INDEX IF NOT EXISTS idx_listings_status       ON listings(status);
CREATE INDEX IF NOT EXISTS idx_trends_url            ON trends(product_url);
CREATE INDEX IF NOT EXISTS idx_price_history_url     ON price_history(japan_url);
CREATE INDEX IF NOT EXISTS idx_comp_prices_keyword   ON competitor_prices(keyword);

-- ─────────────────────────────────────────────────────────────────────────────
-- research_candidates
-- ─────────────────────────────────────────────────────────────────────────────
-- One row per Shopee product (UNIQUE on shopee_product_id).  The Research AI
-- engine upserts this table on every scan so scores are always current.
--
-- Integration point with the pipeline
-- ─────────────────────────────────────
-- 1. Research AI (src/research_ai/research_engine.py)
--    Calls upsert_research_candidate() for products scoring above
--    settings.RESEARCH_MIN_SCORE.
--
-- 2. Japan marketplace search (future: src/japan_source/)
--    Reads get_research_candidates(status='pending') to determine which
--    keywords to forward to each Japan scraper.
--
-- 3. Matching engine (src/matching/product_matcher.py)
--    After a Japan product is found and matched, calls
--    update_candidate_status(id, 'matched') to track pipeline progress.
--
-- 4. Profit engine (src/profit/profit_engine.py)
--    If all analyses for a matched candidate fall below thresholds, calls
--    update_candidate_status(id, 'rejected') so it is excluded from future runs.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS research_candidates (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    -- FK to the Shopee product being evaluated
    shopee_product_id   INTEGER NOT NULL REFERENCES products(id),
    -- Composite score (0-100); weighted sum of the five sub-scores below
    research_score      REAL    NOT NULL DEFAULT 0,
    -- Sub-scores (each 0-100; weights defined in ResearchScorer.WEIGHTS)
    score_demand        REAL    NOT NULL DEFAULT 0,   -- sales/rating/review signal
    score_velocity      REAL    NOT NULL DEFAULT 0,   -- rate-of-growth signal
    score_stability     REAL    NOT NULL DEFAULT 0,   -- price-stability signal
    score_price_gap     REAL    NOT NULL DEFAULT 0,   -- estimated arbitrage headroom
    score_brand         REAL    NOT NULL DEFAULT 0,   -- brand recognition via product_key
    -- Human-readable explanation of the top scoring factors
    reason              TEXT    NOT NULL DEFAULT '',
    -- Lifecycle: pending → matched | rejected
    status              TEXT    NOT NULL DEFAULT 'pending',
    created_at          TEXT    NOT NULL,
    UNIQUE(shopee_product_id)
);

-- Primary query: pending candidates sorted by score
CREATE INDEX IF NOT EXISTS idx_research_score
    ON research_candidates(research_score DESC);
-- Status + score composite for filtered reads
CREATE INDEX IF NOT EXISTS idx_research_status_score
    ON research_candidates(status, research_score DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- related_product_candidates
-- ─────────────────────────────────────────────────────────────────────────────
-- Each row is one search keyword produced by the Related Product Discovery AI
-- for a specific seed Shopee product.  The Japan marketplace scraper reads
-- this table to decide which keyword searches to run next.
--
-- Discovery methods
-- ─────────────────
-- brand    — sibling products sharing the same brand (e.g. all Bandai sets)
-- series   — sequential set codes (OP01 → OP02 → OP03)
-- keyword  — cross-product-type keywords from the seed's title tokens
-- category — accessory / complementary category (e.g. Card → Sleeve)
--
-- Integration points
-- ──────────────────
-- 1. Related Product Discovery AI (src/related_discovery/discovery_engine.py)
--    Calls upsert_related_candidate() for each generated keyword.
--
-- 2. Japan marketplace search (future: src/japan_source/)
--    Reads get_related_candidates() to obtain its keyword work queue.
--    For each keyword, searches Amazon JP / Rakuten / Yahoo Shopping.
--
-- 3. ProductMatcher / ProfitEngine
--    Any found Japan products feed into the standard matching pipeline
--    without modification.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS related_product_candidates (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    -- FK to the Shopee product that triggered this discovery
    seed_product_id     INTEGER NOT NULL REFERENCES products(id),
    -- The generated search keyword to send to the Japan scraper
    related_keyword     TEXT    NOT NULL,
    -- Which expansion strategy produced this keyword
    discovery_method    TEXT    NOT NULL DEFAULT 'keyword',
    -- Estimated quality / usefulness of this keyword (0-100)
    confidence_score    REAL    NOT NULL DEFAULT 0,
    created_at          TEXT    NOT NULL,
    -- Prevent duplicate (seed, keyword, method) triples across re-runs
    UNIQUE(seed_product_id, related_keyword, discovery_method)
);

-- Primary access pattern: highest-confidence keywords first
CREATE INDEX IF NOT EXISTS idx_related_confidence
    ON related_product_candidates(confidence_score DESC);
-- Per-seed lookup (used by the job to skip seeds already processed)
CREATE INDEX IF NOT EXISTS idx_related_seed
    ON related_product_candidates(seed_product_id, discovery_method);
-- Method filter (used by the Japan scraper to batch by strategy type)
CREATE INDEX IF NOT EXISTS idx_related_method
    ON related_product_candidates(discovery_method, confidence_score DESC);
"""

# ── Migration: add new columns to existing databases ─────────────────────────

_MIGRATIONS: list[str] = [
    # products table additions
    "ALTER TABLE products ADD COLUMN review_count           INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE products ADD COLUMN market                 TEXT    NOT NULL DEFAULT 'PH'",
    "ALTER TABLE products ADD COLUMN product_key            TEXT",
    "ALTER TABLE products ADD COLUMN product_key_confidence TEXT    NOT NULL DEFAULT 'none'",
    # sources table additions
    "ALTER TABLE sources  ADD COLUMN product_key            TEXT",
    "ALTER TABLE sources  ADD COLUMN product_key_confidence TEXT    NOT NULL DEFAULT 'none'",
    "ALTER TABLE sources  ADD COLUMN updated_at             TEXT    NOT NULL DEFAULT ''",
    # matches table additions
    "ALTER TABLE matches  ADD COLUMN confidence_level       TEXT    NOT NULL DEFAULT 'medium_fuzzy'",
    # product_snapshots — CREATE TABLE handles new databases; this is a no-op
    # guard for databases that existed before this migration was added.  We
    # cannot ALTER TABLE a table that doesn't exist yet, so we use CREATE TABLE
    # IF NOT EXISTS inside the migration list instead of ALTER TABLE.
    """
    CREATE TABLE IF NOT EXISTS product_snapshots (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id       INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
        price_jpy        REAL    NOT NULL DEFAULT 0,
        competitor_price REAL,
        stock_status     TEXT    NOT NULL DEFAULT 'unknown',
        sales_count      INTEGER NOT NULL DEFAULT 0,
        review_count     INTEGER NOT NULL DEFAULT 0,
        exchange_rate    REAL,
        captured_at      TEXT    NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_snapshots_product_time "
    "ON product_snapshots(product_id, captured_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_captured "
    "ON product_snapshots(captured_at DESC)",
    # profit_analysis — same CREATE TABLE IF NOT EXISTS guard pattern
    """
    CREATE TABLE IF NOT EXISTS profit_analysis (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        shopee_product_id       INTEGER NOT NULL REFERENCES products(id),
        japan_product_id        INTEGER NOT NULL REFERENCES sources(id),
        supplier_price          REAL    NOT NULL DEFAULT 0,
        domestic_shipping_cost  REAL    NOT NULL DEFAULT 0,
        safety_margin           REAL    NOT NULL DEFAULT 0,
        shopee_price            REAL    NOT NULL DEFAULT 0,
        shopee_fee              REAL    NOT NULL DEFAULT 0,
        fee_rate                REAL    NOT NULL DEFAULT 0,
        exchange_rate           REAL    NOT NULL DEFAULT 0,
        net_revenue_jpy         REAL    NOT NULL DEFAULT 0,
        cost_jpy                REAL    NOT NULL DEFAULT 0,
        profit                  REAL    NOT NULL DEFAULT 0,
        roi                     REAL    NOT NULL DEFAULT 0,
        is_profitable           INTEGER NOT NULL DEFAULT 0,
        match_method            TEXT    NOT NULL DEFAULT 'title_fuzzy',
        confidence_level        TEXT    NOT NULL DEFAULT 'medium_fuzzy',
        similarity_score        REAL    NOT NULL DEFAULT 0,
        analyzed_at             TEXT    NOT NULL,
        UNIQUE(shopee_product_id, japan_product_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_profit_analysis_profitable "
    "ON profit_analysis(is_profitable, profit DESC)",
    "CREATE INDEX IF NOT EXISTS idx_profit_analysis_roi "
    "ON profit_analysis(roi DESC)",
    "CREATE INDEX IF NOT EXISTS idx_profit_analysis_analyzed "
    "ON profit_analysis(analyzed_at DESC)",
    # research_candidates — same CREATE TABLE IF NOT EXISTS guard pattern
    """
    CREATE TABLE IF NOT EXISTS research_candidates (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        shopee_product_id   INTEGER NOT NULL REFERENCES products(id),
        research_score      REAL    NOT NULL DEFAULT 0,
        score_demand        REAL    NOT NULL DEFAULT 0,
        score_velocity      REAL    NOT NULL DEFAULT 0,
        score_stability     REAL    NOT NULL DEFAULT 0,
        score_price_gap     REAL    NOT NULL DEFAULT 0,
        score_brand         REAL    NOT NULL DEFAULT 0,
        reason              TEXT    NOT NULL DEFAULT '',
        status              TEXT    NOT NULL DEFAULT 'pending',
        created_at          TEXT    NOT NULL,
        UNIQUE(shopee_product_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_research_score "
    "ON research_candidates(research_score DESC)",
    "CREATE INDEX IF NOT EXISTS idx_research_status_score "
    "ON research_candidates(status, research_score DESC)",
    # related_product_candidates — CREATE TABLE IF NOT EXISTS guard for existing DBs
    """
    CREATE TABLE IF NOT EXISTS related_product_candidates (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        seed_product_id     INTEGER NOT NULL REFERENCES products(id),
        related_keyword     TEXT    NOT NULL,
        discovery_method    TEXT    NOT NULL DEFAULT 'keyword',
        confidence_score    REAL    NOT NULL DEFAULT 0,
        created_at          TEXT    NOT NULL,
        UNIQUE(seed_product_id, related_keyword, discovery_method)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_related_confidence "
    "ON related_product_candidates(confidence_score DESC)",
    "CREATE INDEX IF NOT EXISTS idx_related_seed "
    "ON related_product_candidates(seed_product_id, discovery_method)",
    "CREATE INDEX IF NOT EXISTS idx_related_method "
    "ON related_product_candidates(discovery_method, confidence_score DESC)",
    # ── Competition Analyzer AI ───────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS competitor_listings (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        shopee_product_id   INTEGER NOT NULL REFERENCES products(id),
        product_key         TEXT,
        competitor_title    TEXT    NOT NULL DEFAULT '',
        competitor_price    REAL    NOT NULL DEFAULT 0,
        competitor_stock    INTEGER,
        seller_rating       REAL,
        competitor_url      TEXT    NOT NULL DEFAULT '',
        scraped_at          TEXT    NOT NULL,
        UNIQUE(shopee_product_id, competitor_url)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_comp_listings_product "
    "ON competitor_listings(shopee_product_id, competitor_price ASC)",
    "CREATE INDEX IF NOT EXISTS idx_comp_listings_key "
    "ON competitor_listings(product_key, competitor_price ASC)",
    "CREATE INDEX IF NOT EXISTS idx_comp_listings_scraped "
    "ON competitor_listings(scraped_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS price_recommendations (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        shopee_product_id   INTEGER NOT NULL REFERENCES products(id),
        product_key         TEXT,
        competitor_count    INTEGER NOT NULL DEFAULT 0,
        min_market_price    REAL    NOT NULL DEFAULT 0,
        median_market_price REAL    NOT NULL DEFAULT 0,
        max_market_price    REAL    NOT NULL DEFAULT 0,
        recommended_price   REAL    NOT NULL DEFAULT 0,
        min_viable_price    REAL    NOT NULL DEFAULT 0,
        strategy_used       TEXT    NOT NULL DEFAULT 'median_minus_discount',
        strategy_note       TEXT    NOT NULL DEFAULT '',
        calculated_at       TEXT    NOT NULL,
        UNIQUE(shopee_product_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_price_rec_product "
    "ON price_recommendations(shopee_product_id)",
    "CREATE INDEX IF NOT EXISTS idx_price_rec_key "
    "ON price_recommendations(product_key)",
    "CREATE INDEX IF NOT EXISTS idx_price_rec_calculated "
    "ON price_recommendations(calculated_at DESC)",
    # ── Performance optimization indexes (P0-P3) ─────────────────────────
    # P0: Blocking bottlenecks — missing FK indexes
    "CREATE INDEX IF NOT EXISTS idx_matches_shopee_id "
    "ON matches(shopee_product_id)",
    "CREATE INDEX IF NOT EXISTS idx_matches_japan_id "
    "ON matches(japan_product_id)",
    "CREATE INDEX IF NOT EXISTS idx_listings_source_url "
    "ON listings(source_url)",
    "CREATE INDEX IF NOT EXISTS idx_profit_analysis_japan_id "
    "ON profit_analysis(japan_product_id)",
    # P1: High-impact composite indexes
    "CREATE INDEX IF NOT EXISTS idx_matches_profit_roi "
    "ON matches(profit_jpy DESC, roi_percent DESC)",
    "CREATE INDEX IF NOT EXISTS idx_listings_status_profit "
    "ON listings(status, profit_jpy DESC)",
    "CREATE INDEX IF NOT EXISTS idx_trends_url_time "
    "ON trends(product_url, computed_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_profit_analysis_shopee_profitable "
    "ON profit_analysis(shopee_product_id, is_profitable, profit DESC)",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_product_price_time "
    "ON product_snapshots(product_id, captured_at DESC, price_jpy, exchange_rate, stock_status)",
    "CREATE INDEX IF NOT EXISTS idx_snapshots_stock_time "
    "ON product_snapshots(stock_status, captured_at DESC)",
    # P2: Supplementary composite indexes
    "CREATE INDEX IF NOT EXISTS idx_sources_source_price "
    "ON sources(source, price_jpy ASC)",
    "CREATE INDEX IF NOT EXISTS idx_sources_key_price "
    "ON sources(product_key, price_jpy ASC)",
    "CREATE INDEX IF NOT EXISTS idx_products_market_sales "
    "ON products(market, sales DESC)",
    "CREATE INDEX IF NOT EXISTS idx_products_sales_rating_price "
    "ON products(sales DESC, rating DESC, price DESC)",
    # P3: Archive tables
    """
    CREATE TABLE IF NOT EXISTS supplier_snapshots_archive (
        id           INTEGER PRIMARY KEY,
        product_key  TEXT,
        supplier_url TEXT    NOT NULL,
        price_jpy    REAL    NOT NULL DEFAULT 0,
        stock_status TEXT    NOT NULL DEFAULT 'unknown',
        captured_at  TEXT    NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_sup_snap_archive_url "
    "ON supplier_snapshots_archive(supplier_url, captured_at DESC)",
    """
    CREATE TABLE IF NOT EXISTS product_snapshots_archive (
        id               INTEGER PRIMARY KEY,
        product_id       INTEGER NOT NULL,
        price_jpy        REAL    NOT NULL DEFAULT 0,
        competitor_price REAL,
        stock_status     TEXT    NOT NULL DEFAULT 'unknown',
        sales_count      INTEGER NOT NULL DEFAULT 0,
        review_count     INTEGER NOT NULL DEFAULT 0,
        exchange_rate    REAL,
        captured_at      TEXT    NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_snap_archive_product "
    "ON product_snapshots_archive(product_id, captured_at DESC)",
    # ── OpportunityDiscoveryAI scores ─────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS product_opportunity_scores (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        product_key         TEXT,
        product_url         TEXT    NOT NULL DEFAULT '',
        keyword             TEXT    NOT NULL DEFAULT '',
        shopee_price        REAL    NOT NULL DEFAULT 0,
        opportunity_score   REAL    NOT NULL DEFAULT 0,
        demand_score        REAL    NOT NULL DEFAULT 0,
        competition_score   REAL    NOT NULL DEFAULT 0,
        price_spread_score  REAL    NOT NULL DEFAULT 0,
        trust_score         REAL    NOT NULL DEFAULT 0,
        created_at          TEXT    NOT NULL,
        UNIQUE(product_url, keyword)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_opp_scores_keyword_score "
    "ON product_opportunity_scores(keyword, opportunity_score DESC)",
    "CREATE INDEX IF NOT EXISTS idx_opp_scores_key "
    "ON product_opportunity_scores(product_key, opportunity_score DESC)",
    "CREATE INDEX IF NOT EXISTS idx_opp_scores_created "
    "ON product_opportunity_scores(created_at DESC)",
    # ── ProductMatchingAI columns (matches table) ─────────────────────────────
    # match_score: AI similarity 0.0–1.0 (0 = pre-AI legacy row)
    "ALTER TABLE matches ADD COLUMN match_score       REAL NOT NULL DEFAULT 0",
    # matching_method: 'keyword' (exact/structural) or 'ai_match' (AI scored)
    "ALTER TABLE matches ADD COLUMN matching_method   TEXT NOT NULL DEFAULT 'keyword'",
    "CREATE INDEX IF NOT EXISTS idx_matches_ai_score "
    "ON matches(match_score DESC)",
    "CREATE INDEX IF NOT EXISTS idx_matches_ai_method_score "
    "ON matches(matching_method, match_score DESC)",
]


# ── Database class ────────────────────────────────────────────────────────────

class Database:
    """Thread-safe SQLite wrapper with high-level CRUD for all entities."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self._path = db_path or settings.DB_PATH
        self._lock = Lock()

    def initialize(self) -> None:
        """Create schema and run migrations (idempotent). Call once at startup."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            conn.executescript(_DDL)
            self._run_migrations(conn)
        logger.info(f"Database initialised: {self._path}")

    @staticmethod
    def _run_migrations(conn: sqlite3.Connection) -> None:
        """Apply additive migrations, ignoring already-applied ones."""
        for stmt in _MIGRATIONS:
            try:
                conn.execute(stmt)
            except sqlite3.OperationalError as exc:
                msg = str(exc).lower()
                if "duplicate column" in msg or "already exists" in msg:
                    pass  # already applied
                else:
                    raise

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        with self._lock:
            conn = sqlite3.connect(str(self._path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    # ── Products ──────────────────────────────────────────────────────────────

    def upsert_product(self, p: ShopeeProduct) -> int:
        now = datetime.utcnow().isoformat()
        sql = """
            INSERT INTO products
                (title, price, sales, rating, review_count, seller, image, url,
                 keyword, market, product_key, product_key_confidence,
                 created_at, updated_at)
            VALUES
                (:title,:price,:sales,:rating,:review_count,:seller,:image,:url,
                 :keyword,:market,:product_key,:pk_confidence,:now,:now)
            ON CONFLICT(url) DO UPDATE SET
                title                  = excluded.title,
                price                  = excluded.price,
                sales                  = excluded.sales,
                rating                 = excluded.rating,
                review_count           = excluded.review_count,
                seller                 = excluded.seller,
                image                  = excluded.image,
                keyword                = excluded.keyword,
                market                 = excluded.market,
                product_key            = excluded.product_key,
                product_key_confidence = excluded.product_key_confidence,
                updated_at             = excluded.updated_at
        """
        with self.connection() as conn:
            cur = conn.execute(sql, {
                "title": p.title,
                "price": p.price,
                "sales": p.sales_count,
                "rating": p.rating,
                "review_count": p.review_count,
                "seller": p.seller,
                "image": p.image_url,
                "url": p.product_url,
                "keyword": p.keyword,
                "market": p.market,
                "product_key": p.product_key,
                "pk_confidence": p.product_key_confidence,
                "now": now,
            })
            return cur.lastrowid  # type: ignore[return-value]

    def get_products(
        self,
        keyword: Optional[str] = None,
        min_sales: int = 0,
        min_rating: float = 0.0,
        min_price: float = 0.0,
        market: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """Fetch products with optional filters."""
        sql = "SELECT * FROM products WHERE sales >= ? AND rating >= ? AND price >= ?"
        params: list = [min_sales, min_rating, min_price]
        if keyword:
            sql += " AND keyword = ?"
            params.append(keyword)
        if market:
            sql += " AND market = ?"
            params.append(market)
        sql += " ORDER BY sales DESC LIMIT ?"
        params.append(limit)
        with self.connection() as conn:
            return [dict(r) for r in conn.execute(sql, params).fetchall()]

    def get_product_id(self, url: str) -> Optional[int]:
        with self.connection() as conn:
            row = conn.execute("SELECT id FROM products WHERE url=?", [url]).fetchone()
        return row["id"] if row else None

    def get_products_by_key(self, product_key: str) -> List[Dict[str, Any]]:
        """Return all Shopee products that share the same product_key."""
        with self.connection() as conn:
            return [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM products WHERE product_key=?", [product_key]
                ).fetchall()
            ]

    def set_product_key(self, url: str, product_key: str, confidence: str) -> None:
        """Backfill product_key for an existing row identified by URL."""
        with self.connection() as conn:
            conn.execute(
                "UPDATE products SET product_key=?, product_key_confidence=?, "
                "updated_at=? WHERE url=?",
                [product_key, confidence, datetime.utcnow().isoformat(), url],
            )

    # ── Trends ────────────────────────────────────────────────────────────────

    def save_trend(self, trend: TrendData) -> int:
        sql = """
            INSERT INTO trends
                (product_url,sales_velocity,review_growth,price_stability,
                 direction,trend_score,computed_at)
            VALUES (?,?,?,?,?,?,?)
        """
        with self.connection() as conn:
            cur = conn.execute(sql, [
                trend.product_url, trend.sales_velocity, trend.review_growth_rate,
                trend.price_stability, trend.trend_direction,
                trend.trend_score, trend.computed_at.isoformat(),
            ])
            return cur.lastrowid  # type: ignore[return-value]

    def get_latest_trends(self, limit: int = 100) -> List[Dict[str, Any]]:
        sql = """
            SELECT t.*, p.title, p.price, p.sales, p.keyword
            FROM trends t
            JOIN products p ON p.url = t.product_url
            WHERE t.computed_at = (
                SELECT MAX(t2.computed_at) FROM trends t2
                WHERE t2.product_url = t.product_url
            )
            ORDER BY t.trend_score DESC
            LIMIT ?
        """
        with self.connection() as conn:
            return [dict(r) for r in conn.execute(sql, [limit]).fetchall()]

    # ── Sources (Japan products) ──────────────────────────────────────────────

    def upsert_source(self, p: JapanProduct) -> int:
        now = datetime.utcnow().isoformat()
        sql = """
            INSERT INTO sources
                (title,price_jpy,stock,image,url,source,seller,condition,
                 product_key,product_key_confidence,created_at,updated_at)
            VALUES
                (:title,:price,:stock,:image,:url,:source,:seller,:condition,
                 :product_key,:pk_confidence,:now,:now)
            ON CONFLICT(url) DO UPDATE SET
                title                  = excluded.title,
                price_jpy              = excluded.price_jpy,
                stock                  = excluded.stock,
                image                  = excluded.image,
                seller                 = excluded.seller,
                product_key            = excluded.product_key,
                product_key_confidence = excluded.product_key_confidence,
                updated_at             = excluded.updated_at
        """
        with self.connection() as conn:
            cur = conn.execute(sql, {
                "title": p.title,
                "price": p.price_jpy,
                "stock": p.stock_status,
                "image": p.image_url,
                "url": p.product_url,
                "source": p.source,
                "seller": p.seller,
                "condition": p.condition,
                "product_key": p.product_key,
                "pk_confidence": p.product_key_confidence,
                "now": now,
            })
            return cur.lastrowid  # type: ignore[return-value]

    def get_source_id(self, url: str) -> Optional[int]:
        with self.connection() as conn:
            row = conn.execute("SELECT id FROM sources WHERE url=?", [url]).fetchone()
        return row["id"] if row else None

    def get_all_sources(self) -> List[Dict[str, Any]]:
        with self.connection() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM sources").fetchall()]

    def get_sources_by_key(self, product_key: str) -> List[Dict[str, Any]]:
        """Return all Japan sources that share the same product_key."""
        with self.connection() as conn:
            return [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM sources WHERE product_key=? ORDER BY price_jpy ASC",
                    [product_key],
                ).fetchall()
            ]

    def set_source_key(self, url: str, product_key: str, confidence: str) -> None:
        """Backfill product_key for an existing Japan source row."""
        with self.connection() as conn:
            conn.execute(
                "UPDATE sources SET product_key=?, product_key_confidence=?, "
                "updated_at=? WHERE url=?",
                [product_key, confidence, datetime.utcnow().isoformat(), url],
            )

    def update_source_stock(self, url: str, stock_status: str) -> None:
        with self.connection() as conn:
            conn.execute("UPDATE sources SET stock=? WHERE url=?", [stock_status, url])

    # ── Matches ───────────────────────────────────────────────────────────────

    def upsert_match(
        self,
        profit: ProfitResult,
        match_method: str = "title_fuzzy",
        confidence_level: str = MatchConfidence.MEDIUM_FUZZY,
        match_score: float = 0.0,
        matching_method: str = "keyword",
    ) -> Optional[int]:
        """Persist a match/profit row.

        Parameters
        ----------
        profit:
            Computed :class:`ProfitResult` for this pair.
        match_method:
            Structural match strategy used (product_key | barcode |
            brand_model | title_fuzzy).  Overridden by ``profit.match_method``
            when set.
        confidence_level:
            Reliability tier.  Overridden by ``profit.confidence_level``
            when set.
        match_score:
            AI similarity score from :class:`~src.product_matching.matcher.
            ProductMatchingAI` (0.0–1.0).  0.0 for legacy / non-AI rows.
        matching_method:
            'keyword' for exact/structural matches; 'ai_match' for pairs
            validated by :class:`~src.product_matching.matcher.ProductMatchingAI`.
        """
        shopee_id = self.get_product_id(profit.shopee_product.product_url)
        japan_id = self.get_source_id(profit.japan_product.product_url)
        if not shopee_id or not japan_id:
            return None
        # Prefer method / confidence stored on the ProfitResult when available
        method = profit.match_method if profit.match_method else match_method
        level = profit.confidence_level if profit.confidence_level else confidence_level
        sql = """
            INSERT INTO matches
                (shopee_product_id, japan_product_id, similarity, profit_jpy,
                 roi_percent, match_method, confidence_level,
                 match_score, matching_method, created_at)
            VALUES (:s,:j,:sim,:profit,:roi,:method,:conf,:mscore,:mmeth,:now)
            ON CONFLICT(shopee_product_id,japan_product_id) DO UPDATE SET
                similarity       = excluded.similarity,
                profit_jpy       = excluded.profit_jpy,
                roi_percent      = excluded.roi_percent,
                match_method     = excluded.match_method,
                confidence_level = excluded.confidence_level,
                match_score      = excluded.match_score,
                matching_method  = excluded.matching_method
        """
        with self.connection() as conn:
            cur = conn.execute(sql, {
                "s": shopee_id, "j": japan_id,
                "sim": profit.similarity_score, "profit": profit.profit_jpy,
                "roi": profit.roi_percent, "method": method, "conf": level,
                "mscore": match_score, "mmeth": matching_method,
                "now": datetime.utcnow().isoformat(),
            })
            return cur.lastrowid  # type: ignore[return-value]

    def get_profitable_matches(
        self,
        min_profit: float = 0,
        min_roi: float = 0,
        min_confidence: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return profitable matches, optionally filtered by confidence tier.

        Args:
            min_profit:    Minimum profit in JPY.
            min_roi:       Minimum ROI percent.
            min_confidence: One of MatchConfidence values.  If supplied only
                           matches at this confidence or better are returned
                           (order: exact > brand_model > high_fuzzy > medium_fuzzy
                           > low_fuzzy).
        """
        confidence_order = {
            "exact": 5,
            "brand_model": 4,
            "high_fuzzy": 3,
            "medium_fuzzy": 2,
            "low_fuzzy": 1,
        }
        sql = """
            SELECT m.*, p.title AS shopee_title, p.price AS shopee_price,
                   p.product_key AS shopee_key,
                   s.title AS japan_title, s.price_jpy, s.url AS japan_url,
                   s.source, s.product_key AS japan_key
            FROM matches m
            JOIN products p ON p.id = m.shopee_product_id
            JOIN sources  s ON s.id = m.japan_product_id
            WHERE m.profit_jpy >= ? AND m.roi_percent >= ?
            ORDER BY m.profit_jpy DESC
        """
        with self.connection() as conn:
            rows = [dict(r) for r in conn.execute(sql, [min_profit, min_roi]).fetchall()]

        if min_confidence:
            min_rank = confidence_order.get(min_confidence, 0)
            rows = [r for r in rows
                    if confidence_order.get(r.get("confidence_level", ""), 0) >= min_rank]
        return rows

    # ── Listings ──────────────────────────────────────────────────────────────

    def save_listing(self, listing: ShopeeListing) -> int:
        now = datetime.utcnow().isoformat()
        sql = """
            INSERT INTO listings
                (title,description,price,stock,images,category_id,brand,status,
                 keywords,source_url,profit_jpy,roi_percent,shopee_item_id,
                 payload_json,created_at,updated_at)
            VALUES
                (:title,:desc,:price,:stock,:images,:cat,:brand,:status,
                 :keywords,:src,:profit,:roi,:item_id,:payload,:now,:now)
        """
        with self.connection() as conn:
            cur = conn.execute(sql, {
                "title": listing.title, "desc": listing.description,
                "price": listing.price, "stock": listing.stock,
                "images": json.dumps(listing.images),
                "cat": listing.category_id, "brand": listing.brand,
                "status": listing.status,
                "keywords": json.dumps(listing.keywords),
                "src": listing.source_japan_url,
                "profit": listing.profit_jpy, "roi": listing.roi_percent,
                "item_id": listing.shopee_item_id,
                "payload": json.dumps(listing.to_api_payload()),
                "now": now,
            })
            return cur.lastrowid  # type: ignore[return-value]

    def update_listing(self, listing_id: int, **fields: Any) -> None:
        fields["updated_at"] = datetime.utcnow().isoformat()
        set_clause = ", ".join(f"{k}=:{k}" for k in fields)
        sql = f"UPDATE listings SET {set_clause} WHERE id=:_id"
        fields["_id"] = listing_id
        with self.connection() as conn:
            conn.execute(sql, fields)

    def get_listings(
        self, status: Optional[str] = None, limit: int = 200
    ) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM listings"
        params: list = []
        if status:
            sql += " WHERE status=?"
            params.append(status)
        sql += " ORDER BY profit_jpy DESC LIMIT ?"
        params.append(limit)
        with self.connection() as conn:
            return [dict(r) for r in conn.execute(sql, params).fetchall()]

    def get_listing_by_id(self, listing_id: int) -> Optional[Dict[str, Any]]:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM listings WHERE id=?", [listing_id]).fetchone()
        return dict(row) if row else None

    # ── Orders ────────────────────────────────────────────────────────────────

    def create_order(self, listing_id: int, order_status: str, amount: float) -> int:
        sql = """
            INSERT INTO orders (listing_id, order_status, order_amount, created_at)
            VALUES (?,?,?,?)
        """
        with self.connection() as conn:
            cur = conn.execute(sql, [
                listing_id, order_status, amount, datetime.utcnow().isoformat()
            ])
            return cur.lastrowid  # type: ignore[return-value]

    # ── Price history ─────────────────────────────────────────────────────────

    def record_price(self, japan_url: str, price_jpy: float) -> None:
        with self.connection() as conn:
            conn.execute(
                "INSERT INTO price_history (japan_url,price_jpy,recorded_at) VALUES (?,?,?)",
                [japan_url, price_jpy, datetime.utcnow().isoformat()],
            )

    def get_price_history(self, japan_url: str, limit: int = 30) -> List[Dict[str, Any]]:
        sql = """
            SELECT * FROM price_history WHERE japan_url=?
            ORDER BY recorded_at DESC LIMIT ?
        """
        with self.connection() as conn:
            return [dict(r) for r in conn.execute(sql, [japan_url, limit]).fetchall()]

    # ── Product snapshots ─────────────────────────────────────────────────────

    def record_snapshot(self, snapshot: ProductSnapshot) -> int:
        """Insert one point-in-time snapshot row for a Japan source product.

        Called by both the price monitor and the inventory monitor each time
        they visit a product URL.  The snapshot is always appended (INSERT,
        never upsert) so the full time-series is preserved for trend analysis.

        Args:
            snapshot: Populated ``ProductSnapshot`` model instance.

        Returns:
            ``id`` of the inserted row.
        """
        sql = """
            INSERT INTO product_snapshots
                (product_id, price_jpy, competitor_price, stock_status,
                 sales_count, review_count, exchange_rate, captured_at)
            VALUES (?,?,?,?,?,?,?,?)
        """
        with self.connection() as conn:
            cur = conn.execute(sql, [
                snapshot.product_id,
                snapshot.price_jpy,
                snapshot.competitor_price,
                snapshot.stock_status,
                snapshot.sales_count,
                snapshot.review_count,
                snapshot.exchange_rate,
                snapshot.captured_at.isoformat(),
            ])
            return cur.lastrowid  # type: ignore[return-value]

    def get_snapshots(
        self,
        product_id: int,
        limit: int = 100,
        since_hours: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return snapshots for one product, newest first.

        Args:
            product_id:   FK to ``sources.id``.
            limit:        Maximum rows to return.
            since_hours:  If set, only return rows captured within the last
                          *since_hours* hours.

        Returns:
            List of row dicts sorted by ``captured_at DESC``.
        """
        sql = "SELECT * FROM product_snapshots WHERE product_id=?"
        params: list = [product_id]
        if since_hours is not None:
            sql += " AND captured_at >= datetime('now', ?)"
            params.append(f"-{since_hours} hours")
        sql += " ORDER BY captured_at DESC LIMIT ?"
        params.append(limit)
        with self.connection() as conn:
            return [dict(r) for r in conn.execute(sql, params).fetchall()]

    def get_latest_snapshot(self, product_id: int) -> Optional[Dict[str, Any]]:
        """Return the single most recent snapshot for a product.

        Used by the inventory monitor to compare current stock status against
        the last known status without fetching the full history.

        Returns:
            Row dict or ``None`` if no snapshot exists yet.
        """
        sql = """
            SELECT * FROM product_snapshots
            WHERE product_id=?
            ORDER BY captured_at DESC
            LIMIT 1
        """
        with self.connection() as conn:
            row = conn.execute(sql, [product_id]).fetchone()
        return dict(row) if row else None

    def get_price_delta(
        self,
        product_id: int,
        window_hours: int = 24,
    ) -> Optional[PriceDelta]:
        """Compute price movement for one product over the last *window_hours*.

        Used by the price monitor to decide whether to raise a ``PriceAlert``.
        Returns ``None`` if there are fewer than two snapshots in the window.

        Algorithm:
            earliest_price = price_jpy of the oldest snapshot in the window
            latest_price   = price_jpy of the newest snapshot
            delta_jpy      = latest_price − earliest_price
            delta_pct      = delta_jpy / earliest_price × 100

        Args:
            product_id:   FK to ``sources.id``.
            window_hours: Look-back window in hours.

        Returns:
            ``PriceDelta`` or ``None``.
        """
        sql = """
            SELECT
                MIN(price_jpy) FILTER (WHERE captured_at = (
                    SELECT MIN(captured_at) FROM product_snapshots
                    WHERE product_id=:pid
                    AND captured_at >= datetime('now', :window)
                )) AS earliest_price,
                MAX(price_jpy) FILTER (WHERE captured_at = (
                    SELECT MAX(captured_at) FROM product_snapshots
                    WHERE product_id=:pid
                    AND captured_at >= datetime('now', :window)
                )) AS latest_price,
                COUNT(*) AS cnt
            FROM product_snapshots
            WHERE product_id=:pid
            AND captured_at >= datetime('now', :window)
        """
        window_str = f"-{window_hours} hours"
        with self.connection() as conn:
            row = conn.execute(sql, {"pid": product_id, "window": window_str}).fetchone()

        if not row or row["cnt"] < 2:
            return None
        earliest = row["earliest_price"]
        latest = row["latest_price"]
        if earliest is None or earliest == 0:
            return None

        delta_jpy = latest - earliest
        delta_pct = (delta_jpy / earliest) * 100.0
        return PriceDelta(
            product_id=product_id,
            earliest_price_jpy=earliest,
            latest_price_jpy=latest,
            delta_jpy=round(delta_jpy, 2),
            delta_pct=round(delta_pct, 2),
            window_hours=window_hours,
            snapshots_in_window=row["cnt"],
        )

    def get_stock_changes(
        self,
        product_id: int,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Return rows where stock_status changed from the previous snapshot.

        Uses SQLite's LAG() window function to compare each row's stock_status
        against the previous row's value, returning only the transition rows.
        Used by the inventory monitor to detect out-of-stock and restock events
        without scanning the full history on every run.

        Args:
            product_id: FK to ``sources.id``.
            limit:      Maximum transitions to return (newest first).

        Returns:
            List of row dicts — each has all snapshot columns plus
            ``prev_stock_status`` (the status before the transition).
        """
        sql = """
            WITH ordered AS (
                SELECT *,
                       LAG(stock_status) OVER (
                           PARTITION BY product_id
                           ORDER BY captured_at
                       ) AS prev_stock_status
                FROM product_snapshots
                WHERE product_id=?
            )
            SELECT * FROM ordered
            WHERE prev_stock_status IS NOT NULL
              AND stock_status != prev_stock_status
            ORDER BY captured_at DESC
            LIMIT ?
        """
        with self.connection() as conn:
            return [dict(r) for r in conn.execute(sql, [product_id, limit]).fetchall()]

    def get_products_needing_profit_recalc(
        self,
        min_price_delta_pct: float = 5.0,
        window_hours: int = 24,
    ) -> List[Dict[str, Any]]:
        """Find Japan source products whose price changed significantly.

        Called by the profit engine at the start of each optimisation run to
        identify products where a re-calculation of profit_jpy / roi_percent
        is worthwhile.  Only products with at least two snapshots in the window
        and a price change ≥ *min_price_delta_pct* are returned.

        The caller (profit engine) should then:
          1. Re-fetch the active match for each returned product_id.
          2. Re-compute profit using the latest price_jpy and exchange_rate.
          3. Call ``upsert_match()`` to persist the new values.
          4. Call ``update_listing()`` if the linked listing is active.

        Args:
            min_price_delta_pct: Minimum absolute % change to trigger recalc.
            window_hours:        Look-back window for the price delta query.

        Returns:
            List of dicts with keys: product_id, earliest_price_jpy,
            latest_price_jpy, delta_jpy, delta_pct, snapshots_in_window,
            latest_exchange_rate, latest_stock_status.
        """
        window_str = f"-{window_hours} hours"
        # Correlated subqueries to fetch the chronologically earliest and latest
        # price in the window — simpler and more portable than FIRST_VALUE/LAST_VALUE.
        sql = """
            SELECT
                ps.product_id,
                COUNT(*)                    AS snapshots_in_window,
                -- Chronologically earliest price in window
                (SELECT price_jpy
                 FROM product_snapshots ep
                 WHERE ep.product_id = ps.product_id
                   AND ep.captured_at >= datetime('now', :window)
                 ORDER BY ep.captured_at ASC  LIMIT 1) AS earliest_price_jpy,
                -- Chronologically latest price in window
                (SELECT price_jpy
                 FROM product_snapshots lp
                 WHERE lp.product_id = ps.product_id
                   AND lp.captured_at >= datetime('now', :window)
                 ORDER BY lp.captured_at DESC LIMIT 1) AS latest_price_jpy,
                -- Latest exchange rate overall (not just in window)
                (SELECT exchange_rate
                 FROM product_snapshots xr
                 WHERE xr.product_id = ps.product_id
                 ORDER BY xr.captured_at DESC LIMIT 1) AS latest_exchange_rate,
                -- Latest stock status overall
                (SELECT stock_status
                 FROM product_snapshots ss
                 WHERE ss.product_id = ps.product_id
                 ORDER BY ss.captured_at DESC LIMIT 1) AS latest_stock_status
            FROM product_snapshots ps
            WHERE ps.captured_at >= datetime('now', :window)
            GROUP BY ps.product_id
            HAVING snapshots_in_window >= 2
        """
        with self.connection() as conn:
            raw_rows = [dict(r) for r in conn.execute(sql, {"window": window_str}).fetchall()]

        # Filter by price delta threshold in Python (avoids repeating the CASE WHEN logic)
        results = []
        for r in raw_rows:
            ep = r["earliest_price_jpy"]
            lp = r["latest_price_jpy"]
            if ep is None or ep == 0:
                continue
            delta_jpy = lp - ep
            delta_pct = (delta_jpy / ep) * 100.0
            if abs(delta_pct) >= min_price_delta_pct:
                r["delta_jpy"] = round(delta_jpy, 2)
                r["delta_pct"] = round(delta_pct, 2)
                results.append(r)

        results.sort(key=lambda r: abs(r["delta_pct"]), reverse=True)
        return results

    def purge_old_snapshots(self, retention_days: int = 90) -> int:
        """Delete snapshot rows older than *retention_days* days.

        Should be called once per day by the scheduler to prevent unbounded
        growth of the product_snapshots table.

        Args:
            retention_days: Rows older than this many days are removed.

        Returns:
            Number of rows deleted.
        """
        cutoff = f"-{retention_days} days"
        with self.connection() as conn:
            cur = conn.execute(
                "DELETE FROM product_snapshots WHERE captured_at < datetime('now', ?)",
                [cutoff],
            )
            deleted = cur.rowcount
        if deleted:
            logger.info(f"Purged {deleted} snapshot rows older than {retention_days} days")
        return deleted

    # ── Competitor prices ─────────────────────────────────────────────────────

    def save_competitor_price(
        self, keyword: str, title: str, price: float, url: str
    ) -> None:
        sql = """
            INSERT INTO competitor_prices
                (keyword, comp_title, comp_price, comp_url, scraped_at)
            VALUES (?,?,?,?,?)
        """
        with self.connection() as conn:
            conn.execute(sql, [keyword, title, price, url, datetime.utcnow().isoformat()])

    def get_lowest_competitor_price(self, keyword: str) -> Optional[float]:
        sql = """
            SELECT MIN(comp_price) FROM competitor_prices
            WHERE keyword=? AND scraped_at >= datetime('now', '-1 day')
        """
        with self.connection() as conn:
            row = conn.execute(sql, [keyword]).fetchone()
        return row[0] if row and row[0] else None

    # ── Optimisation log ──────────────────────────────────────────────────────

    def log_optimization(self, result: PriceOptimizationResult) -> None:
        sql = """
            INSERT INTO price_optimizations
                (listing_id,old_price,suggested_price,competitor_price,
                 reason,applied,optimized_at)
            VALUES (?,?,?,?,?,?,?)
        """
        with self.connection() as conn:
            conn.execute(sql, [
                result.listing_id, result.current_price, result.suggested_price,
                result.competitor_price, result.reason,
                1 if result.applied else 0,
                result.optimized_at.isoformat(),
            ])

    # ── Profit analysis ───────────────────────────────────────────────────────

    def save_profit_analysis(self, analysis: ProfitAnalysis) -> Optional[int]:
        """Persist one ``ProfitAnalysis`` record (upsert on pair uniqueness).

        Called by ``ProfitEngine.calculate()`` after every calculation.  The
        UNIQUE(shopee_product_id, japan_product_id) constraint ensures that
        re-running the profit engine overwrites stale values rather than
        accumulating duplicates.

        Args:
            analysis: Populated ``ProfitAnalysis`` model instance.

        Returns:
            Row ``id`` of the inserted/updated row, or ``None`` if either FK
            could not be resolved.
        """
        shopee_id = self.get_product_id(analysis.shopee_product_id)  # type: ignore[arg-type]
        japan_id = self.get_source_id_by_rowid(analysis.japan_product_id)
        # Fallback: accept pre-resolved integer IDs directly
        shopee_id = shopee_id if shopee_id is not None else analysis.shopee_product_id
        japan_id = japan_id if japan_id is not None else analysis.japan_product_id
        if not shopee_id or not japan_id:
            logger.warning(
                "save_profit_analysis: could not resolve FK ids "
                f"shopee={analysis.shopee_product_id} japan={analysis.japan_product_id}"
            )
            return None

        sql = """
            INSERT INTO profit_analysis
                (shopee_product_id, japan_product_id,
                 supplier_price, domestic_shipping_cost, safety_margin,
                 shopee_price, shopee_fee, fee_rate, exchange_rate,
                 net_revenue_jpy, cost_jpy, profit, roi, is_profitable,
                 match_method, confidence_level, similarity_score, analyzed_at)
            VALUES
                (:sid, :jid,
                 :supplier, :shipping, :safety,
                 :sprice, :sfee, :fee_rate, :rate,
                 :revenue, :cost, :profit, :roi, :profitable,
                 :method, :conf, :sim, :at)
            ON CONFLICT(shopee_product_id, japan_product_id) DO UPDATE SET
                supplier_price          = excluded.supplier_price,
                domestic_shipping_cost  = excluded.domestic_shipping_cost,
                safety_margin           = excluded.safety_margin,
                shopee_price            = excluded.shopee_price,
                shopee_fee              = excluded.shopee_fee,
                fee_rate                = excluded.fee_rate,
                exchange_rate           = excluded.exchange_rate,
                net_revenue_jpy         = excluded.net_revenue_jpy,
                cost_jpy                = excluded.cost_jpy,
                profit                  = excluded.profit,
                roi                     = excluded.roi,
                is_profitable           = excluded.is_profitable,
                match_method            = excluded.match_method,
                confidence_level        = excluded.confidence_level,
                similarity_score        = excluded.similarity_score,
                analyzed_at             = excluded.analyzed_at
        """
        with self.connection() as conn:
            cur = conn.execute(sql, {
                "sid": shopee_id,
                "jid": japan_id,
                "supplier": analysis.supplier_price,
                "shipping": analysis.domestic_shipping_cost,
                "safety": analysis.safety_margin,
                "sprice": analysis.shopee_price,
                "sfee": analysis.shopee_fee,
                "fee_rate": analysis.fee_rate,
                "rate": analysis.exchange_rate,
                "revenue": analysis.net_revenue_jpy,
                "cost": analysis.cost_jpy,
                "profit": analysis.profit,
                "roi": analysis.roi,
                "profitable": 1 if analysis.is_profitable else 0,
                "method": analysis.match_method,
                "conf": analysis.confidence_level,
                "sim": analysis.similarity_score,
                "at": analysis.analyzed_at.isoformat(),
            })
            return cur.lastrowid  # type: ignore[return-value]

    def get_source_id_by_rowid(self, rowid: int) -> Optional[int]:
        """Return sources.id for a given integer rowid (pass-through guard)."""
        with self.connection() as conn:
            row = conn.execute("SELECT id FROM sources WHERE id=?", [rowid]).fetchone()
        return row["id"] if row else None

    def get_profitable_analyses(
        self,
        min_profit_jpy: float = 0.0,
        min_roi: float = 0.0,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """Return profitable analysis rows joined with product titles.

        Args:
            min_profit_jpy: Minimum ``profit`` (JPY) threshold.
            min_roi:        Minimum ``roi`` (decimal, e.g. 0.30 = 30 %).
            limit:          Maximum rows.

        Returns:
            List of dicts sorted by ``profit DESC``.
        """
        sql = """
            SELECT pa.*,
                   p.title  AS shopee_title,
                   p.price  AS shopee_current_price,
                   p.url    AS shopee_url,
                   p.product_key AS shopee_key,
                   s.title  AS japan_title,
                   s.url    AS japan_url,
                   s.source AS japan_source,
                   s.product_key AS japan_key
            FROM profit_analysis pa
            JOIN products p ON p.id = pa.shopee_product_id
            JOIN sources  s ON s.id = pa.japan_product_id
            WHERE pa.is_profitable = 1
              AND pa.profit  >= :profit
              AND pa.roi     >= :roi
            ORDER BY pa.profit DESC
            LIMIT :limit
        """
        with self.connection() as conn:
            return [
                dict(r) for r in conn.execute(sql, {
                    "profit": min_profit_jpy,
                    "roi": min_roi,
                    "limit": limit,
                }).fetchall()
            ]

    def get_profit_analysis_by_pair(
        self,
        shopee_product_id: int,
        japan_product_id: int,
    ) -> Optional[Dict[str, Any]]:
        """Return the analysis row for a specific matched pair, or None."""
        sql = """
            SELECT * FROM profit_analysis
            WHERE shopee_product_id=? AND japan_product_id=?
        """
        with self.connection() as conn:
            row = conn.execute(sql, [shopee_product_id, japan_product_id]).fetchone()
        return dict(row) if row else None

    def get_analyses_needing_recalc(
        self,
        product_id: int,
    ) -> List[Dict[str, Any]]:
        """Return all profit_analysis rows linked to a Japan source product.

        Called by the profit recalculation pipeline after
        ``get_products_needing_profit_recalc()`` returns a product_id.
        The caller re-runs ``ProfitEngine.calculate()`` for each row and
        calls ``save_profit_analysis()`` to overwrite the stale result.

        Args:
            product_id: ``sources.id`` of the Japan product.

        Returns:
            List of row dicts (may be empty if the product has no analysis yet).
        """
        with self.connection() as conn:
            return [
                dict(r) for r in conn.execute(
                    "SELECT * FROM profit_analysis WHERE japan_product_id=?",
                    [product_id],
                ).fetchall()
            ]

    # ── Research candidates ───────────────────────────────────────────────────

    def upsert_research_candidate(self, candidate: ResearchCandidate) -> int:
        """Insert or update a research candidate (upsert on shopee_product_id).

        Re-running the Research AI engine on the same product overwrites the
        previous score so the table always holds the latest analysis.  The
        ``status`` field is preserved when updating an existing row — only a
        fresh insert sets it to ``'pending'``.

        Args:
            candidate: Populated ``ResearchCandidate`` model instance.

        Returns:
            Row ``id`` of the inserted/updated row.
        """
        sql = """
            INSERT INTO research_candidates
                (shopee_product_id, research_score,
                 score_demand, score_velocity, score_stability,
                 score_price_gap, score_brand,
                 reason, status, created_at)
            VALUES
                (:pid, :score,
                 :demand, :velocity, :stability,
                 :price_gap, :brand,
                 :reason, :status, :now)
            ON CONFLICT(shopee_product_id) DO UPDATE SET
                research_score  = excluded.research_score,
                score_demand    = excluded.score_demand,
                score_velocity  = excluded.score_velocity,
                score_stability = excluded.score_stability,
                score_price_gap = excluded.score_price_gap,
                score_brand     = excluded.score_brand,
                reason          = excluded.reason
                -- status is intentionally NOT updated so pipeline progress
                -- (matched / rejected) is not overwritten by a re-scan.
        """
        with self.connection() as conn:
            cur = conn.execute(sql, {
                "pid":      candidate.shopee_product_id,
                "score":    round(candidate.research_score, 4),
                "demand":   round(candidate.score_demand, 4),
                "velocity": round(candidate.score_velocity, 4),
                "stability":round(candidate.score_stability, 4),
                "price_gap":round(candidate.score_price_gap, 4),
                "brand":    round(candidate.score_brand, 4),
                "reason":   candidate.reason,
                "status":   candidate.status,
                "now":      candidate.created_at.isoformat(),
            })
            return cur.lastrowid  # type: ignore[return-value]

    def get_research_candidates(
        self,
        status: Optional[str] = "pending",
        min_score: float = 0.0,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """Return research candidates joined with product details.

        Args:
            status:    Filter by lifecycle state (``'pending'`` / ``'matched'``
                       / ``'rejected'``).  Pass ``None`` to return all statuses.
            min_score: Only return rows with ``research_score >= min_score``.
            limit:     Maximum rows to return.

        Returns:
            List of dicts — each has all ``research_candidates`` columns plus
            ``shopee_title``, ``shopee_price``, ``shopee_url``,
            ``shopee_sales``, ``shopee_rating``, ``shopee_market``,
            ``shopee_product_key``.
        """
        base = """
            SELECT rc.*,
                   p.title    AS shopee_title,
                   p.price    AS shopee_price,
                   p.url      AS shopee_url,
                   p.sales    AS shopee_sales,
                   p.rating   AS shopee_rating,
                   p.market   AS shopee_market,
                   p.product_key AS shopee_product_key,
                   p.keyword  AS shopee_keyword
            FROM research_candidates rc
            JOIN products p ON p.id = rc.shopee_product_id
            WHERE rc.research_score >= :min_score
        """
        params: Dict[str, Any] = {"min_score": min_score}
        if status is not None:
            base += " AND rc.status = :status"
            params["status"] = status
        base += " ORDER BY rc.research_score DESC LIMIT :limit"
        params["limit"] = limit
        with self.connection() as conn:
            return [dict(r) for r in conn.execute(base, params).fetchall()]

    def get_research_candidate(self, shopee_product_id: int) -> Optional[Dict[str, Any]]:
        """Return the research candidate row for a specific product, or None."""
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM research_candidates WHERE shopee_product_id=?",
                [shopee_product_id],
            ).fetchone()
        return dict(row) if row else None

    def update_candidate_status(
        self,
        shopee_product_id: int,
        status: str,
    ) -> None:
        """Advance the lifecycle state of a research candidate.

        Called by:
        - The Japan search pipeline when a supplier is found  (→ 'matched')
        - The profit engine when no profitable match remains  (→ 'rejected')

        Args:
            shopee_product_id: ``products.id`` of the Shopee product.
            status:            New status string (use ``ResearchCandidateStatus``
                               values: ``'pending'``, ``'matched'``, ``'rejected'``).
        """
        with self.connection() as conn:
            conn.execute(
                "UPDATE research_candidates SET status=? WHERE shopee_product_id=?",
                [status, shopee_product_id],
            )

    # ── Related product candidates ────────────────────────────────────────────

    def upsert_related_candidate(self, candidate: RelatedProductCandidate) -> int:
        """Insert or refresh a related-product keyword (upsert on the unique triple).

        Re-running the discovery engine on the same seed overwrites the previous
        confidence score so the table always contains up-to-date signals.

        Args:
            candidate: Populated ``RelatedProductCandidate`` instance.

        Returns:
            Row ``id`` of the inserted/updated row.
        """
        method_val = (
            candidate.discovery_method.value
            if isinstance(candidate.discovery_method, DiscoveryMethod)
            else str(candidate.discovery_method)
        )
        sql = """
            INSERT INTO related_product_candidates
                (seed_product_id, related_keyword, discovery_method,
                 confidence_score, created_at)
            VALUES (:seed, :keyword, :method, :score, :now)
            ON CONFLICT(seed_product_id, related_keyword, discovery_method)
            DO UPDATE SET
                confidence_score = excluded.confidence_score
        """
        with self.connection() as conn:
            cur = conn.execute(sql, {
                "seed":    candidate.seed_product_id,
                "keyword": candidate.related_keyword,
                "method":  method_val,
                "score":   round(candidate.confidence_score, 4),
                "now":     candidate.created_at.isoformat(),
            })
            return cur.lastrowid  # type: ignore[return-value]

    def get_related_candidates(
        self,
        method: Optional[str] = None,
        min_confidence: float = 0.0,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """Return related-product keywords joined with seed product details.

        Args:
            method:         Filter by discovery method (``'brand'`` / ``'series'``
                            / ``'keyword'`` / ``'category'``).  ``None`` = all.
            min_confidence: Only return rows with ``confidence_score >= min_confidence``.
            limit:          Maximum rows.

        Returns:
            List of dicts — each has all ``related_product_candidates`` columns
            plus ``seed_title``, ``seed_price``, ``seed_url``, ``seed_keyword``,
            ``seed_product_key``.  Sorted by ``confidence_score DESC``.
        """
        base = """
            SELECT rpc.*,
                   p.title   AS seed_title,
                   p.price   AS seed_price,
                   p.url     AS seed_url,
                   p.keyword AS seed_keyword,
                   p.product_key AS seed_product_key,
                   p.market  AS seed_market
            FROM related_product_candidates rpc
            JOIN products p ON p.id = rpc.seed_product_id
            WHERE rpc.confidence_score >= :min_confidence
        """
        params: Dict[str, Any] = {"min_confidence": min_confidence}
        if method is not None:
            base += " AND rpc.discovery_method = :method"
            params["method"] = method
        base += " ORDER BY rpc.confidence_score DESC LIMIT :limit"
        params["limit"] = limit
        with self.connection() as conn:
            return [dict(r) for r in conn.execute(base, params).fetchall()]

    def get_related_candidates_for_seed(
        self,
        seed_product_id: int,
        min_confidence: float = 0.0,
    ) -> List[Dict[str, Any]]:
        """Return all related keywords generated for one specific seed product.

        Args:
            seed_product_id: ``products.id`` of the seed Shopee product.
            min_confidence:  Minimum confidence threshold.

        Returns:
            List of row dicts sorted by ``confidence_score DESC``.
        """
        sql = """
            SELECT * FROM related_product_candidates
            WHERE seed_product_id = ? AND confidence_score >= ?
            ORDER BY confidence_score DESC
        """
        with self.connection() as conn:
            return [dict(r) for r in conn.execute(sql, [seed_product_id, min_confidence]).fetchall()]

    def count_related_candidates_for_seed(self, seed_product_id: int) -> int:
        """Return total number of discovered keywords for a seed product.

        Used by the discovery engine to skip seeds that already have the
        maximum number of keywords (``settings.DISCOVERY_MAX_KEYWORDS_PER_SEED``).
        """
        with self.connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM related_product_candidates WHERE seed_product_id=?",
                [seed_product_id],
            ).fetchone()
        return row[0] if row else 0

    # ── Statistics ────────────────────────────────────────────────────────────

    # ── Competitor Listings (Competition Analyzer AI) ─────────────────────────

    def upsert_competitor_listing(self, listing: "CompetitorListing") -> int:
        """Insert or refresh one competitor listing row.

        Keyed on (shopee_product_id, competitor_url) so re-scraping the same
        URL updates the price rather than creating a duplicate row.
        """
        sql = """
            INSERT INTO competitor_listings
                (shopee_product_id, product_key, competitor_title,
                 competitor_price, competitor_stock, seller_rating,
                 competitor_url, scraped_at)
            VALUES
                (:product_id, :pk, :title, :price, :stock, :rating, :url, :now)
            ON CONFLICT(shopee_product_id, competitor_url) DO UPDATE SET
                competitor_title  = excluded.competitor_title,
                competitor_price  = excluded.competitor_price,
                competitor_stock  = excluded.competitor_stock,
                seller_rating     = excluded.seller_rating,
                product_key       = excluded.product_key,
                scraped_at        = excluded.scraped_at
        """
        with self.connection() as conn:
            cur = conn.execute(sql, {
                "product_id": listing.shopee_product_id,
                "pk":         listing.product_key,
                "title":      listing.competitor_title,
                "price":      listing.competitor_price,
                "stock":      listing.competitor_stock,
                "rating":     listing.seller_rating,
                "url":        listing.competitor_url,
                "now":        listing.scraped_at.isoformat(),
            })
            return cur.lastrowid  # type: ignore[return-value]

    def get_competitor_listings(
        self,
        shopee_product_id: int,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Return all competitor listings for one product, cheapest first."""
        sql = """
            SELECT * FROM competitor_listings
            WHERE shopee_product_id = ?
            ORDER BY competitor_price ASC
            LIMIT ?
        """
        with self.connection() as conn:
            return [dict(r) for r in conn.execute(sql, [shopee_product_id, limit]).fetchall()]

    def get_competitor_listings_by_key(
        self,
        product_key: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Return all competitor listings that share a product_key."""
        sql = """
            SELECT * FROM competitor_listings
            WHERE product_key = ?
            ORDER BY competitor_price ASC
            LIMIT ?
        """
        with self.connection() as conn:
            return [dict(r) for r in conn.execute(sql, [product_key, limit]).fetchall()]

    def delete_stale_competitor_listings(
        self,
        shopee_product_id: int,
        before_iso: str,
    ) -> int:
        """Remove competitor rows scraped before *before_iso* for one product.

        Used to purge stale data before a fresh scrape of the same product.

        Returns:
            Number of rows deleted.
        """
        with self.connection() as conn:
            cur = conn.execute(
                "DELETE FROM competitor_listings "
                "WHERE shopee_product_id=? AND scraped_at < ?",
                [shopee_product_id, before_iso],
            )
            return cur.rowcount

    # ── Price Recommendations (Competition Analyzer AI) ───────────────────────

    def upsert_price_recommendation(self, rec: "PriceRecommendation") -> int:
        """Insert or overwrite the price recommendation for one product.

        One row per shopee_product_id — always reflects the latest run.
        """
        sql = """
            INSERT INTO price_recommendations
                (shopee_product_id, product_key, competitor_count,
                 min_market_price, median_market_price, max_market_price,
                 recommended_price, min_viable_price,
                 strategy_used, strategy_note, calculated_at)
            VALUES
                (:product_id, :pk, :comp_count,
                 :min_p, :med_p, :max_p,
                 :rec_p, :floor_p,
                 :strategy, :note, :now)
            ON CONFLICT(shopee_product_id) DO UPDATE SET
                product_key         = excluded.product_key,
                competitor_count    = excluded.competitor_count,
                min_market_price    = excluded.min_market_price,
                median_market_price = excluded.median_market_price,
                max_market_price    = excluded.max_market_price,
                recommended_price   = excluded.recommended_price,
                min_viable_price    = excluded.min_viable_price,
                strategy_used       = excluded.strategy_used,
                strategy_note       = excluded.strategy_note,
                calculated_at       = excluded.calculated_at
        """
        strategy_val = (
            rec.strategy_used.value
            if hasattr(rec.strategy_used, "value")
            else str(rec.strategy_used)
        )
        with self.connection() as conn:
            cur = conn.execute(sql, {
                "product_id": rec.shopee_product_id,
                "pk":         rec.product_key,
                "comp_count": rec.competitor_count,
                "min_p":      round(rec.min_market_price, 4),
                "med_p":      round(rec.median_market_price, 4),
                "max_p":      round(rec.max_market_price, 4),
                "rec_p":      round(rec.recommended_price, 4),
                "floor_p":    round(rec.min_viable_price, 4),
                "strategy":   strategy_val,
                "note":       rec.strategy_note,
                "now":        rec.calculated_at.isoformat(),
            })
            return cur.lastrowid  # type: ignore[return-value]

    def get_price_recommendation(
        self,
        shopee_product_id: int,
    ) -> Optional[Dict[str, Any]]:
        """Return the latest price recommendation for one product."""
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM price_recommendations WHERE shopee_product_id=?",
                [shopee_product_id],
            ).fetchone()
        return dict(row) if row else None

    def get_price_recommendations(
        self,
        limit: int = 200,
        min_competitor_count: int = 1,
    ) -> List[Dict[str, Any]]:
        """Return all price recommendations joined with product details.

        Sorted by recommended_price descending (highest potential revenue first).
        """
        sql = """
            SELECT pr.*,
                   p.title    AS shopee_title,
                   p.price    AS current_price,
                   p.url      AS shopee_url,
                   p.market   AS market
            FROM price_recommendations pr
            JOIN products p ON p.id = pr.shopee_product_id
            WHERE pr.competitor_count >= :min_count
            ORDER BY pr.recommended_price DESC
            LIMIT :limit
        """
        with self.connection() as conn:
            return [dict(r) for r in conn.execute(
                sql, {"min_count": min_competitor_count, "limit": limit}
            ).fetchall()]

    def get_stats(self) -> Dict[str, Any]:
        with self.connection() as conn:
            return {
                "products":           conn.execute("SELECT COUNT(*) FROM products").fetchone()[0],
                "products_with_key":  conn.execute(
                    "SELECT COUNT(*) FROM products WHERE product_key IS NOT NULL"
                ).fetchone()[0],
                "sources":            conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0],
                "sources_with_key":   conn.execute(
                    "SELECT COUNT(*) FROM sources WHERE product_key IS NOT NULL"
                ).fetchone()[0],
                "matches":            conn.execute("SELECT COUNT(*) FROM matches").fetchone()[0],
                "exact_matches":      conn.execute(
                    "SELECT COUNT(*) FROM matches WHERE confidence_level='exact'"
                ).fetchone()[0],
                "profitable_matches": conn.execute(
                    f"SELECT COUNT(*) FROM matches WHERE profit_jpy>={settings.MIN_PROFIT_YEN}"
                    f" AND roi_percent>={settings.MIN_ROI_PERCENT}"
                ).fetchone()[0],
                "listings":           conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0],
                "active_listings":    conn.execute(
                    "SELECT COUNT(*) FROM listings WHERE status='active'"
                ).fetchone()[0],
                "orders":             conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0],
                # Research AI
                "research_pending":  conn.execute(
                    "SELECT COUNT(*) FROM research_candidates WHERE status='pending'"
                ).fetchone()[0],
                "research_matched":  conn.execute(
                    "SELECT COUNT(*) FROM research_candidates WHERE status='matched'"
                ).fetchone()[0],
                "research_rejected": conn.execute(
                    "SELECT COUNT(*) FROM research_candidates WHERE status='rejected'"
                ).fetchone()[0],
                # Related Product Discovery AI
                "related_total":    conn.execute(
                    "SELECT COUNT(*) FROM related_product_candidates"
                ).fetchone()[0],
                "related_brand":    conn.execute(
                    "SELECT COUNT(*) FROM related_product_candidates WHERE discovery_method='brand'"
                ).fetchone()[0],
                "related_series":   conn.execute(
                    "SELECT COUNT(*) FROM related_product_candidates WHERE discovery_method='series'"
                ).fetchone()[0],
                "related_keyword":  conn.execute(
                    "SELECT COUNT(*) FROM related_product_candidates WHERE discovery_method='keyword'"
                ).fetchone()[0],
                "related_category": conn.execute(
                    "SELECT COUNT(*) FROM related_product_candidates WHERE discovery_method='category'"
                ).fetchone()[0],
                # Competition Analyzer AI
                "total_competitor_listings": conn.execute(
                    "SELECT COUNT(*) FROM competitor_listings"
                ).fetchone()[0],
                "total_price_recommendations": conn.execute(
                    "SELECT COUNT(*) FROM price_recommendations"
                ).fetchone()[0],
                # OpportunityDiscoveryAI
                "opportunity_scores": conn.execute(
                    "SELECT COUNT(*) FROM product_opportunity_scores"
                ).fetchone()[0],
            }

    # ── OpportunityDiscoveryAI ────────────────────────────────────────────────

    def upsert_opportunity_scores(
        self,
        scores: "list[Any]",
        keyword: str,
    ) -> None:
        """Persist OpportunityScore objects to ``product_opportunity_scores``.

        Parameters
        ----------
        scores:
            List of :class:`~src.opportunity_discovery.scorer.OpportunityScore`
            instances returned by ``OpportunityDiscoveryAI.score_products()``.
        keyword:
            The search keyword for this batch (used as part of the UNIQUE key).
        """
        if not scores:
            return

        now = datetime.utcnow().isoformat()
        sql = """
            INSERT INTO product_opportunity_scores
                (product_key, product_url, keyword, shopee_price,
                 opportunity_score, demand_score, competition_score,
                 price_spread_score, trust_score, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(product_url, keyword) DO UPDATE SET
                product_key        = excluded.product_key,
                shopee_price       = excluded.shopee_price,
                opportunity_score  = excluded.opportunity_score,
                demand_score       = excluded.demand_score,
                competition_score  = excluded.competition_score,
                price_spread_score = excluded.price_spread_score,
                trust_score        = excluded.trust_score,
                created_at         = excluded.created_at
        """
        rows = [
            (
                getattr(s.product, "product_key", None),
                s.product.product_url,
                keyword,
                s.product.price,
                s.opportunity_score,
                s.demand_score,
                s.competition_score,
                s.price_spread_score,
                s.trust_score,
                now,
            )
            for s in scores
        ]
        with self.connection() as conn:
            conn.executemany(sql, rows)


# Singleton
db = Database()
