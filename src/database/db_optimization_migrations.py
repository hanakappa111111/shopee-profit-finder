"""
Database optimization migrations.

Append the contents of ``OPTIMIZATION_MIGRATIONS`` to the ``_MIGRATIONS`` list
in ``src/database/database.py`` to apply all performance indexes.

All statements use ``IF NOT EXISTS`` / ``IF EXISTS`` guards so they are safe
to run multiple times (idempotent).  The existing migration runner in
``Database._run_migrations()`` already ignores ``duplicate column`` errors;
these index migrations will simply no-op if the index already exists.

Priority ordering (apply in order):
  P0 — Blocking bottlenecks (missing FK indexes, missing monitoring indexes)
  P1 — High-impact composite indexes
  P2 — Supplementary composite indexes
  P3 — Cleanup and maintenance improvements
"""

from __future__ import annotations

# ── P0: Blocking bottlenecks ──────────────────────────────────────────────────

_P0_MIGRATIONS: list[str] = [
    # matches.shopee_product_id — required for every JOIN from products → matches.
    # Without this, the query planner does a full table scan of matches for each
    # product row during get_profitable_matches() and get_active_matches_with_sources().
    "CREATE INDEX IF NOT EXISTS idx_matches_shopee_id "
    "ON matches(shopee_product_id)",

    # matches.japan_product_id — required for JOIN from sources → matches and for
    # get_analyses_needing_recalc() which looks up matches by Japan product.
    "CREATE INDEX IF NOT EXISTS idx_matches_japan_id "
    "ON matches(japan_product_id)",

    # listings.source_url — called on every supplier monitoring check via
    # get_listings_by_source_url(). Without this index every check is a full
    # scan of the listings table.
    "CREATE INDEX IF NOT EXISTS idx_listings_source_url "
    "ON listings(source_url)",

    # profit_analysis.japan_product_id — get_analyses_needing_recalc() queries
    # WHERE japan_product_id=? but the existing UNIQUE index has shopee_product_id
    # as its leading column, making it unusable for japan_product_id lookups.
    "CREATE INDEX IF NOT EXISTS idx_profit_analysis_japan_id "
    "ON profit_analysis(japan_product_id)",
]


# ── P1: High-impact composite indexes ─────────────────────────────────────────

_P1_MIGRATIONS: list[str] = [
    # matches: composite profit + ROI filter — avoids post-filter sort on
    # get_profitable_matches(min_profit, min_roi).
    "CREATE INDEX IF NOT EXISTS idx_matches_profit_roi "
    "ON matches(profit_jpy DESC, roi_percent DESC)",

    # listings: composite status + profit sort — get_listings(status=...) sorts
    # by profit_jpy DESC; the existing status-only index cannot satisfy the sort.
    "CREATE INDEX IF NOT EXISTS idx_listings_status_profit "
    "ON listings(status, profit_jpy DESC)",

    # trends: composite url + time — fixes the O(N²) correlated subquery in
    # get_latest_trends(). The existing idx_trends_url indexes only url.
    "CREATE INDEX IF NOT EXISTS idx_trends_url_time "
    "ON trends(product_url, computed_at DESC)",

    # profit_analysis: shopee_product_id + profitable flag + profit sort.
    # Used by the monitoring pipeline to find all profitable analyses for a
    # given Shopee product quickly.
    "CREATE INDEX IF NOT EXISTS idx_profit_analysis_shopee_profitable "
    "ON profit_analysis(shopee_product_id, is_profitable, profit DESC)",

    # product_snapshots: covering composite for get_price_delta() and
    # get_products_needing_profit_recalc() — includes all columns read in
    # those queries so the index can serve the query without touching the
    # main table pages.
    "CREATE INDEX IF NOT EXISTS idx_snapshots_product_price_time "
    "ON product_snapshots(product_id, captured_at DESC, price_jpy, exchange_rate, stock_status)",

    # product_snapshots: stock_status + time — enables bulk stock-change
    # detection across all products in a single indexed scan.
    "CREATE INDEX IF NOT EXISTS idx_snapshots_stock_time "
    "ON product_snapshots(stock_status, captured_at DESC)",
]


# ── P2: Supplementary composite indexes ───────────────────────────────────────

_P2_MIGRATIONS: list[str] = [
    # sources: marketplace filter + price sort — used when dispatching
    # monitoring jobs by JapanSource type and when finding cheapest source.
    "CREATE INDEX IF NOT EXISTS idx_sources_source_price "
    "ON sources(source, price_jpy ASC)",

    # sources: product_key + price — improves get_sources_by_key() which
    # is already ordered by price_jpy ASC. The existing idx_sources_product_key
    # indexes only the key column.
    "CREATE INDEX IF NOT EXISTS idx_sources_key_price "
    "ON sources(product_key, price_jpy ASC)",

    # products: market + sales — multi-market product discovery pipeline filters
    # by market first, then orders by sales velocity.
    "CREATE INDEX IF NOT EXISTS idx_products_market_sales "
    "ON products(market, sales DESC)",

    # products: composite for Research AI scoring — sales, rating, price are
    # the three primary demand signals used in WHERE clauses of get_products().
    "CREATE INDEX IF NOT EXISTS idx_products_sales_rating_price "
    "ON products(sales DESC, rating DESC, price DESC)",

    # competitor_prices: keyword + time — fixes get_lowest_competitor_price()
    # which filters WHERE keyword=? AND scraped_at >= datetime('now', '-1 day').
    # The existing keyword-only index cannot apply the time filter from the index.
    "CREATE INDEX IF NOT EXISTS idx_comp_prices_keyword_time "
    "ON competitor_prices(keyword, scraped_at DESC)",

    # price_history: url + time — supports future time-bounded price history
    # queries and is needed by an efficient replacement for get_price_delta()
    # if price_history is used as the primary time-series rather than
    # product_snapshots.
    "CREATE INDEX IF NOT EXISTS idx_price_history_url_time "
    "ON price_history(japan_url, recorded_at DESC)",

    # price_optimizations: listing_id lookup + time — for audit queries
    # showing optimization history for a specific listing.
    "CREATE INDEX IF NOT EXISTS idx_price_optimizations_listing_time "
    "ON price_optimizations(listing_id, optimized_at DESC)",

    # orders: listing_id lookup — foreign key reference from listings → orders.
    "CREATE INDEX IF NOT EXISTS idx_orders_listing_id "
    "ON orders(listing_id, created_at DESC)",
]


# ── P3: Retention and maintenance migrations ──────────────────────────────────

_P3_MIGRATIONS: list[str] = [
    # supplier_snapshots archive table — holds rows older than retention_days.
    # The cleanup job copies old rows here before deleting from the live table
    # so historical supplier pricing data is preserved for long-term analysis.
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

    # product_snapshots archive table — mirrors product_snapshots schema.
    # Rows older than SNAPSHOT_RETENTION_DAYS are moved here rather than
    # hard-deleted so historical profit recalculation remains possible.
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
]


# ── Combined list for _MIGRATIONS in database.py ─────────────────────────────

OPTIMIZATION_MIGRATIONS: list[str] = (
    _P0_MIGRATIONS
    + _P1_MIGRATIONS
    + _P2_MIGRATIONS
    + _P3_MIGRATIONS
)


# ── PRAGMA recommendations (apply once at Database.initialize()) ──────────────
#
# These are NOT SQL migrations — they are PRAGMA statements to add to the
# Database.initialize() method in database.py.  Copy these into the
# initialize() method before conn.executescript(_DDL):
#
#   conn.execute("PRAGMA journal_mode=WAL")          # already set
#   conn.execute("PRAGMA synchronous=NORMAL")        # FULL → NORMAL: 2× faster, safe with WAL
#   conn.execute("PRAGMA cache_size=-65536")         # 64 MB page cache (default is 2MB)
#   conn.execute("PRAGMA temp_store=MEMORY")         # temp tables in RAM
#   conn.execute("PRAGMA mmap_size=268435456")       # 256 MB memory-mapped I/O
#   conn.execute("PRAGMA wal_autocheckpoint=1000")   # checkpoint every 1000 WAL pages
#
# Remove PRAGMA journal_mode=WAL from the per-connection connection() method —
# WAL mode persists in the database file header after the first initialize().
# Keep PRAGMA foreign_keys=ON in connection() since it is per-connection.
