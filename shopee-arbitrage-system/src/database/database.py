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
    JapanProduct,
    ListingStatus,
    MatchConfidence,
    MatchResult,
    PriceDelta,
    PriceOptimizationResult,
    ProfitResult,
    ProductSnapshot,
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
        """Apply additive column migrations, ignoring 'duplicate column' errors."""
        for stmt in _MIGRATIONS:
            try:
                conn.execute(stmt)
            except sqlite3.OperationalError as exc:
                if "duplicate column" in str(exc).lower():
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
    ) -> Optional[int]:
        shopee_id = self.get_product_id(profit.shopee_product.product_url)
        japan_id = self.get_source_id(profit.japan_product.product_url)
        if not shopee_id or not japan_id:
            return None
        # Prefer method / confidence stored on the ProfitResult when available
        method = profit.match_method if profit.match_method else match_method
        level = profit.confidence_level if profit.confidence_level else confidence_level
        sql = """
            INSERT INTO matches
                (shopee_product_id,japan_product_id,similarity,profit_jpy,roi_percent,
                 match_method,confidence_level,created_at)
            VALUES (:s,:j,:sim,:profit,:roi,:method,:conf,:now)
            ON CONFLICT(shopee_product_id,japan_product_id) DO UPDATE SET
                similarity      = excluded.similarity,
                profit_jpy      = excluded.profit_jpy,
                roi_percent     = excluded.roi_percent,
                match_method    = excluded.match_method,
                confidence_level= excluded.confidence_level
        """
        with self.connection() as conn:
            cur = conn.execute(sql, {
                "s": shopee_id, "j": japan_id,
                "sim": profit.similarity_score, "profit": profit.profit_jpy,
                "roi": profit.roi_percent, "method": method, "conf": level,
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

    # ── Statistics ────────────────────────────────────────────────────────────

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
            }


# Singleton
db = Database()
