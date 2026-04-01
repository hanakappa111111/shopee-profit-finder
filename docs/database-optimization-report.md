# Database Optimization Report
## Shopee Arbitrage Automation System

**Prepared by:** Senior Database Engineer
**Date:** 2026-03-26
**Scope:** Performance, indexing, snapshot optimization, query tuning, and scalability strategy for 100K+ products / 10M+ snapshot scale

---

## 1. Current Schema Performance Analysis

### 1.1 Table Inventory

| Table | Role | Growth Rate | Risk |
|---|---|---|---|
| `products` | Shopee scraped products | Moderate (bounded by keyword coverage) | Low |
| `sources` | Japan source products | Moderate | Low |
| `matches` | Product pair links | Grows with sources × products | Medium |
| `listings` | Managed Shopee listings | Bounded by business volume | Low |
| `trends` | Trend snapshots | **Unbounded — INSERT only, never upserted** | 🔴 High |
| `price_history` | Legacy Japan price log | **Unbounded — INSERT only** | 🔴 High |
| `product_snapshots` | Monitoring time-series | **Highest growth rate** (4× per day per source) | 🔴 High |
| `supplier_snapshots` | Supplier monitoring time-series | **Highest growth rate** (8× per day per source) | 🔴 High |
| `profit_analysis` | Per-pair profit calculations | One row per pair (UPSERT) | Low |
| `research_candidates` | Research AI queue | One row per product (UPSERT) | Low |
| `related_product_candidates` | Discovery AI keywords | Bounded by seeds × lookahead | Low |
| `competitor_listings` | Competition data | Refreshed per scrape run | Medium |
| `price_recommendations` | One row per product (UPSERT) | Bounded | Low |
| `competitor_prices` | Legacy competitor data | **Unbounded** | Medium |
| `price_optimizations` | Optimization audit log | **Unbounded — INSERT only** | Medium |
| `orders` | Order tracking | Bounded by business volume | Low |

### 1.2 Current Index Coverage

**Existing indexes (from DDL):**

```
products:     keyword | sales DESC | product_key | market
sources:      product_key
matches:      profit_jpy DESC | confidence_level
listings:     status
trends:       product_url
price_history: japan_url
competitor_prices: keyword
product_snapshots: (product_id, captured_at DESC) | captured_at DESC
profit_analysis:  (is_profitable, profit DESC) | roi DESC | analyzed_at DESC
research_candidates: research_score DESC | (status, research_score DESC)
related_product_candidates: confidence_score DESC | (seed_product_id, discovery_method) | (discovery_method, confidence_score DESC)
competitor_listings: (shopee_product_id, price ASC) | (product_key, price ASC) | scraped_at DESC
price_recommendations: shopee_product_id | product_key | calculated_at DESC
supplier_snapshots: (supplier_url, captured_at DESC) | (product_key, captured_at DESC) | captured_at DESC
```

### 1.3 Critical Gaps Identified

#### 🔴 P0 — Missing FK indexes on `matches` table

The `matches` table has **no indexes on its FK columns** `shopee_product_id` or `japan_product_id`. Every JOIN from `products` or `sources` into `matches` requires a full table scan of `matches`. At 100K matches this is already perceptible; at 1M+ it becomes blocking.

Affected queries: `get_profitable_matches()`, `get_active_matches_with_sources()`, `get_analyses_needing_recalc()`, all JOIN paths.

#### 🔴 P0 — Missing index on `listings.source_url`

`get_listings_by_source_url()` is called on **every single supplier monitoring check** — once per active match per monitoring cycle. With no index on `source_url`, each call is a full table scan of `listings`.

#### 🔴 P0 — Missing index on `profit_analysis.japan_product_id`

`get_analyses_needing_recalc()` queries `WHERE japan_product_id=?`. The only index covering `profit_analysis` has `shopee_product_id` as its leading column. A lookup by `japan_product_id` alone cannot use this index and falls back to a full table scan.

#### 🟠 P1 — `get_latest_trends()` correlated subquery

The query uses `WHERE t.computed_at = (SELECT MAX(t2.computed_at) FROM trends t2 WHERE t2.product_url = t.product_url)`. For every row in `trends`, SQLite must execute a separate subquery. This is **O(N²)** in the number of trend rows and will destroy performance as the trends table grows.

#### 🟠 P1 — `get_products_needing_profit_recalc()` triple correlated subqueries

This function uses **three separate correlated subqueries** per grouped `product_id` row (earliest price, latest price, latest exchange rate, latest stock status). Each subquery is an indexed seek, but with 100K sources each having multiple snapshots, this is still issuing 400K+ individual seeks per profit recalc run.

#### 🟠 P1 — `get_stats()` executes 15 separate `COUNT` queries

Each call to `get_stats()` opens the database connection 15 times, executing one `COUNT(*)` per statement. This can be collapsed into far fewer queries.

#### 🟡 P2 — `listings` status filter produces unsorted output

`get_listings(status=...)` sorts by `profit_jpy DESC` with a `WHERE status=?`. The existing `idx_listings_status` index covers the filter but SQLite must re-sort the output since `profit_jpy` is not part of the index.

#### 🟡 P2 — `products` multi-filter lacks composite index

`get_products(keyword, min_sales, min_rating, market)` applies four simultaneous filters. SQLite can only use one index at a time; the remaining filters are applied via table scan on the partial result.

#### 🟡 P2 — `competitor_prices` keyword + time filter lacks composite index

`get_lowest_competitor_price()` queries `WHERE keyword=? AND scraped_at >= datetime('now', '-1 day')`. The existing index is only on `keyword`. A composite `(keyword, scraped_at DESC)` would allow the time filter to be applied directly from the index without a post-filter scan.

#### 🟡 P2 — `trends` and `price_history` tables are unbounded

Both tables only support INSERT. At 8 trend snapshots per product per day, `trends` grows by ~800K rows per day for 100K products. Neither table has a cleanup job.

#### 🟡 P2 — PRAGMA overhead per connection

`PRAGMA journal_mode=WAL` is executed on every connection open. WAL mode persists in the database header after the first application — this pragma is a no-op on subsequent calls but still adds a round-trip per connection.

---

## 2. Recommended Indexes

All recommendations are delivered as additive migrations following the existing `_MIGRATIONS` pattern (safe to apply to live databases).

### 2.1 `matches` table — FK lookup indexes

```sql
-- Enable efficient JOIN from products → matches
CREATE INDEX IF NOT EXISTS idx_matches_shopee_id
    ON matches(shopee_product_id);

-- Enable efficient JOIN from sources → matches (profit recalc pipeline)
CREATE INDEX IF NOT EXISTS idx_matches_japan_id
    ON matches(japan_product_id);

-- Composite profit+ROI filter for get_profitable_matches()
CREATE INDEX IF NOT EXISTS idx_matches_profit_roi
    ON matches(profit_jpy DESC, roi_percent DESC);
```

**Impact:** Eliminates full table scans on `matches` for every JOIN and every monitoring cycle.

### 2.2 `listings` table — source URL and composite status

```sql
-- Critical: used on every supplier monitoring check
CREATE INDEX IF NOT EXISTS idx_listings_source_url
    ON listings(source_url);

-- Composite: status filter + profit sort in one pass
CREATE INDEX IF NOT EXISTS idx_listings_status_profit
    ON listings(status, profit_jpy DESC);
```

**Impact:** `get_listings_by_source_url()` drops from full table scan to single index seek.

### 2.3 `profit_analysis` — japan_product_id lookup

```sql
-- Required for get_analyses_needing_recalc(product_id)
CREATE INDEX IF NOT EXISTS idx_profit_analysis_japan_id
    ON profit_analysis(japan_product_id);

-- Composite for monitoring: profitable + shopee product
CREATE INDEX IF NOT EXISTS idx_profit_analysis_shopee_profitable
    ON profit_analysis(shopee_product_id, is_profitable, profit DESC);
```

**Impact:** Profit recalculation pipeline no longer scans the full `profit_analysis` table for each Japan product.

### 2.4 `trends` table — covering composite index

```sql
-- Replace correlated subquery with efficient latest-per-group lookup
CREATE INDEX IF NOT EXISTS idx_trends_url_time
    ON trends(product_url, computed_at DESC);
```

**Impact:** `get_latest_trends()` correlated subquery can use this index for O(log N) per product instead of O(N).

### 2.5 `product_snapshots` — additional monitoring patterns

```sql
-- Stock status queries: detect all products currently out of stock
CREATE INDEX IF NOT EXISTS idx_snapshots_stock_time
    ON product_snapshots(stock_status, captured_at DESC);

-- Covering index for get_price_delta() — avoids table page fetches
CREATE INDEX IF NOT EXISTS idx_snapshots_product_price_time
    ON product_snapshots(product_id, captured_at DESC, price_jpy, exchange_rate, stock_status);
```

**Impact:** `get_price_delta()` and `get_stock_changes()` can resolve from the index without touching the main table pages.

### 2.6 `sources` table — marketplace and price filters

```sql
-- Marketplace-scoped queries (monitoring dispatcher by source type)
CREATE INDEX IF NOT EXISTS idx_sources_source_price
    ON sources(source, price_jpy ASC);

-- Covering index for product_key lookups sorted by cheapest first
CREATE INDEX IF NOT EXISTS idx_sources_key_price
    ON sources(product_key, price_jpy ASC);
```

### 2.7 `products` table — composite market+sales filter

```sql
-- Multi-market arbitrage pipeline filter
CREATE INDEX IF NOT EXISTS idx_products_market_sales
    ON products(market, sales DESC);

-- Composite covering index for the Research AI scoring query
CREATE INDEX IF NOT EXISTS idx_products_sales_rating_price
    ON products(sales DESC, rating DESC, price DESC);
```

### 2.8 `competitor_prices` — composite keyword + time

```sql
-- Fix get_lowest_competitor_price() time window filter
CREATE INDEX IF NOT EXISTS idx_comp_prices_keyword_time
    ON competitor_prices(keyword, scraped_at DESC);
```

### 2.9 `price_history` — composite URL + time

```sql
-- Support time-bounded price history queries
CREATE INDEX IF NOT EXISTS idx_price_history_url_time
    ON price_history(japan_url, recorded_at DESC);
```

---

## 3. Snapshot Table Optimization

The `product_snapshots` and `supplier_snapshots` tables are the **primary scaling risk** in the system. They are the only tables that grow unboundedly at a rate proportional to monitoring frequency × product count.

### 3.1 Growth Projections

At 100K active matches with 4 monitoring cycles per day:

| Table | Rows/day | Rows/year (no cleanup) |
|---|---|---|
| `product_snapshots` | 400,000 | 146,000,000 |
| `supplier_snapshots` | 800,000 | 292,000,000 |

At 1M active matches: **4M rows/day in `product_snapshots` alone**.

### 3.2 Cleanup Strategy (Current — Partially Adequate)

The existing `purge_old_snapshots()` deletes from `product_snapshots` older than 90 days. This is correct for the primary snapshots table but is **missing for `supplier_snapshots`**.

Add to the database layer:

```python
def purge_old_supplier_snapshots(self, retention_days: int = 90) -> int:
    cutoff = f"-{retention_days} days"
    with self.connection() as conn:
        cur = conn.execute(
            "DELETE FROM supplier_snapshots WHERE captured_at < datetime('now', ?)",
            [cutoff],
        )
        return cur.rowcount
```

And register in the scheduler's `_run_snapshot_cleanup()` method.

### 3.3 Archival Strategy for Long-Term Analytics

Rather than hard-deleting old snapshots, move them to an archive table so historical profit analysis remains possible. This uses an `INSERT ... SELECT` + `DELETE` pattern in the cleanup job:

```sql
-- Archive table (same schema, no indexes other than product_id + captured_at)
CREATE TABLE IF NOT EXISTS product_snapshots_archive (
    id              INTEGER PRIMARY KEY,
    product_id      INTEGER NOT NULL,
    price_jpy       REAL    NOT NULL DEFAULT 0,
    competitor_price REAL,
    stock_status    TEXT    NOT NULL DEFAULT 'unknown',
    sales_count     INTEGER NOT NULL DEFAULT 0,
    review_count    INTEGER NOT NULL DEFAULT 0,
    exchange_rate   REAL,
    captured_at     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_snap_archive_product
    ON product_snapshots_archive(product_id, captured_at DESC);
```

Cleanup job then:
```sql
-- Step 1: copy old rows to archive
INSERT OR IGNORE INTO product_snapshots_archive
    SELECT * FROM product_snapshots
    WHERE captured_at < datetime('now', '-90 days');

-- Step 2: delete from live table
DELETE FROM product_snapshots WHERE captured_at < datetime('now', '-90 days');

-- Step 3: VACUUM the archive periodically (monthly)
```

### 3.4 Snapshot Deduplication

Currently both monitors record a snapshot on every visit regardless of whether anything changed. A deduplication guard that skips the INSERT when price and stock are identical to the previous snapshot would cut volume by 60–80% in stable periods.

Pattern to add in `insert_supplier_snapshot()`:

```python
prev = self.get_latest_supplier_snapshot(supplier_url)
if prev and prev["price_jpy"] == price_jpy and prev["stock_status"] == stock_status:
    return prev["id"]   # Skip insert — nothing changed
```

### 3.5 Partial Index for Monitoring Hot Path

For the monitoring hot path (fetching the latest snapshot per URL), a **partial index** covering only the most recent rows dramatically reduces index size:

```sql
-- Only index rows from the last 30 days (the monitoring hot window)
CREATE INDEX IF NOT EXISTS idx_supplier_snaps_url_recent
    ON supplier_snapshots(supplier_url, captured_at DESC)
    WHERE captured_at >= date('now', '-30 days');
```

Note: SQLite evaluates partial index conditions at query time only if the WHERE clause in the query exactly matches the partial index predicate. For maximum benefit, queries that target recent data should include the date filter explicitly.

---

## 4. Query Optimization Recommendations

### 4.1 Fix `get_latest_trends()` — O(N²) → O(N log N)

**Current (problematic):**
```sql
SELECT t.*, p.title, p.price, p.sales, p.keyword
FROM trends t
JOIN products p ON p.url = t.product_url
WHERE t.computed_at = (
    SELECT MAX(t2.computed_at) FROM trends t2
    WHERE t2.product_url = t.product_url   -- correlated: executes once per trend row
)
ORDER BY t.trend_score DESC
LIMIT ?
```

**Recommended (window function):**
```sql
WITH latest AS (
    SELECT product_url, MAX(computed_at) AS max_at
    FROM trends
    GROUP BY product_url      -- uses idx_trends_url_time
)
SELECT t.*, p.title, p.price, p.sales, p.keyword
FROM trends t
JOIN latest   l ON l.product_url = t.product_url AND t.computed_at = l.max_at
JOIN products p ON p.url = t.product_url
ORDER BY t.trend_score DESC
LIMIT ?
```

The CTE aggregation runs once, using the `(product_url, computed_at DESC)` index, and the outer join is a simple lookup.

### 4.2 Fix `get_products_needing_profit_recalc()` — Triple Correlated Subqueries

**Current:** Three correlated subqueries (`earliest_price`, `latest_price`, `latest_exchange_rate`, `latest_stock_status`) execute N times each for N grouped product IDs.

**Recommended:**
```sql
WITH window_data AS (
    SELECT
        product_id,
        price_jpy,
        exchange_rate,
        stock_status,
        captured_at,
        COUNT(*) OVER (PARTITION BY product_id) AS cnt,
        FIRST_VALUE(price_jpy)     OVER (PARTITION BY product_id ORDER BY captured_at ASC)  AS earliest_price_jpy,
        FIRST_VALUE(price_jpy)     OVER (PARTITION BY product_id ORDER BY captured_at DESC) AS latest_price_jpy,
        FIRST_VALUE(exchange_rate) OVER (PARTITION BY product_id ORDER BY captured_at DESC) AS latest_exchange_rate,
        FIRST_VALUE(stock_status)  OVER (PARTITION BY product_id ORDER BY captured_at DESC) AS latest_stock_status,
        ROW_NUMBER()               OVER (PARTITION BY product_id ORDER BY captured_at DESC) AS rn
    FROM product_snapshots
    WHERE captured_at >= datetime('now', :window)
)
SELECT product_id, cnt AS snapshots_in_window,
       earliest_price_jpy, latest_price_jpy,
       latest_exchange_rate, latest_stock_status
FROM window_data
WHERE rn = 1 AND cnt >= 2
```

All four values are derived in a single pass over the index-ordered rows. No correlated subqueries execute.

### 4.3 Batch `get_stats()` — 15 Queries → 3

**Current:** 15 separate `COUNT(*)` statements.

**Recommended:** Collapse into grouped queries:

```sql
-- Query 1: product/source/match/listing counts
SELECT
    (SELECT COUNT(*) FROM products)                                AS products,
    (SELECT COUNT(*) FROM products WHERE product_key IS NOT NULL)  AS products_with_key,
    (SELECT COUNT(*) FROM sources)                                 AS sources,
    (SELECT COUNT(*) FROM sources  WHERE product_key IS NOT NULL)  AS sources_with_key,
    (SELECT COUNT(*) FROM matches)                                 AS matches,
    (SELECT COUNT(*) FROM listings)                                AS listings,
    (SELECT COUNT(*) FROM listings WHERE status='active')          AS active_listings,
    (SELECT COUNT(*) FROM orders)                                  AS orders;

-- Query 2: research counts grouped by status
SELECT status, COUNT(*) FROM research_candidates GROUP BY status;

-- Query 3: related candidates grouped by method
SELECT discovery_method, COUNT(*) FROM related_product_candidates GROUP BY discovery_method;
```

This drops from 15 connection round-trips to 3.

### 4.4 `get_profitable_matches()` — Confidence Filter in SQL

The current implementation loads all matches above the profit/ROI threshold into Python and then filters by confidence level in a list comprehension. With 100K+ matches this means loading large result sets into memory.

**Recommendation:** Map confidence tier to a numeric rank and filter entirely in SQL:

```sql
SELECT m.*, p.title AS shopee_title, p.price AS shopee_price, ...
FROM matches m
JOIN products p ON p.id = m.shopee_product_id
JOIN sources  s ON s.id = m.japan_product_id
WHERE m.profit_jpy >= :min_profit
  AND m.roi_percent >= :min_roi
  AND CASE m.confidence_level
        WHEN 'exact'        THEN 5
        WHEN 'brand_model'  THEN 4
        WHEN 'high_fuzzy'   THEN 3
        WHEN 'medium_fuzzy' THEN 2
        WHEN 'low_fuzzy'    THEN 1
        ELSE 0
      END >= :min_rank
ORDER BY m.profit_jpy DESC
```

### 4.5 Add PRAGMA Tuning at Database Initialization

Move PRAGMA settings from every `connection()` call to the `initialize()` method which runs once at startup:

```python
def initialize(self) -> None:
    self._path.parent.mkdir(parents=True, exist_ok=True)
    with self.connection() as conn:
        # Performance PRAGMAs — set once, persist in WAL header
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")        # safe with WAL; 2× faster than FULL
        conn.execute("PRAGMA cache_size=-65536")         # 64 MB page cache
        conn.execute("PRAGMA temp_store=MEMORY")         # temp tables in RAM
        conn.execute("PRAGMA mmap_size=268435456")       # 256 MB memory-mapped I/O
        conn.execute("PRAGMA wal_autocheckpoint=1000")   # checkpoint every 1000 pages
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(_DDL)
        self._run_migrations(conn)
    logger.info(f"Database initialised: {self._path}")
```

Remove `PRAGMA journal_mode=WAL` and `PRAGMA foreign_keys=ON` from the per-connection `connection()` context manager. Keep `foreign_keys=ON` in `connection()` since it is a per-connection setting (not persisted), but skip `journal_mode` after the first run.

### 4.6 `get_active_matches_with_sources()` — Add Profitable Filter

This method loads **all matches** for each monitoring cycle. If the system has 500K matches but only 10K are profitable active ones, 490K rows are fetched and then most are discarded by the monitors.

**Recommendation:** Add an optional minimum profit filter:

```sql
SELECT m.id AS match_id, ...
FROM matches m
JOIN sources  s ON s.id = m.japan_product_id
JOIN products p ON p.id = m.shopee_product_id
WHERE m.profit_jpy > 0          -- exclude zero-profit/stale matches
ORDER BY m.profit_jpy DESC
```

### 4.7 Unbounded Tables — Add Retention Policy

Three tables currently have no cleanup:

| Table | Recommended Retention | Query Pattern |
|---|---|---|
| `trends` | 30 days | Latest per product only needed for scoring |
| `price_history` | 90 days | Legacy — can be superseded by `product_snapshots` |
| `competitor_prices` | 7 days | Only `scraped_at >= -1 day` is queried |
| `price_optimizations` | 180 days | Audit log |

Add purge methods following the existing `purge_old_snapshots()` pattern and register them in `_run_snapshot_cleanup()`.

---

## 5. Scalability Strategy

### 5.1 Phase 1: Current → 100K Products / 1M Snapshots

Apply all indexes from Section 2 via the migration system. Apply PRAGMA tuning from Section 4.5. Add snapshot deduplication from Section 3.4. Add retention cleanup for all unbounded tables.

**Expected result:** Monitoring cycles remain under 1 second per batch.

### 5.2 Phase 2: 100K → 500K Products / 10M Snapshots

**Introduce time-bucketed snapshot tables.** Rather than one monolithic `product_snapshots` table, split by rolling monthly periods:

```
product_snapshots_2026_03   -- current month (hot, fully indexed)
product_snapshots_2026_02   -- last month (warm, indexed)
product_snapshots_archive   -- older (cold, minimal indexes)
```

A `product_snapshots_view` VIEW unions all three. Queries against the view hit only the hot table when a `captured_at >= datetime('now', '-30 days')` filter is present — SQLite's query planner skips the archive table entirely. The monthly cleanup job promotes the previous month's table to archive.

**Introduce WAL checkpoint management.** At high write rates, the WAL file can grow large and slow down readers. Schedule a forced checkpoint (using `wal_checkpoint(TRUNCATE)`) at the end of each nightly cleanup run.

**Consider moving to PostgreSQL.** At 500K products, SQLite's single-writer model begins to limit throughput when multiple pipeline stages (scraping, matching, monitoring) run concurrently. PostgreSQL with MVCC allows true concurrent writers without the global `Lock` used in the current `Database` class.

### 5.3 Phase 3: 1M+ Products / 100M+ Snapshots

**Full migration to PostgreSQL.** The existing `Database` class's SQL is mostly portable (standard SQL, no SQLite-specific functions except `datetime('now', ...)` which maps to PostgreSQL's `NOW() - INTERVAL`). Key migration tasks:

- Replace `ON CONFLICT(url) DO UPDATE` → PostgreSQL `ON CONFLICT (url) DO UPDATE` (identical syntax)
- Replace `datetime('now', '-N hours')` → `NOW() - INTERVAL 'N hours'`
- Replace `INTEGER PRIMARY KEY AUTOINCREMENT` → `BIGSERIAL`
- Replace `TEXT` timestamps → `TIMESTAMPTZ`

**PostgreSQL-native optimizations to add at migration:**

| Feature | Purpose |
|---|---|
| Table partitioning by `captured_at` (range) | `product_snapshots` partition by month |
| Partial indexes with WHERE clause | Monitor-active rows only |
| `pg_partman` extension | Automated partition management |
| `timescaledb` extension | Time-series hypertables for snapshot tables |
| Read replicas | Monitoring queries hit replica; writes go to primary |
| `pg_stat_statements` | Identify slowest queries in production |

### 5.4 Connection Pool for Multi-Stage Pipeline

The current pattern creates and destroys one connection per method call. At high frequency (monitoring cycles checking 100K matches at 2s/check) this adds measurable overhead.

**Recommendation:** Implement thread-local persistent connections with a fixed maximum pool size:

```python
import threading

class Database:
    _local = threading.local()

    @contextmanager
    def connection(self):
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(str(self._path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
        conn = self._local.conn
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
```

The thread-local pattern ensures each worker thread has its own connection (safe for WAL mode) without the overhead of creating and closing on every method call. The global `Lock` can be removed since WAL mode allows concurrent readers and SQLite's file-level locking handles writer serialization.

### 5.5 Monitoring Architecture Optimization

At scale, the current monitoring model fetches all matches and checks each URL sequentially. A more scalable pattern:

1. **Group by marketplace**: Dispatch all `amazon_jp` URLs to one worker, all `rakuten` URLs to another, etc. Each marketplace has different rate limits and network latency profiles.

2. **Batch snapshots**: Instead of inserting one snapshot per match, batch-insert using `executemany()`:
   ```python
   conn.executemany(
       "INSERT INTO supplier_snapshots (supplier_url, price_jpy, stock_status, captured_at) VALUES (?,?,?,?)",
       [(url, price, stock, now) for url, price, stock in results],
   )
   ```

3. **Delta-only writes**: Only write a snapshot if price or stock changed (deduplication from Section 3.4). At steady state this reduces writes by 70-80%.

---

## 6. Implementation Plan

The following migrations are safe to apply additively to any live database. They are ordered by impact priority.

### Migration File: `src/database/db_optimization_migrations.py`

See companion file `src/database/db_optimization_migrations.py` for the complete migration list ready to append to `_MIGRATIONS` in `database.py`.

### Priority Order

| Priority | Migration | Impact |
|---|---|---|
| P0 | `idx_matches_shopee_id` | Eliminates JOIN full-scan on matches |
| P0 | `idx_matches_japan_id` | Eliminates profit recalc full-scan |
| P0 | `idx_listings_source_url` | Eliminates monitoring full-scan on listings |
| P0 | `idx_profit_analysis_japan_id` | Eliminates recalc full-scan on profit_analysis |
| P1 | `idx_matches_profit_roi` | Composite filter for get_profitable_matches() |
| P1 | `idx_listings_status_profit` | Composite for get_listings() with sort |
| P1 | `idx_trends_url_time` | Fix O(N²) correlated subquery |
| P2 | `idx_sources_source_price` | Marketplace-scoped monitoring dispatch |
| P2 | `idx_products_market_sales` | Multi-market product filter |
| P2 | `idx_comp_prices_keyword_time` | Time-window competitor price lookup |
| P2 | `idx_price_history_url_time` | Composite for price history queries |
| P3 | Snapshot deduplication | 60-80% write reduction on monitoring tables |
| P3 | Retention policy for trends/price_history/competitor_prices | Prevent unbounded growth |
| P3 | PRAGMA tuning at init | 20-30% general throughput improvement |
| P3 | Batch get_stats() queries | Reduce dashboard overhead from 15 → 3 queries |
