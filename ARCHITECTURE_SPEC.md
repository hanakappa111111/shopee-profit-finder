# Shopee Arbitrage System — Final Architecture Specification

**Version:** 1.0
**Status:** Official Reference Document
**Scope:** Philippines · Singapore · Malaysia
**Focus:** High-margin anime figures and Japanese trading cards (Pokémon, One Piece, Bandai, Good Smile Company)

---

## TABLE OF CONTENTS

1. Current Architecture Summary
2. Architecture Problems and Risks
3. Final Recommended System Architecture
4. Data Flow Pipeline
5. Database Schema (Final Version)
6. Universal Product Identification Strategy
7. Product Matching Strategy
8. Profit Calculation Model
9. Automation Pipeline
10. Final Project Structure
11. Scalability Considerations

---

## 1. CURRENT ARCHITECTURE SUMMARY

### Overview

The system is implemented as a Python 3.11+ monolith organised into service-oriented modules under a single `src/` package tree. It is **not** a microservices architecture — all modules share one process, one SQLite database, and one settings singleton. This is an appropriate design choice for the current operational scale (one Shopee shop, a few hundred products per day).

### Current Module Inventory

| Module | Location | Role |
|---|---|---|
| Configuration | `src/config/settings.py` | Pydantic `BaseSettings` singleton loaded from `.env`. Covers all thresholds, API keys, schedule times. |
| Logger | `src/utils/logger.py` | Loguru instance with stderr + rotating daily file + errors-only file. |
| Retry | `src/utils/retry.py` | Parametric `@retry` decorator supporting both sync and async functions with exponential backoff. |
| Data Models | `src/database/models.py` | Pydantic v1 models used as the single source of truth for all data shapes across the system. |
| Database | `src/database/database.py` | Thread-safe SQLite wrapper (WAL mode, foreign keys, single Lock). Exposes high-level CRUD for all 9 tables. |
| Market Scraper | `src/market_analyzer/shopee_market_scraper.py` | Playwright async scraper for Shopee search results. Intercepts the internal `api/v4/search/search_items` XHR response first, falls back to DOM parsing. |
| Trend Detector | `src/market_analyzer/trend_detector.py` | Computes `sales_velocity`, `price_stability`, and `trend_score` (0–100) per product using weighted formula. Persists snapshots to `trends` table. |
| Winning Product Finder | `src/product_finder/winning_product_finder.py` | Filters products by hard thresholds (sales ≥ 200, rating ≥ 4.5, price ≥ ₱800). Computes composite `win_score` (sales 35%, rating 25%, price 20%, trend 20%). |
| Related Product Engine | `src/product_finder/related_product_engine.py` | Expands keywords using brand dictionaries (Pokémon → ポケモン, One Piece → ワンピース). Generates Japanese search queries for each winner. |
| Amazon JP Scraper | `src/japan_source/amazon_scraper.py` | `requests` + BS4 against `amazon.co.jp/s`. Parses price from `.a-price-whole`, stock from `#availability`. |
| Rakuten Scraper | `src/japan_source/rakuten_scraper.py` | `requests` + BS4 against `search.rakuten.co.jp`. Parses `div.searchresultitem` cards. |
| Yahoo Shopping Scraper | `src/japan_source/yahoo_scraper.py` | `requests` + BS4 against `shopping.yahoo.co.jp`. Multiple card selector fallbacks. |
| Mercari Scraper | `src/japan_source/mercari_scraper.py` | `requests` + JSON-LD parsing first, DOM fallback. Acknowledged JS limitation. Sets `condition="used"`. |
| Japan Source Searcher | `src/japan_source/mercari_scraper.py` `JapanSourceSearcher` | Aggregates all four Japan scrapers. Deduplicates by URL, sorts by price ascending. |
| Product Matcher | `src/matching/product_matcher.py` | RapidFuzz multi-strategy matching: `token_set_ratio`, `token_sort_ratio`, `partial_ratio`. Secondary pass using brand + model number extraction. |
| Profit Engine | `src/profit/profit_engine.py` | Full formula with live PHP→JPY rate (cached 1h). Filters by both `MIN_PROFIT_YEN` and `MIN_ROI_PERCENT`. |
| Title Generator | `src/ai/title_generator.py` | OpenAI ChatCompletion generating 5 optimised Shopee titles (≤120 chars, English, collector-focused). |
| Description Generator | `src/ai/description_generator.py` | OpenAI generating structured JSON with description paragraphs and bullet points. |
| Keyword Generator | `src/ai/keyword_generator.py` | OpenAI generating SEO keywords, hashtags, and search tags. |
| Listing Builder | `src/listing/listing_builder.py` | Assembles `ShopeeListing` from `ProfitResult`. Applies 10% price buffer, category heuristics, brand extraction. |
| Listing Manager | `src/listing/listing_manager.py` | Shopee Partner API v2 client with HMAC-SHA256 signing. Wraps `add_item`, `update_item`, `update_price`, `update_stock`. Provides `dry_run_create`. |
| Inventory Monitor | `src/monitoring/inventory_monitor.py` | Re-scrapes Japan product pages for stock status. On out-of-stock detection, updates associated Shopee listings to stock=0. |
| Price Monitor | `src/monitoring/price_monitor.py` | Re-scrapes Japan prices; fires `PriceAlert` on changes exceeding 5% threshold. Records all snapshots to `price_history`. |
| Price Optimizer | `src/optimizer/price_optimizer.py` | Scrapes Shopee competitor prices (using `__NEXT_DATA__` JSON when available). Computes undercut price at `competitor × (1 − 3%)`, floors at minimum margin. |
| Job Scheduler | `src/scheduler/job_scheduler.py` | `schedule` library wrapper with lazy imports to avoid circular dependencies. Runs market analysis daily, inventory checks 3× daily, price optimisation 2× daily. |
| Main Orchestrator | `main.py` | 8-step async pipeline with CLI interface (`run`, `schedule`, `monitor`, `optimize`, `stats`). |

### How the System Currently Operates

At startup, `main.py` calls `db.initialize()` to apply the DDL schema to SQLite. On `python main.py run`, the 8-step async pipeline executes sequentially:

```
Shopee scrape → Trend detection → Winner selection → Japan sourcing
→ Product matching → Profit calculation → Listing generation → DB save
```

The `schedule` command runs the pipeline once immediately, then enters a blocking `schedule.run_pending()` loop firing recurring jobs.

---

## 2. ARCHITECTURE PROBLEMS AND RISKS

### Problem 1 — Model Field Naming Inconsistency

**Location:** `src/profit/profit_engine.py` line 99
**Issue:** The `ProfitEngine.calculate()` method references `match.shopee_product.price_php`, but the `ShopeeProduct` model in `models.py` defines the field as `price` (not `price_php`). Similarly, `format_report` references `result.match_result` and `result.net_profit_jpy`, while `ProfitResult` in `models.py` defines `profit_jpy` and embeds products directly, not through a `match_result` wrapper. The `find_matches` method in `product_matcher.py` references `shopee.id` and `japan_product.id`, but the Pydantic models have no `id` field — IDs live only in the database rows.
**Risk:** These field mismatches will cause `AttributeError` at runtime when the profit calculation step executes. They must be corrected before the pipeline can run end-to-end.
**Resolution required:** Align `ProfitEngine` to use `match.shopee_product.price`, `match.japan_product.price_jpy`, and return `ProfitResult(shopee_product=..., japan_product=..., profit_jpy=..., ...)` exactly matching the model. Use URL-based keys for deduplication in `ProductMatcher`, not `.id`.

### Problem 2 — Listing Manager HMAC Signature Parameter Order

**Location:** `src/listing/listing_manager.py` line 52
**Issue:** The signature base string is constructed as `{path}{partner_id}{shop_id}{timestamp}`. The official Shopee Partner API v2 specification requires the order `{partner_id}{path}{timestamp}{access_token}{shop_id}`. The current implementation will generate invalid signatures and all API calls will be rejected with a signature error.
**Risk:** No live Shopee listing operations will succeed.
**Resolution required:** Update `_sign()` to use the correct Shopee-specified parameter order.

### Problem 3 — No `product_key` / Normalised Identity Field

**Location:** `database.py` `products` table, `sources` table
**Issue:** Products are uniquely identified solely by URL. There is no normalised product identity key (e.g. a hash of normalised brand + model number + edition) that spans both tables. This makes cross-platform identity resolution fragile — the same physical product from different Japan platforms will have different URLs and cannot be linked without re-running the full fuzzy matcher.
**Risk:** At scale, the same Japan product scraped from Amazon JP and Rakuten creates two separate rows in `sources` that are independently matched to the same Shopee product, causing duplicate profit records and inflated candidate counts.
**Resolution required:** Introduce a `product_key` TEXT column to both `products` and `sources` tables (see Section 6).

### Problem 4 — Trend Detection Relies on Snapshot, Not Time Series

**Location:** `src/market_analyzer/trend_detector.py`
**Issue:** `TrendDetector.compute_trend()` calculates velocity as `sales_count / days_since_created`. This divides cumulative lifetime sales by product age — it does not measure *recent* acceleration. A product with 1,000 sales created 500 days ago looks identical in velocity to one that gained all 1,000 sales in the past 7 days.
**Risk:** The trend detection cannot reliably identify genuinely emerging products. It effectively acts as a popularity filter, not a trend detector.
**Resolution required:** The `products` table needs to store periodic sales snapshots (a `product_snapshots` table tracking `(product_url, sales_count, recorded_at)`) so velocity can be computed as `Δsales / Δtime` over a configurable rolling window.

### Problem 5 — Mercari Scraper Has Near-Zero Expected Yield

**Location:** `src/japan_source/mercari_scraper.py`
**Issue:** Mercari's search results are server-side rendered via React. Static `requests` + BS4 will return the pre-hydration HTML shell, which contains no product listings. The code acknowledges this with a warning log but still constitutes a silent failure that makes the Japan source step appear to complete successfully when it has not fetched any Mercari data.
**Risk:** All Mercari-sourced products will be silently missing from the pipeline. This is only a correctness issue (missing data), not a crash risk.
**Resolution required:** Implement a Playwright-based Mercari scraper (matching the pattern of `shopee_market_scraper.py`) or use Mercari's public API if available. Mark it clearly as a `TODO: requires Playwright` in the current implementation rather than running the requests fallback that always returns empty.

### Problem 6 — Competitor Price Scraping Depends on Shopee's Next.js Structure

**Location:** `src/optimizer/price_optimizer.py`
**Issue:** The price optimizer attempts to read competitor prices from Shopee's `__NEXT_DATA__` script tag using static requests. Shopee's search results are dynamically loaded via XHR — the `__NEXT_DATA__` block in the initial HTML does not contain product price listings. This means `_scrape_shopee_prices()` will consistently return empty results and competitor-based price optimisation will never actually execute.
**Risk:** Price optimisation is entirely non-functional in the current implementation.
**Resolution required:** The competitor price scraper should use the same Playwright + XHR intercept pattern used in `shopee_market_scraper.py` to intercept the `api/v4/search/search_items` response.

### Problem 7 — Single-File SQLite Bottleneck at Scale

**Location:** `src/database/database.py`
**Issue:** The `Database` class uses a single `threading.Lock` to serialise all SQLite writes. At the current scale (one pipeline run per day, ~100 products) this is adequate. As product volume grows and monitoring jobs run concurrently (3× inventory checks + 2× price optimisations per day), this global lock will create queuing delays. SQLite in WAL mode supports concurrent reads but still serialises writes.
**Risk:** Low risk at current scale; becomes a bottleneck if monitoring volume exceeds ~1,000 products or if multiple markets run simultaneously.
**Resolution required (future):** For >10,000 products or multi-market operation, migrate the storage layer to PostgreSQL. The `Database` class interface should remain unchanged; only the backend changes.

### Problem 8 — Missing `profit_analysis` Intermediate Table

**Location:** `database.py` schema
**Issue:** Profit data is stored inline within the `matches` table (`profit_jpy`, `roi_percent`). There is no separate `profit_analysis` table that preserves the full breakdown dict (exchange rate used, fee applied, shipping cost at time of calculation). This means historical profit records cannot be audited if thresholds or exchange rates change.
**Risk:** Profit recalculation requires re-running the entire matching step. Regulatory or business audits of past decisions are impossible.
**Resolution required:** Add a `profit_analysis` table (see Section 5).

### Problem 9 — No Market Segmentation by Target Country

**Location:** `settings.py`, `database.py`
**Issue:** `SHOPEE_MARKET` is a single string setting (`PH`, `SG`, or `MY`). The `products` table has no `market` column. A seller operating across multiple Shopee markets cannot currently run the pipeline per-market or store per-market competitor prices and profit margins (which differ because PHP, SGD, and MYR exchange rates to JPY differ).
**Risk:** Multi-market operation requires manual settings changes and separate database files, which is brittle.
**Resolution required:** Add a `market` TEXT column to `products`, `listings`, and `competitor_prices` tables. Parameterise pipeline steps with the target market.

### Problem 10 — No Review Count Field for Trend Accuracy

**Location:** `src/database/models.py` `ShopeeProduct`
**Issue:** `TrendData` includes a `review_growth_rate` field, but `ShopeeProduct` has no `review_count` field. The trend detector cannot compute review growth because the data was never scraped.
**Risk:** The `review_growth_rate` metric is always 0.0, reducing the trend score's accuracy.
**Resolution required:** Add `review_count: int` to `ShopeeProduct` and extract it in `shopee_market_scraper.py`.

---

## 3. FINAL RECOMMENDED SYSTEM ARCHITECTURE

### Logical Architecture Layers

```
┌─────────────────────────────────────────────────────────┐
│                     CLI / main.py                       │
│           (Pipeline orchestration + scheduler)          │
├─────────────────────────────────────────────────────────┤
│  INTELLIGENCE LAYER                                     │
│  ┌──────────────────┐  ┌──────────────────────────────┐ │
│  │  Market Analyzer │  │    Product Finder            │ │
│  │  - Shopee scraper│  │    - WinningProductFinder    │ │
│  │  - TrendDetector │  │    - RelatedProductEngine    │ │
│  └──────────────────┘  └──────────────────────────────┘ │
├─────────────────────────────────────────────────────────┤
│  SCRAPING LAYER                                         │
│  ┌──────────┐ ┌─────────┐ ┌──────────┐ ┌─────────────┐ │
│  │Amazon JP │ │Rakuten  │ │  Yahoo   │ │   Mercari   │ │
│  │(requests)│ │(requests│ │(requests)│ │(Playwright) │ │
│  └──────────┘ └─────────┘ └──────────┘ └─────────────┘ │
│                  JapanSourceSearcher (aggregator)       │
├─────────────────────────────────────────────────────────┤
│  ANALYSIS LAYER                                         │
│  ┌────────────────────┐  ┌──────────────────────────┐   │
│  │  ProductMatcher    │  │     ProfitEngine          │   │
│  │  (RapidFuzz+brand) │  │  (formula + FX rate)      │   │
│  └────────────────────┘  └──────────────────────────┘   │
├─────────────────────────────────────────────────────────┤
│  CONTENT LAYER (AI)                                     │
│  ┌──────────────────┐ ┌────────────────┐ ┌──────────┐   │
│  │  TitleGenerator  │ │DescGenerator   │ │Keyword   │   │
│  │  (OpenAI)        │ │(OpenAI)        │ │Generator │   │
│  └──────────────────┘ └────────────────┘ └──────────┘   │
├─────────────────────────────────────────────────────────┤
│  LISTING LAYER                                          │
│  ┌────────────────────┐  ┌──────────────────────────┐   │
│  │  ListingBuilder    │  │    ListingManager        │   │
│  │  (assembles draft) │  │  (Shopee API v2 + HMAC)  │   │
│  └────────────────────┘  └──────────────────────────┘   │
├─────────────────────────────────────────────────────────┤
│  MONITORING & OPTIMISATION LAYER                        │
│  ┌─────────────────┐ ┌──────────────┐ ┌─────────────┐  │
│  │InventoryMonitor │ │ PriceMonitor │ │PriceOptimizer│ │
│  │(stock checks)   │ │(price alerts)│ │(undercut)    │  │
│  └─────────────────┘ └──────────────┘ └─────────────┘  │
├─────────────────────────────────────────────────────────┤
│  INFRASTRUCTURE LAYER                                   │
│  ┌────────────────┐  ┌───────────────┐  ┌───────────┐   │
│  │   Database     │  │ JobScheduler  │  │  Settings │   │
│  │ (SQLite/WAL)   │  │ (schedule lib)│  │  (Pydantic│   │
│  └────────────────┘  └───────────────┘  └───────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Module Responsibilities (Final)

**`src/config/settings.py`** — Single configuration authority. All thresholds, API credentials, schedule times, and feature flags live here. No module hardcodes business parameters.

**`src/utils/logger.py`** — Application-wide Loguru instance. Modules import `from src.utils.logger import logger`.

**`src/utils/retry.py`** — Generic `@retry` and `@retry_on_network_error` decorators. Used by all scrapers and API clients.

**`src/database/models.py`** — Pydantic v1 models. Every data structure in the system is defined here. No data dict should flow between modules without being validated through a model.

**`src/database/database.py`** — Thread-safe SQLite CRUD layer. All persistence goes through this class. No module writes raw SQL outside this file.

**`src/market_analyzer/shopee_market_scraper.py`** — Playwright async scraper for Shopee. Uses XHR interception as primary strategy. Stores results via `db.upsert_product()`.

**`src/market_analyzer/trend_detector.py`** — Computes `TrendData` per product using time-series velocity (requires `product_snapshots` table) and price stability. Persists via `db.save_trend()`.

**`src/product_finder/winning_product_finder.py`** — Applies hard-filter thresholds then composite `win_score` to return `List[WinningProduct]`.

**`src/product_finder/related_product_engine.py`** — Keyword expansion and bilingual Japan search query generation. No DB access — pure transformation logic.

**`src/japan_source/amazon_scraper.py`** — Scrapes `amazon.co.jp` via requests + BS4. Returns `List[JapanProduct]` with `source=JapanSource.AMAZON_JP`.

**`src/japan_source/rakuten_scraper.py`** — Scrapes Rakuten Ichiba. Returns `List[JapanProduct]` with `source=JapanSource.RAKUTEN`.

**`src/japan_source/yahoo_scraper.py`** — Scrapes Yahoo Shopping Japan. Returns `List[JapanProduct]` with `source=JapanSource.YAHOO_SHOPPING`.

**`src/japan_source/mercari_scraper.py`** — Scrapes Mercari Japan (Playwright-required for full yield). Returns `List[JapanProduct]` with `source=JapanSource.MERCARI`, `condition="used"`. Also houses `JapanSourceSearcher` aggregator.

**`src/matching/product_matcher.py`** — Multi-strategy RapidFuzz matcher returning `List[MatchResult]`. Three strategies: title fuzzy, brand+model, barcode (EAN-13). Threshold configurable from settings.

**`src/profit/profit_engine.py`** — Stateless profit formula. Reads live PHP→JPY FX rate (cached 1h). Returns `ProfitResult` with full `breakdown` dict. Filters by `MIN_PROFIT_YEN` AND `MIN_ROI_PERCENT`.

**`src/ai/title_generator.py`** — OpenAI title generation. Returns `GeneratedTitles` with 5 options. Falls back gracefully without crashing pipeline.

**`src/ai/description_generator.py`** — OpenAI description generation. Returns `GeneratedDescription`.

**`src/ai/keyword_generator.py`** — OpenAI keyword/hashtag/search-tag generation. Returns `GeneratedKeywords`.

**`src/listing/listing_builder.py`** — Combines `ProfitResult` + AI content into a complete `ShopeeListing`. Applies pricing buffer, category heuristics, and brand extraction.

**`src/listing/listing_manager.py`** — Shopee Partner API v2 client. HMAC-SHA256 signing using the correct parameter order. CRUD: `create_listing`, `update_price`, `update_stock`, `pause_listing`.

**`src/monitoring/inventory_monitor.py`** — Scheduled re-scraper for Japan product stock status. On out-of-stock detection: updates `sources` table and calls `listing_manager.update_stock(0)` for associated listings.

**`src/monitoring/price_monitor.py`** — Scheduled price snapshot collector. Records to `price_history`. Fires `PriceAlert` on changes > 5%.

**`src/optimizer/price_optimizer.py`** — Uses Playwright-intercepted Shopee search prices (not static requests) to compute `suggested_price = competitor_price × (1 − PRICE_UNDERCUT_PERCENT / 100)`, floored at `japan_cost_estimate + MIN_MARGIN_PHP`.

**`src/scheduler/job_scheduler.py`** — `schedule` library wrapper with lazy imports. Runs jobs in-process. Does not spawn threads per job (runs sequentially).

**`main.py`** — Async pipeline entry point and CLI router.

---

## 4. DATA FLOW PIPELINE

### Phase A — Market Intelligence

```
[1] Shopee Market Scrape
    ↓ ShopeeMarketScraper (Playwright, XHR intercept)
    ↓ Extracts: title, price, sales_count, rating, seller, image_url, product_url
    ↓ Saves → products table (upsert by URL)

[2] Snapshot Recording
    ↓ After each scrape run: insert (product_url, sales_count, recorded_at)
    ↓ Saves → product_snapshots table

[3] Trend Detection
    ↓ TrendDetector reads products + product_snapshots
    ↓ Computes: sales_velocity (Δsales/Δtime), price_stability, trend_score
    ↓ Assigns direction: RISING | STABLE | FALLING
    ↓ Saves → trends table
```

### Phase B — Product Selection

```
[4] Winning Product Identification
    ↓ WinningProductFinder reads products + latest trends from DB
    ↓ Hard filters: sales ≥ 200, rating ≥ 4.5, price ≥ ₱800
    ↓ Soft score: composite win_score (sales 35%, rating 25%, price 20%, trend 20%)
    ↓ Returns: List[WinningProduct] sorted by win_score DESC

[5] Japan Search Query Generation
    ↓ RelatedProductEngine generates 2–5 search queries per winner
    ↓ Includes: English variants + Japanese brand names (ポケモン, バンダイ, etc.)
    ↓ Returns: List[str] search terms per winner
```

### Phase C — Japan Sourcing

```
[6] Japan Platform Search
    ↓ JapanSourceSearcher dispatches query to all 4 scrapers in sequence
    ↓ Amazon JP → Rakuten → Yahoo Shopping → Mercari
    ↓ Extracts: title, price_jpy, stock_status, image_url, product_url, source
    ↓ Deduplicates by URL, sorts by price ASC
    ↓ Saves → sources table (upsert by URL)
```

### Phase D — Analysis

```
[7] Product Matching
    ↓ ProductMatcher compares each WinningProduct against all sourced JapanProducts
    ↓ Strategy 1: title fuzzy (token_set_ratio, token_sort_ratio, partial_ratio) ≥ 70
    ↓ Strategy 2: brand name exact + model number exact → score 95
    ↓ Strategy 3: EAN-13 barcode match → score 100
    ↓ Returns: List[MatchResult] sorted by similarity DESC

[8] Profit Calculation
    ↓ ProfitEngine calculates for each MatchResult:
    ↓   net_revenue_php  = shopee_price × 0.83
    ↓   net_revenue_jpy  = net_revenue_php × PHP/JPY_rate
    ↓   profit_jpy       = net_revenue_jpy - japan_price - 300
    ↓   roi_percent      = (profit_jpy / (japan_price + 300)) × 100
    ↓ Filters: profit_jpy ≥ 2,000 AND roi_percent ≥ 30%
    ↓ Saves → matches table + profit_analysis table
    ↓ Returns: List[ProfitResult] (profitable only)
```

### Phase E — Content Generation

```
[9] AI Listing Generation
    ↓ For each profitable ProfitResult:
    ↓   TitleGenerator → 5 candidate titles (≤120 chars, English)
    ↓   DescriptionGenerator → description + bullet points
    ↓   KeywordGenerator → SEO keywords + hashtags
    ↓ Fallback (no API key): template-based title/description
    ↓ ListingBuilder assembles ShopeeListing with:
    ↓   price = shopee_product.price × 1.10 (10% buffer)
    ↓   stock = 10 (default, replenishable)
    ↓   category = heuristic from title keywords
    ↓   brand = extracted from title
    ↓   status = DRAFT

[10] Candidate Persistence
    ↓ Saves → listings table (status=draft)
    ↓ Human review step (current: manual; future: auto-post)
```

### Phase F — Live Operations (Recurring)

```
[Inventory Monitor — 3× daily]
    ↓ Re-scrapes each active Japan source URL
    ↓ If OUT_OF_STOCK detected:
    ↓   → db.update_source_stock(url, "out_of_stock")
    ↓   → listing_manager.update_stock(shopee_item_id, 0)
    ↓   → db.update_listing(id, status="sold_out")

[Price Monitor — continuous / 6h]
    ↓ Re-scrapes each active Japan source URL for price
    ↓ If |Δprice| ≥ 5%:
    ↓   → db.record_price(url, new_price)
    ↓   → fires PriceAlert (log + optional callback)

[Price Optimizer — 2× daily]
    ↓ For each ACTIVE listing:
    ↓   → scrape current Shopee competitor prices (Playwright + XHR)
    ↓   → compute suggested_price = lowest_competitor × 0.97
    ↓   → floor at: (japan_cost + shipping) / 0.83 + MIN_MARGIN_PHP
    ↓   → log to price_optimizations table
    ↓   → if apply=True: listing_manager.update_price(...)
```

---

## 5. DATABASE SCHEMA (FINAL VERSION)

### Entity Relationship Overview

```
products ─────────────────────┐
    │                         │
    ↓ (1:many)               ↓ (many:many via matches)
product_snapshots          sources
                               │
trends (→ products.url)        ↓ (1:many)
                           price_history
matches (products × sources)
    │
    ↓ (1:1)
profit_analysis
    │
    ↓ (many:1)
listings
    │
    ├──→ orders (1:many)
    └──→ price_optimizations (1:many)

competitor_prices (standalone, keyed by keyword + market)
```

---

### Table: `products`

Shopee products scraped from market search.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment surrogate key |
| `product_key` | TEXT | Normalised identity hash (brand + model + edition). See Section 6. |
| `title` | TEXT NOT NULL | Original Shopee listing title |
| `price` | REAL NOT NULL | Current listing price in local currency |
| `currency` | TEXT DEFAULT 'PHP' | Currency code (PHP / SGD / MYR) |
| `sales_count` | INTEGER DEFAULT 0 | Cumulative units sold |
| `review_count` | INTEGER DEFAULT 0 | Total review count (for trend growth calculation) |
| `rating` | REAL DEFAULT 0.0 | Average star rating (0.0–5.0) |
| `seller` | TEXT DEFAULT '' | Seller username or shop name |
| `image_url` | TEXT DEFAULT '' | Primary product image URL |
| `product_url` | TEXT UNIQUE NOT NULL | Canonical Shopee product URL |
| `keyword` | TEXT DEFAULT '' | Search keyword that found this product |
| `market` | TEXT DEFAULT 'PH' | Target Shopee market: PH / SG / MY |
| `win_score` | REAL DEFAULT 0.0 | Latest computed winner score (0–100) |
| `created_at` | TEXT NOT NULL | ISO8601 UTC timestamp of first discovery |
| `updated_at` | TEXT NOT NULL | ISO8601 UTC timestamp of last scrape update |

**Indexes:** `(keyword)`, `(market)`, `(sales_count DESC)`, `(win_score DESC)`, `(product_key)`

---

### Table: `product_snapshots`

Time-series sales and review snapshots for velocity-based trend detection.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `product_url` | TEXT NOT NULL | FK → products.product_url |
| `sales_count` | INTEGER NOT NULL | Sales count at this moment |
| `review_count` | INTEGER DEFAULT 0 | Review count at this moment |
| `price` | REAL NOT NULL | Price at this moment |
| `recorded_at` | TEXT NOT NULL | ISO8601 UTC timestamp |

**Indexes:** `(product_url, recorded_at DESC)`

---

### Table: `trends`

Computed trend scores per product per run.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `product_url` | TEXT NOT NULL | FK → products.product_url |
| `sales_velocity` | REAL DEFAULT 0 | Units sold per day (Δsales/Δdays) |
| `review_growth_rate` | REAL DEFAULT 0 | % review increase in velocity window |
| `price_stability` | REAL DEFAULT 1 | 1.0 = stable, 0.0 = highly volatile |
| `direction` | TEXT DEFAULT 'stable' | rising / stable / falling |
| `trend_score` | REAL DEFAULT 0 | Composite 0–100 score |
| `computed_at` | TEXT NOT NULL | ISO8601 UTC timestamp |

**Indexes:** `(product_url)`, `(computed_at DESC)`, `(trend_score DESC)`

---

### Table: `sources`

Products found on Japanese e-commerce platforms.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `product_key` | TEXT | Normalised identity hash (matches products.product_key when same item) |
| `title` | TEXT NOT NULL | Japanese platform product title |
| `price_jpy` | REAL NOT NULL | Current price in Japanese Yen |
| `stock_status` | TEXT DEFAULT 'unknown' | in_stock / out_of_stock / limited / unknown |
| `image_url` | TEXT DEFAULT '' | Product image URL |
| `product_url` | TEXT UNIQUE NOT NULL | Canonical product URL on source platform |
| `source` | TEXT NOT NULL | amazon_jp / rakuten / yahoo_shopping / mercari |
| `seller` | TEXT DEFAULT '' | Seller name on source platform |
| `condition` | TEXT DEFAULT 'new' | new / used / like_new |
| `created_at` | TEXT NOT NULL | ISO8601 UTC timestamp |
| `updated_at` | TEXT NOT NULL | ISO8601 UTC timestamp of last re-scrape |

**Indexes:** `(source)`, `(stock_status)`, `(product_key)`, `(price_jpy ASC)`

---

### Table: `matches`

Cross-platform product identity links between Shopee and Japan.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `shopee_product_id` | INTEGER NOT NULL | FK → products.id |
| `japan_product_id` | INTEGER NOT NULL | FK → sources.id |
| `similarity` | REAL NOT NULL | RapidFuzz score (0–100) |
| `match_method` | TEXT DEFAULT 'title' | title_fuzzy / brand_model / barcode |
| `confidence_level` | TEXT DEFAULT 'medium' | high / medium / low (derived from score + method) |
| `created_at` | TEXT NOT NULL | ISO8601 UTC timestamp |
| UNIQUE | `(shopee_product_id, japan_product_id)` | Prevent duplicate pairs |

**Indexes:** `(shopee_product_id)`, `(japan_product_id)`, `(similarity DESC)`

---

### Table: `profit_analysis`

Full profit breakdown per match, with audit trail.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `match_id` | INTEGER NOT NULL | FK → matches.id |
| `shopee_price` | REAL NOT NULL | Shopee selling price at calculation time |
| `japan_price_jpy` | REAL NOT NULL | Japan purchase price at calculation time |
| `shipping_jpy` | REAL NOT NULL | Domestic Japan shipping used |
| `fee_rate` | REAL NOT NULL | Shopee fee rate applied (e.g. 0.17) |
| `exchange_rate` | REAL NOT NULL | PHP/JPY rate used at calculation time |
| `net_revenue_php` | REAL NOT NULL | Shopee price after fee deduction |
| `net_revenue_jpy` | REAL NOT NULL | PHP revenue converted to JPY |
| `profit_jpy` | REAL NOT NULL | Final net profit in JPY |
| `roi_percent` | REAL NOT NULL | Return on investment percentage |
| `is_profitable` | INTEGER NOT NULL | 1 = passes both thresholds, 0 = does not |
| `calculated_at` | TEXT NOT NULL | ISO8601 UTC timestamp |

**Indexes:** `(match_id)`, `(profit_jpy DESC)`, `(is_profitable)`, `(calculated_at DESC)`

---

### Table: `listings`

Shopee listings built and managed by this system.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `profit_analysis_id` | INTEGER | FK → profit_analysis.id (nullable for manual listings) |
| `title` | TEXT NOT NULL | Final Shopee listing title (≤120 chars) |
| `description` | TEXT DEFAULT '' | Full listing description (≤3000 chars) |
| `price` | REAL NOT NULL | Current listing price |
| `stock` | INTEGER DEFAULT 10 | Current Shopee stock quantity |
| `images` | TEXT DEFAULT '[]' | JSON array of image URLs |
| `category_id` | INTEGER DEFAULT 0 | Shopee category ID |
| `brand` | TEXT DEFAULT '' | Brand name for Shopee brand field |
| `keywords` | TEXT DEFAULT '[]' | JSON array of SEO keywords |
| `status` | TEXT DEFAULT 'draft' | draft / active / paused / sold_out / deleted |
| `source_japan_url` | TEXT DEFAULT '' | Source Japan product URL for monitoring |
| `market` | TEXT DEFAULT 'PH' | Target market: PH / SG / MY |
| `shopee_item_id` | INTEGER | Shopee's item ID after successful posting |
| `payload_json` | TEXT DEFAULT '{}' | Full Shopee API payload (for re-posting) |
| `created_at` | TEXT NOT NULL | ISO8601 UTC timestamp |
| `updated_at` | TEXT NOT NULL | ISO8601 UTC timestamp |

**Indexes:** `(status)`, `(market)`, `(shopee_item_id)`, `(source_japan_url)`

---

### Table: `orders`

Order fulfilment tracking synced from Shopee.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `listing_id` | INTEGER NOT NULL | FK → listings.id |
| `shopee_order_id` | TEXT | Shopee's order reference number |
| `order_status` | TEXT DEFAULT 'pending' | pending / paid / shipped / completed / cancelled |
| `order_amount` | REAL DEFAULT 0 | Order value in local currency |
| `buyer_name` | TEXT DEFAULT '' | Buyer username (for records) |
| `created_at` | TEXT NOT NULL | ISO8601 UTC timestamp of order creation |
| `updated_at` | TEXT NOT NULL | ISO8601 UTC timestamp of last status change |

**Indexes:** `(listing_id)`, `(order_status)`, `(created_at DESC)`

---

### Table: `price_history`

Time-series price snapshots for Japan source products.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `japan_url` | TEXT NOT NULL | FK → sources.product_url |
| `price_jpy` | REAL NOT NULL | Price in JPY at this moment |
| `recorded_at` | TEXT NOT NULL | ISO8601 UTC timestamp |

**Indexes:** `(japan_url, recorded_at DESC)`

---

### Table: `competitor_prices`

Shopee competitor price snapshots for the price optimiser.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `keyword` | TEXT NOT NULL | Search keyword used to find this competitor |
| `market` | TEXT DEFAULT 'PH' | Shopee market: PH / SG / MY |
| `comp_title` | TEXT NOT NULL | Competitor listing title |
| `comp_price` | REAL NOT NULL | Competitor listing price |
| `comp_url` | TEXT NOT NULL | Competitor listing URL |
| `scraped_at` | TEXT NOT NULL | ISO8601 UTC timestamp |

**Indexes:** `(keyword, market)`, `(scraped_at DESC)`, `(comp_price ASC)`

---

### Table: `price_optimizations`

Audit log of all price optimisation decisions.

| Column | Type | Description |
|---|---|---|
| `id` | INTEGER PK | Auto-increment |
| `listing_id` | INTEGER NOT NULL | FK → listings.id |
| `old_price` | REAL NOT NULL | Price before optimisation |
| `suggested_price` | REAL NOT NULL | Computed optimised price |
| `competitor_price` | REAL NOT NULL | Competitor price used as reference |
| `reason` | TEXT DEFAULT '' | Human-readable explanation |
| `applied` | INTEGER DEFAULT 0 | 1 = price was actually changed, 0 = dry-run |
| `optimized_at` | TEXT NOT NULL | ISO8601 UTC timestamp |

---

## 6. UNIVERSAL PRODUCT IDENTIFICATION STRATEGY

### The Problem

The same physical product appears under different titles across platforms:

| Platform | Title |
|---|---|
| Shopee | "Pokemon Card Booster Box S&V Obsidian Flames 3-Pack" |
| Amazon JP | "ポケモンカードゲーム スカーレット＆バイオレット 黒炎の支配者 3パックBOX" |
| Rakuten | "【新品】ポケモンカード 黒炎の支配者 ボックス SV3" |
| Mercari | "ポケモンカード SV3 黒炎 3BOX まとめ売り" |

These all refer to the same product (SV3 Obsidian Flames). A URL-only identity scheme cannot link them.

### The `product_key` Strategy

Every product — both Shopee and Japan — should have a computed `product_key` TEXT field that is a **normalised identity fingerprint**. This key enables cross-platform deduplication without requiring a fuzzy match.

**Generation algorithm:**

```
Step 1 — Extract structured attributes
  brand        = detect_brand(title)          → "pokemon"
  series       = detect_series(title)         → "scarlet violet" / "sv"
  edition_code = detect_edition_code(title)   → "sv3" / "obsidian flames"
  product_type = detect_type(title)           → "booster box" / "card pack"
  barcode      = extract_ean13(title)         → "4521329382769" (if present)

Step 2 — Build normalised string
  If barcode found:
    product_key = "barcode:{barcode}"         → deterministic, most reliable
  Else:
    components = [brand, product_type, edition_code]
    components = [c for c in components if c]
    normalised = sorted(components)           → order-independent
    product_key = sha256("|".join(normalised)).hexdigest()[:16]

Step 3 — Store
  products.product_key = product_key
  sources.product_key  = product_key
```

**Attribute extraction rules:**

| Attribute | Detection method |
|---|---|
| Brand | Known brand dictionary lookup (case-insensitive): `pokemon`, `one piece`, `bandai`, `good smile`, `nendoroid`, `funko`, `aniplex`, `kotobukiya`, `dragon ball` |
| Edition code | Regex: `sv\d+`, `op\d+`, `eb\d+`, `\bop-\d+\b`, Japanese edition codes (黒炎, 蒼空, etc.) via a lookup table |
| Product type | Keyword normalisation: `booster box` / `ブースターパック` → `booster_box`; `starter deck` → `starter_deck`; `figure` → `figure` |
| Barcode (EAN-13) | Regex `\d{13}` against title + description |
| Model number | Regex `[A-Z]{2,4}-\d{3,}` (e.g. `BAS-001`, `OP-01`) |

**Confidence levels for `product_key` matches:**

| Key type | Confidence |
|---|---|
| Barcode match | Very High (99%) |
| Brand + model number | High (90%) |
| Brand + edition code + type | Medium-High (80%) |
| Brand + type only | Medium (65%) |
| Title fuzzy only | Low (threshold-dependent) |

**Japanese text normalisation:**

Before extraction, run Japanese text through:
1. Convert full-width characters to half-width (`ＳＶ３` → `SV3`)
2. Convert hiragana/katakana brand markers to English equivalents using a lookup table
3. Strip `【...】` and `「...」` brackets
4. Collapse all whitespace

---

## 7. PRODUCT MATCHING STRATEGY

### Current Implementation

The matcher uses three strategies executed in sequence:

1. **Title fuzzy matching** via `process.extract` (RapidFuzz) with `token_set_ratio` as primary scorer
2. **Brand + model number matching** — both must match exactly
3. **Deduplication** by `(shopee_url, japan_url)` pair key

### Final Recommended Matching Pipeline

The matcher should execute four strategies in priority order, stopping at the first confident match for each pair:

```
Strategy 1 — Product Key Match (highest confidence)
  If products.product_key == sources.product_key
  → score = 100, method = "product_key"
  → Skip remaining strategies for this pair

Strategy 2 — Barcode Match (very high confidence)
  If EAN-13 extracted from both titles matches
  → score = 100, method = "barcode"

Strategy 3 — Brand + Model Number Match (high confidence)
  brand_shopee == brand_japan (case-insensitive)
  AND model_shopee == model_japan (case-insensitive)
  → score = 95, method = "brand_model"

Strategy 4 — Multi-metric Title Fuzzy (medium confidence)
  combined_score = max(
      token_set_ratio(norm_a, norm_b),
      token_sort_ratio(norm_a, norm_b),
      partial_ratio(norm_a, norm_b)
  )
  If combined_score >= threshold (default 70)
  → score = combined_score, method = "title_fuzzy"
```

### Threshold Logic and Confidence Model

| Score Range | Method | Confidence Level | Auto-Accept? |
|---|---|---|---|
| 100 | product_key / barcode | Very High | Yes |
| 90–99 | brand_model | High | Yes |
| 80–89 | title_fuzzy | Medium-High | Yes |
| 70–79 | title_fuzzy | Medium | Yes (with profit filter) |
| < 70 | any | Low | Reject |

**Additional quality filters applied after matching:**

- If `condition="used"` (Mercari) and match method is `title_fuzzy` with score < 80: raise threshold to 85 (used items have less standardised titles)
- If Japan product is from Mercari and the Shopee product shows "new/sealed" keywords: reduce confidence one level (condition mismatch risk)
- If `match_method == "title_fuzzy"` and `profit_jpy > 10,000`: flag for human review before posting (high-value, fuzzy match = higher risk)

### Deduplication Rule

Multiple Japan products (from different sources) may match the same Shopee product. Deduplication policy:

1. Group matches by `shopee_product_id`
2. Within each group, keep only the **lowest-price Japan product per unique `product_key`**
3. If no `product_key` exists, keep the match with the **highest similarity score**

---

## 8. PROFIT CALCULATION MODEL

### Standard Formula

```
Input:
  shopee_price    : float  — Shopee listing price in PHP (or SGD/MYR)
  japan_price_jpy : float  — Japan purchase price in JPY
  exchange_rate   : float  — PHP/JPY live rate (fallback: 2.50 for PH, 0.012 for SG, 0.034 for MY)
  shopee_fee_rate : float  — Platform fee (default: 0.17)
  shipping_jpy    : float  — Japan domestic shipping (default: 300 JPY)

Calculation:
  net_revenue_php  = shopee_price × (1 − shopee_fee_rate)
                   = shopee_price × 0.83
  net_revenue_jpy  = net_revenue_php × exchange_rate
  total_cost_jpy   = japan_price_jpy + shipping_jpy
  profit_jpy       = net_revenue_jpy − total_cost_jpy
  roi_percent      = (profit_jpy / total_cost_jpy) × 100

Acceptance criteria:
  profit_jpy  ≥ 2,000   (minimum absolute profit)
  roi_percent ≥ 30.0    (minimum return on investment)
```

### Recommended Thresholds

| Parameter | Conservative | Standard | Aggressive |
|---|---|---|---|
| Min profit (JPY) | 3,000 | 2,000 | 1,000 |
| Min ROI | 40% | 30% | 20% |
| Max Japan price (JPY) | 15,000 | 30,000 | unlimited |
| Shopee fee rate | 0.17 | 0.17 | 0.17 |
| Domestic shipping | 500 | 300 | 300 |

**Recommended operating mode:** Standard (¥2,000 profit floor + 30% ROI).

### Currency-Specific Exchange Rate Fallbacks

| Market | Currency | Fallback rate | Notes |
|---|---|---|---|
| Philippines (PH) | PHP | 2.50 JPY/PHP | Update weekly |
| Singapore (SG) | SGD | 110.0 JPY/SGD | Update weekly |
| Malaysia (MY) | MYR | 33.0 JPY/MYR | Update weekly |

### Safety Margins

The standard formula does **not** include:
- International freight from Japan to PH/SG/MY (handled by the seller outside this system)
- Packaging cost
- Platform listing fees (one-time, usually free)

If the operator pays international freight, add estimated cost to `total_cost_jpy` before computing profit. Recommended: add ¥500–¥1,500 per item depending on weight.

### Price Optimiser Floor Formula

```
floor_price_local = ((japan_price_jpy + shipping_jpy) / exchange_rate) / (1 − fee_rate)
                    + MIN_MARGIN_LOCAL

MIN_MARGIN_LOCAL defaults: PHP 200 / SGD 6 / MYR 20

The price optimiser must NEVER set a listing price below floor_price_local.
```

---

## 9. AUTOMATION PIPELINE

### Job Schedule (Final)

| Job | Trigger | Frequency | Purpose |
|---|---|---|---|
| Full Pipeline | `MARKET_ANALYSIS_TIME` (02:00) | Once daily | Market scrape → winners → Japan source → match → profit → AI listing → save candidates |
| Inventory Monitor | `INVENTORY_CHECK_TIMES` (08:00, 14:00, 20:00) | 3× daily | Re-scrape Japan sources for stock status; auto-pause out-of-stock Shopee listings |
| Price Monitor | Every 6 hours | 4× daily | Record Japan price snapshots; fire alerts on ≥5% change |
| Price Optimizer | `PRICE_OPTIMIZE_TIMES` (09:00, 18:00) | 2× daily | Scrape Shopee competitors; compute undercut prices; apply if `apply=True` |

### Job Dependency Rules

- **Full Pipeline** must not run concurrently with itself. Use a file lock (`data/pipeline.lock`) to prevent overlap.
- **Inventory Monitor** and **Price Monitor** are independent and can overlap.
- **Price Optimizer** should run only after the Price Monitor completes (to ensure it uses fresh competitor data).
- All jobs must catch and log all exceptions without crashing the scheduler process.

### Scheduler Execution Architecture

The current `schedule` library is appropriate for single-process, single-market operation. The `JobScheduler.start()` method enters a blocking loop with `time.sleep(60)` polling — this is correct behaviour.

**Important:** The scheduler runs all jobs **synchronously in the same process**. Long-running jobs (Full Pipeline: ~30–60 minutes) block the scheduler thread. For future scaling, consider:

1. Run the pipeline as a subprocess (`subprocess.run(["python", "main.py", "run"])`)
2. Or move to `APScheduler` with a thread pool
3. Or move to `Celery` with a Redis broker for multi-market operation

### Failure Recovery

Each job in `JobScheduler` is wrapped in `try/except Exception` that logs and continues. Failed jobs are not automatically retried at the job level. Recovery happens at the function level via the `@retry` decorator on individual scraper and API calls.

If the full pipeline fails mid-run, the next scheduled run starts fresh from Step 1. This is safe because all writes use `INSERT OR REPLACE` (upsert) semantics — no data is duplicated.

---

## 10. FINAL PROJECT STRUCTURE

```
shopee-arbitrage-system/
│
├── main.py                         ← Pipeline orchestrator + CLI entry point
├── requirements.txt
├── .env.example
├── ARCHITECTURE_SPEC.md            ← This document
│
├── src/
│   │
│   ├── config/
│   │   └── settings.py             ← Pydantic BaseSettings singleton
│   │
│   ├── utils/
│   │   ├── logger.py               ← Loguru setup + exported logger instance
│   │   └── retry.py                ← @retry and @retry_on_network_error decorators
│   │
│   ├── database/
│   │   ├── models.py               ← All Pydantic data models (source of truth)
│   │   └── database.py             ← SQLite CRUD layer (thread-safe, WAL)
│   │
│   ├── market_analyzer/
│   │   ├── shopee_market_scraper.py ← Playwright async scraper (XHR intercept)
│   │   └── trend_detector.py        ← Sales velocity + price stability scoring
│   │
│   ├── product_finder/
│   │   ├── winning_product_finder.py ← Hard-filter + win_score composite scoring
│   │   └── related_product_engine.py ← Keyword expansion + bilingual query gen
│   │
│   ├── japan_source/
│   │   ├── amazon_scraper.py        ← amazon.co.jp (requests + BS4)
│   │   ├── rakuten_scraper.py       ← rakuten.co.jp (requests + BS4)
│   │   ├── yahoo_scraper.py         ← shopping.yahoo.co.jp (requests + BS4)
│   │   └── mercari_scraper.py       ← mercari.com/jp (Playwright required)
│   │                                   + JapanSourceSearcher aggregator
│   ├── matching/
│   │   └── product_matcher.py       ← 4-strategy matcher (product_key > barcode >
│   │                                   brand_model > title_fuzzy)
│   ├── profit/
│   │   └── profit_engine.py         ← PHP→JPY formula + ROI filter
│   │
│   ├── ai/
│   │   ├── title_generator.py       ← OpenAI: 5 listing titles (≤120 chars)
│   │   ├── description_generator.py ← OpenAI: description + bullets
│   │   └── keyword_generator.py     ← OpenAI: SEO keywords + hashtags
│   │
│   ├── listing/
│   │   ├── listing_builder.py       ← Assembles ShopeeListing from ProfitResult
│   │   └── listing_manager.py       ← Shopee Partner API v2 (HMAC auth)
│   │
│   ├── monitoring/
│   │   ├── inventory_monitor.py     ← Stock re-scrape + auto-pause
│   │   └── price_monitor.py         ← Japan price alerts + history
│   │
│   ├── optimizer/
│   │   └── price_optimizer.py       ← Competitor scrape + undercut pricing
│   │
│   └── scheduler/
│       └── job_scheduler.py         ← schedule lib wrapper + lazy imports
│
├── data/
│   └── arbitrage.db                 ← SQLite database file
│
└── logs/
    ├── system_YYYY-MM-DD.log
    └── errors_YYYY-MM-DD.log
```

### Module Responsibility Summary

| Module | Responsibility | External dependencies |
|---|---|---|
| `config` | All settings, thresholds, credentials | pydantic, python-dotenv |
| `utils` | Logging and retry infrastructure | loguru |
| `database` | Persistence only — no business logic | sqlite3 (stdlib) |
| `market_analyzer` | Shopee data collection + trend scoring | playwright |
| `product_finder` | Winner detection + query expansion | rapidfuzz |
| `japan_source` | Japan platform data collection | requests, beautifulsoup4 |
| `matching` | Cross-platform identity resolution | rapidfuzz |
| `profit` | Financial calculation only — no I/O | requests (FX rate) |
| `ai` | OpenAI content generation — no DB writes | openai |
| `listing` | Listing assembly + Shopee API | requests |
| `monitoring` | Ongoing operational health checks | requests, beautifulsoup4 |
| `optimizer` | Competitive price analysis | requests, beautifulsoup4, playwright |
| `scheduler` | Job coordination — no business logic | schedule |

---

## 11. SCALABILITY CONSIDERATIONS

### Dimension 1 — Increasing Product Volume

**Current capacity:** ~500 Shopee products per day, ~5 Japan results per product. SQLite handles this comfortably.

**Scale threshold:** When the `products` table exceeds ~50,000 rows OR when the matching step takes more than 10 minutes per run, the following changes are needed:

- **Vectorise the matcher:** Pre-compute TF-IDF vectors for all Japan product titles and use cosine similarity for candidate retrieval before running RapidFuzz on the top-K shortlist. Libraries: `scikit-learn` `TfidfVectorizer` + `sklearn.metrics.pairwise`.
- **Add a `product_key` index** to skip matching entirely for products already matched in previous runs.
- **Paginate the pipeline:** Instead of loading all products into memory at once, process in batches of 100.
- **Migrate to PostgreSQL** at >100,000 rows for better concurrent read performance, full-text search (`tsvector`), and proper JSONB storage for the `breakdown` dict.

### Dimension 2 — Multiple Shopee Markets (PH + SG + MY)

**Current state:** `SHOPEE_MARKET` is a single-value setting. Running for two markets requires changing the setting and re-running manually.

**Required changes for multi-market:**

1. Add `market` column to `products`, `listings`, `competitor_prices` tables (already specified in Section 5).
2. Parameterise all pipeline steps with a `market` argument.
3. Each market has a separate Shopee shop ID and Partner credentials — add `SHOPEE_SHOP_ID_SG`, `SHOPEE_PARTNER_ID_SG` etc. to settings.
4. Currency-aware profit calculation: use market-specific FX rates (PHP, SGD, MYR → JPY).
5. Separate `SEARCH_KEYWORDS` per market (Malaysian buyers may search differently than Filipino buyers).
6. Run the scheduler with a `market` parameter: `python main.py run --market SG`.

At 3+ markets, consider running each market pipeline as a separate process to prevent the single Lock from serialising cross-market writes.

### Dimension 3 — Multiple Japan Suppliers

**Current state:** 4 scrapers (Amazon JP, Rakuten, Yahoo, Mercari) are hardcoded.

**Extension pattern for new sources:**

1. Create `src/japan_source/new_platform_scraper.py` following the same interface: `class NewPlatformScraper` with `def search(query: str, limit: int) -> List[JapanProduct]`.
2. Add `NEW_PLATFORM = "new_platform"` to the `JapanSource` enum in `models.py`.
3. Import and instantiate the new scraper in `JapanSourceSearcher.__init__()`.
4. The database, matching, and profit layers require **no changes**.

Suitable future sources: `Surugaya`, `Mandarake`, `Hobby Search (Amiami)`, `Yahoo Auction Japan`.

### Dimension 4 — Automation of Posting (Current: Manual Draft Review)

The system currently saves listings in `status=draft` and requires a human to post them to Shopee. To automate posting:

1. Add a post-approval step: a configurable `AUTO_POST_LISTINGS=false` setting.
2. Add a `MIN_CONFIDENCE_TO_AUTO_POST` threshold: only auto-post matches with confidence = High or Very High (score ≥ 90).
3. Rate-limit auto-posting: no more than `MAX_POSTS_PER_DAY=10` new listings per day to avoid triggering Shopee's anti-spam systems.
4. Add a `listings_queue` table to track the posting queue and prevent duplicate posts.

### Dimension 5 — Concurrency and Async Refactoring

The current architecture mixes sync and async: the Shopee scraper is `async` (Playwright), while all Japan scrapers are synchronous (`requests`). The pipeline orchestrator bridges them with `asyncio.run()`.

For higher throughput:

1. Convert Japan scrapers to `httpx.AsyncClient` (async-native HTTP)
2. Run all 4 Japan platform searches concurrently with `asyncio.gather()`
3. This reduces Japan sourcing time from ~40 seconds (sequential) to ~12 seconds (parallel at 4 concurrent) for 10 winner products

This is a non-breaking change — the public interface (`search()` returning `List[JapanProduct]`) can be preserved while making the implementation async internally.

---

*This document is the official architecture specification. All development decisions should reference it. Amendments must increment the version number and document the change rationale.*
