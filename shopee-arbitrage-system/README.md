# 🛒 Shopee Arbitrage System v2

A production-grade, service-oriented Python automation platform that identifies high-margin Japanese products and prepares them for listing on Shopee (Philippines, Singapore, Malaysia).

---

## 🎯 What It Does

The system runs a fully automated 8-step daily pipeline:

| Step | Module | Action |
|------|--------|--------|
| 1 | `market_analyzer` | Scrape Shopee for anime/TCG search results using Playwright |
| 2 | `market_analyzer` | Compute trend scores (sales velocity, price stability) |
| 3 | `product_finder` | Identify "winning" products (sales > 200, rating > 4.5, price > ₱800) |
| 4 | `japan_source` | Search Amazon JP, Rakuten, Yahoo Shopping, Mercari for each winner |
| 5 | `matching` | Match Shopee ↔ Japan products using RapidFuzz similarity (≥70%) |
| 6 | `profit` | Calculate profit & ROI per match (fee 17%, shipping ¥300, live FX rate) |
| 7 | `listing` | Generate AI-optimised titles, descriptions, and SEO keywords |
| 8 | `database` | Save profitable candidates (profit > ¥2,000 & ROI > 30%) |

Additional recurring jobs:
- **Inventory Monitor** (3× daily): detects when Japan suppliers go out-of-stock → automatically zeros Shopee stock
- **Price Monitor** (continuous): alerts on Japan price changes > 5%
- **Price Optimizer** (2× daily): undercuts competitors by 3% while protecting minimum margin

---

## 🗂 Project Structure

```
shopee-arbitrage-system/
├── src/
│   ├── market_analyzer/
│   │   ├── shopee_market_scraper.py    # Playwright Shopee scraper + API intercept
│   │   └── trend_detector.py           # Sales velocity & trend scoring
│   ├── product_finder/
│   │   ├── winning_product_finder.py   # High-margin product identification
│   │   └── related_product_engine.py  # Query expansion + Japan search terms
│   ├── japan_source/
│   │   ├── amazon_scraper.py          # Amazon Japan (new products)
│   │   ├── rakuten_scraper.py         # Rakuten Ichiba
│   │   ├── yahoo_scraper.py           # Yahoo Shopping Japan
│   │   └── mercari_scraper.py         # Mercari Japan (used) + JapanSourceSearcher
│   ├── matching/
│   │   └── product_matcher.py         # RapidFuzz multi-strategy matching
│   ├── profit/
│   │   └── profit_engine.py           # Profit / ROI with live PHP→JPY rate
│   ├── ai/
│   │   ├── title_generator.py         # OpenAI: 5 optimised listing titles
│   │   ├── description_generator.py   # OpenAI: buyer-focused descriptions
│   │   └── keyword_generator.py       # OpenAI: SEO keywords + hashtags
│   ├── listing/
│   │   ├── listing_builder.py         # Assembles complete ShopeeListing objects
│   │   └── listing_manager.py         # Shopee Partner API v2 (HMAC auth)
│   ├── monitoring/
│   │   ├── inventory_monitor.py       # Japan stock monitoring + auto-pause
│   │   └── price_monitor.py           # Japan price change detection
│   ├── optimizer/
│   │   └── price_optimizer.py         # Competitor price analysis + optimisation
│   ├── database/
│   │   ├── models.py                  # Pydantic data models
│   │   └── database.py                # SQLite CRUD (thread-safe)
│   ├── scheduler/
│   │   └── job_scheduler.py           # Central job scheduler (lazy imports)
│   ├── config/
│   │   └── settings.py                # Centralised Pydantic settings
│   └── utils/
│       ├── logger.py                  # Loguru structured + rotating logs
│       └── retry.py                   # Retry decorator (sync + async)
├── main.py                            # Pipeline orchestrator + CLI
├── requirements.txt
├── .env.example
└── README.md
```

---

## ⚙️ Setup

### Prerequisites

- Python 3.11+
- pip / venv

### Install

```bash
git clone <repo-url>
cd shopee-arbitrage-system

python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate

pip install -r requirements.txt
playwright install chromium      # Required for Shopee scraping
```

### Configure

```bash
cp .env.example .env
# Edit .env with your API keys
```

| Variable | Required | Description |
|---|---|---|
| `OPENAI_API_KEY` | For AI features | Your OpenAI key |
| `SHOPEE_PARTNER_ID` | For posting | Shopee Partner ID |
| `SHOPEE_PARTNER_KEY` | For posting | Shopee Partner Key |
| `SHOPEE_SHOP_ID` | For posting | Your Shopee Shop ID |
| `SHOPEE_MARKET` | Optional | `PH` (default) / `SG` / `MY` |

The bot is fully operational without Shopee credentials — listings are saved locally for manual review.

---

## 🚀 Usage

```bash
# Run the full 8-step pipeline once
python main.py run

# Start the daily scheduler (runs pipeline then loops forever)
python main.py schedule

# Run inventory + price monitors only
python main.py monitor

# Run price optimizer (dry-run, no changes applied)
python main.py optimize

# Run price optimizer and apply price changes
python main.py optimize-apply

# View DB statistics + top candidates
python main.py stats
```

---

## 💰 Profit Formula

```
net_revenue_php  = shopee_price_php × (1 − 0.17)       ← 17% Shopee fee
net_revenue_jpy  = net_revenue_php × PHP/JPY rate       ← live exchange rate
profit_jpy       = net_revenue_jpy − japan_price − 300  ← ¥300 domestic shipping
ROI (%)          = profit_jpy / (japan_price + 300) × 100
```

Candidates are saved when: `profit_jpy ≥ ¥2,000` AND `ROI ≥ 30%`

---

## 📊 Database Schema

| Table | Description |
|---|---|
| `products` | Scraped Shopee listings |
| `trends` | Sales velocity & trend scores per product |
| `sources` | Japanese supplier products |
| `matches` | Matched Shopee↔Japan pairs with scores |
| `listings` | Generated Shopee listing candidates |
| `orders` | Order tracking (Shopee fulfilment) |
| `price_history` | Japan product price time series |
| `competitor_prices` | Shopee competitor price snapshots |
| `price_optimizations` | Price optimisation log |

---

## 🔧 Configuration Reference

| Setting | Default | Description |
|---|---|---|
| `SEARCH_KEYWORDS` | anime figure, pokemon card... | Shopee search terms |
| `MIN_SALES_COUNT` | 200 | Min sold count for winners |
| `MIN_RATING` | 4.5 | Min star rating |
| `MIN_PRICE_PHP` | 800 | Min Shopee price (~¥5,000) |
| `MIN_PROFIT_YEN` | 2000 | Min profit threshold |
| `MIN_ROI_PERCENT` | 30.0 | Min ROI percentage |
| `MIN_MATCH_SIMILARITY` | 70.0 | RapidFuzz score threshold |
| `PRICE_UNDERCUT_PERCENT` | 3.0 | Competitor undercut % |
| `MARKET_ANALYSIS_TIME` | 02:00 | Daily pipeline time |

---

## 🧩 Extending the System

**Add a new Japan platform:**
1. Create `src/japan_source/my_platform_scraper.py` following `amazon_scraper.py`
2. Import it in `mercari_scraper.py` → `JapanSourceSearcher`

**Add a new Shopee market:**
1. Set `SHOPEE_MARKET=SG` in `.env`
2. Update `SHOPEE_BASE_URL` in settings to `shopee.sg`

**Enable automatic listing posting:**
1. Set Shopee Partner API credentials in `.env`
2. In `main.py`, add a step that calls `ListingManager().create_listing(listing)` for each saved candidate

**Add custom alert notifications:**
```python
def telegram_alert(alert: StockAlert) -> None:
    send_telegram(f"Stock out: {alert.japan_product_url}")

monitor = InventoryMonitor(alert_callback=telegram_alert)
```

---

## 📝 Logs

Logs rotate daily in `logs/`:
- `logs/system_YYYY-MM-DD.log` — full structured log
- `logs/errors_YYYY-MM-DD.log` — errors only

---

## ⚠️ Legal & Ethics

- Review each platform's Terms of Service before scraping
- Respect `robots.txt` — rate limiting is pre-configured (`REQUEST_DELAY_SECONDS=2.0`)
- Ensure your Shopee listings comply with import regulations in your market (PH/SG/MY)
- Mercari and similar second-hand platforms may have additional restrictions on bulk querying
