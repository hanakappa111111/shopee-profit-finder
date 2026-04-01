"""
Centralised application configuration.
All values are loaded from environment variables / .env file.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """
    Master configuration for the Shopee Arbitrage System.
    Override any value by setting the corresponding environment variable
    or adding it to your .env file.
    """

    # ── System mode ─────────────────────────────────────────────────────────
    # When False, the scheduler's setup_jobs() becomes a no-op and no
    # continuous automation runs.  The system operates purely on-demand via
    # run_research_pipeline(keyword).  Set to True to re-enable the old
    # always-on pipeline.
    AUTOMATION_ENABLED: bool = Field(default=False, env="AUTOMATION_ENABLED")

    # ── Paths ─────────────────────────────────────────────────────────────────
    BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent
    DATA_DIR: Path = BASE_DIR / "data"
    LOG_DIR: Path = BASE_DIR / "logs"
    DB_PATH: Path = BASE_DIR / "data" / "arbitrage.db"

    # ── OpenAI ────────────────────────────────────────────────────────────────
    OPENAI_API_KEY: str = Field(default="", env="OPENAI_API_KEY")
    OPENAI_MODEL: str = Field(default="gpt-4o-mini", env="OPENAI_MODEL")

    # ── Shopee ────────────────────────────────────────────────────────────────
    SHOPEE_PARTNER_ID: str = Field(default="", env="SHOPEE_PARTNER_ID")
    SHOPEE_PARTNER_KEY: str = Field(default="", env="SHOPEE_PARTNER_KEY")
    SHOPEE_SHOP_ID: str = Field(default="", env="SHOPEE_SHOP_ID")
    SHOPEE_BASE_URL: str = "https://shopee.ph"
    SHOPEE_API_BASE: str = "https://partner.shopeemobile.com/api/v2"
    SHOPEE_MARKET: str = Field(default="PH", env="SHOPEE_MARKET")  # PH | SG | MY

    # ── Search keywords ───────────────────────────────────────────────────────
    SEARCH_KEYWORDS: List[str] = [
        "anime figure",
        "pokemon card",
        "one piece card",
        "bandai figure",
        "good smile figure",
        "nendoroid",
        "funko pop anime",
    ]
    MAX_PAGES_PER_KEYWORD: int = 5
    MAX_PRODUCTS_PER_KEYWORD: int = 100
    JAPAN_RESULTS_LIMIT: int = 5

    # ── Winning product thresholds ────────────────────────────────────────────
    MIN_SALES_COUNT: int = 200          # Minimum sold count
    MIN_RATING: float = 4.5             # Minimum star rating
    MIN_PRICE_PHP: float = 800.0        # Minimum Shopee price (PHP) ≈ ~5,000 yen
    TREND_VELOCITY_DAYS: int = 7        # Days window for trend detection

    # ── Profitability thresholds ──────────────────────────────────────────────
    SHOPEE_FEE_RATE: float = 0.17           # 17% platform fee
    DOMESTIC_SHIPPING_YEN: float = 300.0    # Japan domestic shipping (JPY)
    # Safety margin — extra JPY buffer deducted from gross profit before
    # deciding whether a product is listable.  Acts as a reserve for
    # unpredictable micro-costs (payment gateway slippage, re-packing, etc.).
    # Set to 0 to disable.
    SAFETY_MARGIN_YEN: float = 0.0
    MIN_PROFIT_YEN: float = 2_000.0         # Minimum acceptable profit (JPY)
    # MIN_ROI is stored as a decimal fraction (0–1) matching the formula
    # roi = profit / cost.  0.30 means 30 %.
    MIN_ROI: float = 0.30
    # Legacy alias kept for backward-compatibility — equals MIN_ROI * 100
    MIN_ROI_PERCENT: float = 30.0
    MIN_MATCH_SIMILARITY: float = 70.0      # RapidFuzz threshold (0–100)
    # Minimum absolute % price change that triggers a profit re-calculation
    # when called from the snapshot system.
    PROFIT_RECALC_THRESHOLD_PCT: float = 5.0

    # ── Scraping ──────────────────────────────────────────────────────────────
    REQUEST_DELAY_SECONDS: float = 2.0
    RETRY_MAX_ATTEMPTS: int = 3
    RETRY_BACKOFF_SECONDS: float = 5.0
    PLAYWRIGHT_HEADLESS: bool = True
    PLAYWRIGHT_TIMEOUT_MS: int = 30_000
    # Optional HTTP/HTTPS proxy for scrapers (e.g. "http://user:pass@host:port")
    SCRAPER_PROXY: str = Field(default="", env="SCRAPER_PROXY")

    # ── Rakuten API (free tier — 30 req/s) ───────────────────────────────────
    # Register at https://webservice.rakuten.co.jp/ to get an application ID.
    # When set, the Rakuten scraper uses the API instead of HTML scraping.
    RAKUTEN_APP_ID: str = Field(default="", env="RAKUTEN_APP_ID")

    # ── Price optimiser ───────────────────────────────────────────────────────
    PRICE_UNDERCUT_PERCENT: float = 3.0     # Undercut competitor by 3%
    MIN_MARGIN_PHP: float = 200.0           # Never go below this margin

    # ── Research AI ───────────────────────────────────────────────────────────
    # Minimum composite research score (0–100) to persist a candidate.
    # Products below this threshold are silently skipped.
    RESEARCH_MIN_SCORE: float = 50.0
    # Maximum number of new/updated candidates produced per scan run.
    # Acts as a safety cap to avoid overwhelming the Japan search pipeline.
    RESEARCH_MAX_CANDIDATES: int = 100
    # Look-back window for snapshot-derived velocity and stability signals.
    RESEARCH_SCORE_WINDOW_DAYS: int = 7
    # Minimum sales count required before a product is considered at all.
    # Products with fewer sales are likely too new to assess demand reliably.
    RESEARCH_MIN_SALES: int = 50
    # Local time to run the daily research scan (24-hour clock).
    RESEARCH_JOB_TIME: str = "01:00"

    # ── Related Product Discovery AI ──────────────────────────────────────────
    # Minimum research_score a ResearchCandidate must have to be used as a seed.
    # Higher = fewer but better-quality seeds.
    DISCOVERY_SEED_MIN_SCORE: float = 60.0
    # Minimum confidence_score for a generated keyword to be persisted.
    DISCOVERY_MIN_CONFIDENCE: float = 40.0
    # Hard cap on keywords produced per seed per run.
    DISCOVERY_MAX_KEYWORDS_PER_SEED: int = 20
    # How many sequential set codes to generate ahead AND behind the seed code.
    # e.g. seed=OP01, lookahead=3 → generates OP02, OP03, OP04.
    DISCOVERY_SERIES_LOOKAHEAD: int = 3
    # Local time for the daily discovery scan (runs after RESEARCH_JOB_TIME).
    DISCOVERY_JOB_TIME: str = "01:30"

    # ── Competition Analyzer AI ───────────────────────────────────────────────
    # Maximum number of competitor listings to scrape per product.
    COMPETITION_MAX_COMPETITORS: int = 20
    # Discount applied below median price when recommending a listing price (PHP).
    COMPETITION_MEDIAN_DISCOUNT_PHP: float = 50.0
    # Minimum number of competitor prices required to apply market strategy.
    COMPETITION_MIN_COMPETITORS: int = 3
    # How many products to analyse per job run.
    COMPETITION_MAX_PRODUCTS: int = 100
    # Scheduled time for the daily competition analysis job.
    COMPETITION_JOB_TIME: str = "02:30"
    # Max age of competitor listings (hours) before they are considered stale.
    COMPETITION_FRESHNESS_HOURS: int = 24

    # ── Supplier Search AI ────────────────────────────────────────────────────
    # Maximum search queries generated per seed product.
    SUPPLIER_MAX_QUERIES_PER_SEED: int = 8
    # Maximum results to keep per query (per marketplace).
    SUPPLIER_MAX_RESULTS_PER_QUERY: int = 5
    # Marketplaces to search (subset of: amazon_jp, rakuten, yahoo_shopping, mercari).
    SUPPLIER_MARKETPLACES: List[str] = ["amazon_jp", "rakuten", "yahoo_shopping", "mercari"]
    # Delay (seconds) between marketplace requests to avoid rate-limiting.
    SUPPLIER_REQUEST_DELAY: float = 2.0
    # Minimum product_key confidence to auto-assign during enrichment.
    SUPPLIER_MIN_KEY_CONFIDENCE: str = "low"
    # Whether to search RelatedProductCandidates in addition to ResearchCandidates.
    SUPPLIER_SEARCH_RELATED: bool = True
    # Minimum research_score for direct ResearchCandidate seeds.
    SUPPLIER_SEED_MIN_SCORE: float = 50.0
    # Minimum confidence_score for RelatedProductCandidate seeds.
    SUPPLIER_RELATED_MIN_CONFIDENCE: float = 50.0
    # Max seeds per job run.
    SUPPLIER_MAX_SEEDS: int = 100
    # Local time for the daily supplier search job (runs after discovery).
    SUPPLIER_SEARCH_JOB_TIME: str = "02:00"

    # ── Scheduler times (24-hour local time) ──────────────────────────────────
    MARKET_ANALYSIS_TIME: str = "02:30"
    INVENTORY_CHECK_TIMES: List[str] = ["08:00", "14:00", "20:00"]
    PRICE_OPTIMIZE_TIMES: List[str] = ["09:00", "18:00"]

    # ── Supplier Monitor ──────────────────────────────────────────────────────
    # How often the supplier PRICE monitor runs (hours between each cycle).
    SUPPLIER_PRICE_MONITOR_HOURS: int = 6
    # How often the supplier INVENTORY monitor runs (hours between each cycle).
    SUPPLIER_INVENTORY_MONITOR_HOURS: int = 3

    # ── Snapshot cleanup ──────────────────────────────────────────────────────
    # Snapshots older than this many days are purged by the nightly cleanup job.
    SNAPSHOT_RETENTION_DAYS: int = 90

    # ── Notifications ───────────────────────────────────────────────────────────
    # Discord webhook URL for profit alerts. Leave empty to disable.
    DISCORD_WEBHOOK_URL: str = Field(default="", env="DISCORD_WEBHOOK_URL")
    # LINE Notify token for profit alerts. Leave empty to disable.
    # Get a token at https://notify-bot.line.me/my/
    LINE_NOTIFY_TOKEN: str = Field(default="", env="LINE_NOTIFY_TOKEN")

    # ── Logging ───────────────────────────────────────────────────────────────
    LOG_LEVEL: str = "INFO"
    LOG_ROTATION: str = "1 day"
    LOG_RETENTION: str = "30 days"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


# Singleton
settings = Settings()

# Ensure dirs exist
settings.DATA_DIR.mkdir(parents=True, exist_ok=True)
settings.LOG_DIR.mkdir(parents=True, exist_ok=True)
