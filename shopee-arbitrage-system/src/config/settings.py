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
    DOMESTIC_SHIPPING_YEN: float = 300.0    # Japan domestic shipping
    MIN_PROFIT_YEN: float = 2_000.0         # Minimum acceptable profit
    MIN_ROI_PERCENT: float = 30.0           # Minimum ROI percentage
    MIN_MATCH_SIMILARITY: float = 70.0      # RapidFuzz threshold (0–100)

    # ── Scraping ──────────────────────────────────────────────────────────────
    REQUEST_DELAY_SECONDS: float = 2.0
    RETRY_MAX_ATTEMPTS: int = 3
    RETRY_BACKOFF_SECONDS: float = 5.0
    PLAYWRIGHT_HEADLESS: bool = True
    PLAYWRIGHT_TIMEOUT_MS: int = 30_000

    # ── Price optimiser ───────────────────────────────────────────────────────
    PRICE_UNDERCUT_PERCENT: float = 3.0     # Undercut competitor by 3%
    MIN_MARGIN_PHP: float = 200.0           # Never go below this margin

    # ── Scheduler times (24-hour local time) ──────────────────────────────────
    MARKET_ANALYSIS_TIME: str = "02:00"
    INVENTORY_CHECK_TIMES: List[str] = ["08:00", "14:00", "20:00"]
    PRICE_OPTIMIZE_TIMES: List[str] = ["09:00", "18:00"]

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
