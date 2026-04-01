"""
Pydantic data models — single source of truth for all data shapes.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator


# ── Enumerations ──────────────────────────────────────────────────────────────

class StockStatus(str, Enum):
    IN_STOCK = "in_stock"
    OUT_OF_STOCK = "out_of_stock"
    LIMITED = "limited"
    UNKNOWN = "unknown"


class JapanSource(str, Enum):
    AMAZON_JP = "amazon_jp"
    RAKUTEN = "rakuten"
    YAHOO_SHOPPING = "yahoo_shopping"
    MERCARI = "mercari"


class ListingStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    SOLD_OUT = "sold_out"
    DELETED = "deleted"


class TrendDirection(str, Enum):
    RISING = "rising"
    STABLE = "stable"
    FALLING = "falling"


class MatchConfidence(str, Enum):
    """Confidence tier for a product match.

    Maps to the product_key confidence levels:
      EXACT         → product_key exact match (barcode or full hash)
      BRAND_MODEL   → brand + model_code exact match
      HIGH_FUZZY    → title fuzzy score >= 90
      MEDIUM_FUZZY  → title fuzzy score >= 70
      LOW_FUZZY     → title fuzzy score >= threshold (below 70)
    """
    EXACT = "exact"
    BRAND_MODEL = "brand_model"
    HIGH_FUZZY = "high_fuzzy"
    MEDIUM_FUZZY = "medium_fuzzy"
    LOW_FUZZY = "low_fuzzy"


# ── Shopee / Market Models ────────────────────────────────────────────────────

class ShopeeProduct(BaseModel):
    """A product scraped from Shopee search results."""

    title: str
    price: float = Field(..., ge=0)
    sales_count: int = Field(default=0, ge=0)
    rating: float = Field(default=0.0, ge=0.0, le=5.0)
    review_count: int = Field(default=0, ge=0, description="Number of reviews")
    seller: str = Field(default="")
    product_url: str
    image_url: str = Field(default="")
    keyword: str = Field(default="")
    market: str = Field(default="PH", description="Market code: PH | SG | MY")
    # Universal product identification
    product_key: Optional[str] = Field(
        default=None,
        description="Normalised cross-platform product identity key. "
                    "Format: 'pk:<sha256[:16]>' or 'barcode:<EAN13>'.",
    )
    product_key_confidence: str = Field(
        default="none",
        description="Key generation confidence: barcode | high | medium_high | "
                    "medium | low | none",
    )
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @validator("price", pre=True)
    def _parse_price(cls, v: object) -> float:  # noqa: N805
        if isinstance(v, str):
            v = v.replace("₱", "").replace("S$", "").replace("RM", "").replace(",", "").strip()
        return float(v)


class TrendData(BaseModel):
    """Trend information computed for a Shopee product."""

    product_url: str
    sales_velocity: float = Field(default=0.0, description="Sales per day")
    review_growth_rate: float = Field(default=0.0, description="% review increase")
    price_stability: float = Field(default=0.0, description="1.0 = perfectly stable")
    trend_direction: TrendDirection = TrendDirection.STABLE
    trend_score: float = Field(default=0.0, description="Composite 0-100 score")
    computed_at: datetime = Field(default_factory=datetime.utcnow)


class WinningProduct(BaseModel):
    """A ShopeeProduct that has passed the winning product criteria."""

    product: ShopeeProduct
    trend: Optional[TrendData] = None
    win_score: float = Field(default=0.0, description="Composite winner score 0-100")
    reasons: List[str] = Field(default_factory=list, description="Why it qualified")


# ── Japan Source Models ───────────────────────────────────────────────────────

class JapanProduct(BaseModel):
    """A product found on a Japanese e-commerce platform."""

    title: str
    price_jpy: float = Field(..., ge=0)
    stock_status: StockStatus = StockStatus.UNKNOWN
    image_url: str = Field(default="")
    product_url: str
    source: JapanSource
    seller: str = Field(default="")
    condition: str = Field(default="new", description="new | used | like_new")
    # Universal product identification
    product_key: Optional[str] = Field(
        default=None,
        description="Normalised cross-platform product identity key. "
                    "Format: 'pk:<sha256[:16]>' or 'barcode:<EAN13>'.",
    )
    product_key_confidence: str = Field(
        default="none",
        description="Key generation confidence: barcode | high | medium_high | "
                    "medium | low | none",
    )
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        use_enum_values = True


# ── Matching ──────────────────────────────────────────────────────────────────

class MatchResult(BaseModel):
    """A validated product match between Shopee and Japan platforms."""

    shopee_product: ShopeeProduct
    japan_product: JapanProduct
    similarity_score: float = Field(..., ge=0.0, le=100.0)
    match_method: str = Field(
        default="title_fuzzy",
        description="product_key | barcode | brand_model | title_fuzzy",
    )
    confidence_level: MatchConfidence = Field(
        default=MatchConfidence.MEDIUM_FUZZY,
        description="Reliability tier for this match.",
    )


# ── Profit ────────────────────────────────────────────────────────────────────

class ProfitResult(BaseModel):
    """Full profit/ROI analysis for a matched pair."""

    shopee_product: ShopeeProduct
    japan_product: JapanProduct
    similarity_score: float
    match_method: str = Field(default="title_fuzzy")
    confidence_level: MatchConfidence = Field(default=MatchConfidence.MEDIUM_FUZZY)
    profit_jpy: float
    roi_percent: float
    is_profitable: bool
    breakdown: Dict[str, Any] = Field(default_factory=dict)


# ── AI / Content ──────────────────────────────────────────────────────────────

class GeneratedTitles(BaseModel):
    original_title: str
    titles: List[str] = Field(..., min_items=1, max_items=5)
    best_title: str = ""

    def __init__(self, **data: object) -> None:
        super().__init__(**data)
        if self.titles and not self.best_title:
            self.best_title = self.titles[0]


class GeneratedDescription(BaseModel):
    product_title: str
    description: str
    bullet_points: List[str] = Field(default_factory=list)


class GeneratedKeywords(BaseModel):
    product_title: str
    keywords: List[str] = Field(default_factory=list, description="SEO keywords list")
    hashtags: List[str] = Field(default_factory=list, description="Shopee hashtag list")
    search_tags: List[str] = Field(default_factory=list, description="Search-optimised tags")


# ── Listing ───────────────────────────────────────────────────────────────────

class ShopeeListing(BaseModel):
    """Complete, API-ready Shopee product listing."""

    title: str = Field(..., max_length=120)
    description: str = Field(..., max_length=3000)
    price: float = Field(..., ge=0)
    stock: int = Field(default=10, ge=0)
    images: List[str] = Field(default_factory=list)
    category_id: int = Field(default=0)
    brand: str = Field(default="")
    condition: str = Field(default="NEW")
    weight: float = Field(default=0.5)
    currency: str = Field(default="PHP")
    keywords: List[str] = Field(default_factory=list)
    status: ListingStatus = ListingStatus.DRAFT
    source_japan_url: str = Field(default="")
    profit_jpy: float = Field(default=0.0)
    roi_percent: float = Field(default=0.0)
    shopee_item_id: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    def to_api_payload(self) -> dict:
        """Serialize to Shopee Add Item API payload."""
        return {
            "name": self.title,
            "description": self.description,
            "original_price": self.price,
            "stock": self.stock,
            "image": {"image_url_list": self.images},
            "category_id": self.category_id,
            "condition": self.condition,
            "weight": self.weight,
            "currency": self.currency,
            "brand": {"brand_name": self.brand} if self.brand else {},
        }


# ── Profit Analysis ───────────────────────────────────────────────────────────

class ProfitAnalysis(BaseModel):
    """Persisted, auditable profit calculation result for one matched pair.

    Stored in ``profit_analysis`` and linked by FK to both ``products`` and
    ``sources`` tables.  One row per (shopee_product_id, japan_product_id)
    pair — overwritten on re-calculation so the table always contains the
    latest analysis for each pair.

    Currency conventions
    --------------------
    * ``shopee_price``  — local currency of the target market (PHP / SGD / MYR)
    * ``shopee_fee``    — same local currency (shopee_price × fee_rate)
    * All other monetary fields — JPY

    ROI convention
    --------------
    ``roi`` is stored as a **decimal** (0–1 scale) so that 30 % ROI is
    represented as ``0.30``.  This matches the user-facing threshold
    ``minimum_roi = 0.30`` exactly.
    """

    shopee_product_id: int = Field(..., description="FK → products.id")
    japan_product_id: int = Field(..., description="FK → sources.id")
    # ── Cost components ──────────────────────────────────────────────────────
    supplier_price: float = Field(
        ..., ge=0,
        description="Japan source price in JPY at calculation time",
    )
    domestic_shipping_cost: float = Field(
        ..., ge=0,
        description="Japan domestic shipping cost in JPY",
    )
    safety_margin: float = Field(
        default=0.0, ge=0,
        description="Additional JPY cost buffer deducted from revenue before "
                    "declaring a listing profitable",
    )
    # ── Revenue components ────────────────────────────────────────────────────
    shopee_price: float = Field(
        ..., ge=0,
        description="Shopee listing price in local currency (PHP/SGD/MYR)",
    )
    shopee_fee: float = Field(
        ..., ge=0,
        description="Platform fee in local currency = shopee_price × fee_rate",
    )
    fee_rate: float = Field(
        ..., ge=0, le=1,
        description="Shopee fee rate applied (e.g. 0.17 for 17 %)",
    )
    exchange_rate: float = Field(
        ..., gt=0,
        description="Local-currency-to-JPY rate used in this calculation",
    )
    # ── Derived results ───────────────────────────────────────────────────────
    net_revenue_jpy: float = Field(
        ...,
        description="(shopee_price − shopee_fee) converted to JPY",
    )
    cost_jpy: float = Field(
        ..., ge=0,
        description="Total cost in JPY = supplier_price + domestic_shipping_cost",
    )
    profit: float = Field(
        ...,
        description="net_revenue_jpy − cost_jpy − safety_margin  (JPY)",
    )
    roi: float = Field(
        ...,
        description="profit / cost_jpy  (decimal, e.g. 0.30 = 30 %)",
    )
    is_profitable: bool = Field(
        default=False,
        description="True when profit >= min_profit_jpy AND roi >= min_roi",
    )
    # ── Match provenance ──────────────────────────────────────────────────────
    match_method: str = Field(default="title_fuzzy")
    confidence_level: str = Field(default="medium_fuzzy")
    similarity_score: float = Field(default=0.0, ge=0.0, le=100.0)
    analyzed_at: datetime = Field(default_factory=datetime.utcnow)


# ── Snapshots ────────────────────────────────────────────────────────────────

class ProductSnapshot(BaseModel):
    """One point-in-time capture of a Japan source product's state.

    Stored in ``product_snapshots`` and read by three consumers:

    * **Price monitor** — compares ``price_jpy`` against the previous snapshot
      to detect price increases that could erode profit margin.
    * **Inventory monitor** — compares ``stock_status`` against the previous
      snapshot to detect out-of-stock and restock transitions.
    * **Profit recalculation** — uses ``price_jpy`` + ``exchange_rate`` to
      recompute ROI for all active listings linked to this Japan product.

    The optional ``competitor_price`` field records the lowest Shopee
    competitor price observed at the same moment, making it possible for the
    price optimiser to track the market baseline over time instead of
    computing it purely from live scraping on every run.
    """

    product_id: int = Field(..., description="FK → sources.id (Japan source product)")
    price_jpy: float = Field(..., ge=0, description="Japan source price at capture time")
    competitor_price: Optional[float] = Field(
        default=None,
        ge=0,
        description="Lowest Shopee competitor price in local currency (PHP/SGD/MYR) "
                    "at the same moment.  NULL when no competitor data was available.",
    )
    stock_status: StockStatus = Field(
        default=StockStatus.UNKNOWN,
        description="Japan source stock status at capture time",
    )
    sales_count: int = Field(
        default=0,
        ge=0,
        description="Cumulative Shopee sales count — used by TrendDetector to compute "
                    "velocity (Δsales / Δdays between snapshots).",
    )
    review_count: int = Field(
        default=0,
        ge=0,
        description="Review count at capture time — used to compute review_growth_rate.",
    )
    exchange_rate: Optional[float] = Field(
        default=None,
        gt=0,
        description="PHP→JPY (or SGD→JPY / MYR→JPY) rate at capture time.  "
                    "Stored alongside the snapshot so profit recalculation can "
                    "replay historical ROI without hitting the exchange rate API.",
    )
    captured_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        use_enum_values = True


class PriceDelta(BaseModel):
    """Summary of price movement for one Japan product over a time window.

    Returned by ``Database.get_price_delta()`` and consumed by the price
    monitor to decide whether to trigger a ``PriceAlert``.
    """

    product_id: int
    earliest_price_jpy: float
    latest_price_jpy: float
    delta_jpy: float = Field(description="latest − earliest; positive = price increased")
    delta_pct: float = Field(description="(delta_jpy / earliest_price_jpy) × 100")
    window_hours: int
    snapshots_in_window: int


# ── Monitoring ────────────────────────────────────────────────────────────────

class PriceAlert(BaseModel):
    japan_product_url: str
    old_price_jpy: float
    new_price_jpy: float
    change_percent: float
    alerted_at: datetime = Field(default_factory=datetime.utcnow)


class StockAlert(BaseModel):
    japan_product_url: str
    old_status: StockStatus
    new_status: StockStatus
    affected_listing_ids: List[int] = Field(default_factory=list)
    alerted_at: datetime = Field(default_factory=datetime.utcnow)


class CompetitorPrice(BaseModel):
    """A competitor's current Shopee price for a given keyword/product."""

    keyword: str
    competitor_title: str
    competitor_price: float
    competitor_url: str
    scraped_at: datetime = Field(default_factory=datetime.utcnow)


class PriceOptimizationResult(BaseModel):
    """Result of the price optimization engine for one listing."""

    listing_id: int
    current_price: float
    suggested_price: float
    competitor_price: float
    reason: str
    applied: bool = False
    optimized_at: datetime = Field(default_factory=datetime.utcnow)


# ── Research AI ───────────────────────────────────────────────────────────────

class ResearchCandidateStatus(str, Enum):
    """Lifecycle state of a research candidate.

    State transitions
    -----------------
    PENDING → MATCHED   : Japan product found during supplier search
    PENDING → REJECTED  : Re-scored below threshold on next research scan
    MATCHED → REJECTED  : All profit analyses for this pair fell below thresholds
    """
    PENDING  = "pending"   # Queued for Japan supplier search
    MATCHED  = "matched"   # At least one Japan product linked via matches table
    REJECTED = "rejected"  # Dropped — either no match or unprofitable after analysis


class ResearchCandidate(BaseModel):
    """A Shopee product identified as a promising arbitrage candidate.

    Produced by the Research AI engine and consumed by the Japan marketplace
    search pipeline.  One row per Shopee product (UNIQUE on shopee_product_id)
    — re-running the engine overwrites stale scores.

    Score sub-fields
    ----------------
    All sub-scores are on a 0–100 scale.  The composite ``research_score``
    is a weighted sum (weights defined in ``ResearchScorer.WEIGHTS``).

    * score_demand    — signals from sales_count, rating, review_count
    * score_velocity  — rate of sales/review growth from trend data or snapshots
    * score_stability — price consistency; stable prices → predictable margin
    * score_price_gap — estimated room between Shopee listing price and Japan cost
    * score_brand     — product_key confidence proxy for brand recognition
    """
    shopee_product_id: int = Field(..., description="FK → products.id")
    research_score:  float = Field(..., ge=0.0, le=100.0, description="Composite 0-100")
    score_demand:    float = Field(default=0.0, ge=0.0, le=100.0)
    score_velocity:  float = Field(default=0.0, ge=0.0, le=100.0)
    score_stability: float = Field(default=0.0, ge=0.0, le=100.0)
    score_price_gap: float = Field(default=0.0, ge=0.0, le=100.0)
    score_brand:     float = Field(default=0.0, ge=0.0, le=100.0)
    reason: str = Field(
        default="",
        description="Human-readable explanation: top scoring factors",
    )
    status: ResearchCandidateStatus = ResearchCandidateStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ── Related Product Discovery AI ─────────────────────────────────────────────

class DiscoveryMethod(str, Enum):
    """Which expansion strategy produced this related-product keyword.

    BRAND    — sibling products that share the same brand (e.g. all Bandai TCG sets)
    SERIES   — sequential set codes within one franchise (OP01 → OP02 → OP03)
    KEYWORD  — cross-product-type keywords derived from the seed's title tokens
    CATEGORY — accessory / complementary category keywords (e.g. Card → Sleeve)
    """
    BRAND    = "brand"
    SERIES   = "series"
    KEYWORD  = "keyword"
    CATEGORY = "category"


class RelatedProductCandidate(BaseModel):
    """A search keyword generated by the Related Product Discovery AI.

    Each row represents one *related_keyword* that the Japan marketplace
    search pipeline should issue in order to find potential arbitrage stock.

    Lifecycle
    ---------
    After this record is created the Japan scraper will:
    1. Search ``related_keyword`` on Amazon JP / Rakuten / etc.
    2. Store any found products in ``sources``.
    3. Pass them to the matching engine (``ProductMatcher``).
    4. If a profitable match is found, a ``ProfitAnalysis`` and
       ``ListingCandidate`` are generated as normal.

    The ``seed_product_id`` provides full traceability back to the original
    Shopee product that triggered this discovery chain.

    Confidence bands
    ----------------
    90–100  Series expansion with exact next/prev code confirmed in products DB
    70–89   Brand expansion with existing product found in products DB
    50–69   Brand/keyword expansion — keyword generated, not yet confirmed
    30–49   Category accessory expansion — lower affinity signal
    """
    seed_product_id:  int = Field(..., description="FK → products.id (the seed product)")
    related_keyword:  str = Field(
        ..., min_length=1,
        description="Search term to forward to the Japan marketplace scraper",
    )
    discovery_method: DiscoveryMethod = Field(
        ...,
        description="Strategy that produced this keyword",
    )
    confidence_score: float = Field(
        ..., ge=0.0, le=100.0,
        description="Estimated usefulness of this keyword (0–100)",
    )
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ── Competition Analyzer AI ───────────────────────────────────────────────────

class CompetitorListing(BaseModel):
    """A competing Shopee listing scraped for price-analysis purposes.

    Rows are identified by the combination of ``shopee_product_id`` (or
    ``product_key`` when available) and ``competitor_url``.  The table is
    overwritten on each scrape so it always reflects the current market.
    """

    shopee_product_id: int = Field(
        ...,
        description="FK → products.id  (the product we are pricing against)",
    )
    product_key: Optional[str] = Field(
        default=None,
        description="Shared product_key when the competitor sells the same SKU",
    )
    competitor_title: str
    competitor_price: float = Field(..., ge=0, description="PHP price")
    competitor_stock: Optional[int] = Field(
        default=None, ge=0,
        description="Visible stock count (None when not shown by Shopee)",
    )
    seller_rating: Optional[float] = Field(
        default=None, ge=0.0, le=5.0,
        description="Seller star rating (None when not shown)",
    )
    competitor_url: str = Field(default="")
    scraped_at: datetime = Field(default_factory=datetime.utcnow)


class PriceStrategy(str, Enum):
    """Which pricing strategy was used to compute the recommendation."""
    MEDIAN_MINUS_DISCOUNT   = "median_minus_discount"
    BELOW_MIN               = "below_min"
    FLOOR_ONLY              = "floor_only"       # not enough competitor data
    MANUAL                  = "manual"


class PriceRecommendation(BaseModel):
    """Recommended Shopee listing price for one product.

    Computed by ``PriceStrategyEngine`` after aggregating ``CompetitorListings``.
    The floor is derived from the profit engine's ``min_profit`` and ``min_roi``
    constraints, ensuring we never recommend a price that makes the arbitrage
    unprofitable.
    """

    shopee_product_id: int = Field(..., description="FK → products.id")
    product_key: Optional[str] = Field(default=None)
    # ── Market data ───────────────────────────────────────────────────────────
    competitor_count: int = Field(default=0, ge=0)
    min_market_price: float = Field(..., ge=0, description="Lowest competitor price (PHP)")
    median_market_price: float = Field(..., ge=0, description="Median competitor price (PHP)")
    max_market_price: float = Field(..., ge=0, description="Highest competitor price (PHP)")
    # ── Recommendation ────────────────────────────────────────────────────────
    recommended_price: float = Field(..., ge=0, description="Recommended listing price (PHP)")
    min_viable_price: float = Field(
        ..., ge=0,
        description="Price floor: minimum price that still meets profit/ROI thresholds (PHP)",
    )
    strategy_used: PriceStrategy = Field(
        default=PriceStrategy.MEDIAN_MINUS_DISCOUNT,
        description="Strategy that produced this recommendation",
    )
    strategy_note: str = Field(default="", description="Human-readable explanation")
    calculated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        use_enum_values = True
