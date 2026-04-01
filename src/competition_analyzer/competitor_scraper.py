"""Competition Analyzer AI — Competitor Scraper.

Collects competing Shopee listings for a given product using the existing
market scraper infrastructure.

Two scraping strategies are available and tried in order:

1. **API intercept** (preferred) — calls the Shopee search API directly
   using the XHR endpoint pattern used by ``ShopeeMarketScraper``.  Returns
   rich structured data including price, sales count, and seller info.

2. **Static HTML fallback** — issues a plain ``requests`` GET to the Shopee
   search page and parses what is available in the initial HTML.  Results
   are sparser but require no Playwright/browser setup.

The scraper normalises results into ``CompetitorListing`` objects and never
touches any table — all writes are done by ``AnalyzerEngine``.
"""

from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

from src.config.settings import settings
from src.database.models import CompetitorListing
from src.utils.logger import logger

if TYPE_CHECKING:
    pass


# ── Shared HTTP session ───────────────────────────────────────────────────────

_HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-PH,en;q=0.9",
    "Referer":         "https://shopee.ph/",
}

_API_HEADERS: Dict[str, str] = {
    **_HEADERS,
    "Accept":             "application/json",
    "X-Requested-With":   "XMLHttpRequest",
    "X-Api-Source":       "pc",
    "X-Shopee-Language":  "en",
}

# Shopee search API (non-authenticated, read-only endpoint)
_SEARCH_API = (
    "https://shopee.ph/api/v4/search/search_items"
    "?by=relevancy&keyword={keyword}&limit={limit}&newest=0"
    "&order=desc&page_type=search&scenario=PAGE_GLOBAL_SEARCH"
    "&version=2"
)
_SEARCH_HTML = "https://shopee.ph/search?keyword={keyword}"


class CompetitorScraper:
    """Collect competing Shopee listings for a product.

    Parameters
    ----------
    max_results:
        Maximum number of competitor listings to return per product.
        Defaults to ``settings.COMPETITION_MAX_COMPETITORS``.
    request_delay:
        Seconds to sleep between HTTP requests.
        Defaults to ``settings.REQUEST_DELAY_SECONDS``.
    """

    def __init__(
        self,
        max_results: Optional[int] = None,
        request_delay: Optional[float] = None,
    ) -> None:
        self._max     = max_results or settings.COMPETITION_MAX_COMPETITORS
        self._delay   = request_delay if request_delay is not None else settings.REQUEST_DELAY_SECONDS
        self._session = requests.Session()
        self._session.headers.update(_HEADERS)

    # ── Public API ────────────────────────────────────────────────────────────

    def scrape(
        self,
        shopee_product_id: int,
        keyword: str,
        product_key: Optional[str] = None,
        own_url: Optional[str] = None,
    ) -> List[CompetitorListing]:
        """Scrape competing listings for *keyword*.

        Parameters
        ----------
        shopee_product_id:
            ``products.id`` of the product we are analysing.
        keyword:
            Search query (product title, series code, etc.).
        product_key:
            Our product's key — used to flag listings of the exact same SKU.
        own_url:
            Our own listing URL — excluded from competitor results.

        Returns
        -------
        List[CompetitorListing]
            Competitor data, sorted by price ascending.
        """
        # Try JSON API first (lightweight, no browser required)
        results = self._scrape_api(shopee_product_id, keyword, product_key, own_url)

        # Fall back to HTML parsing if API returned nothing
        if not results:
            logger.debug(
                f"[CompetitorScraper] API returned 0 for {keyword!r}, "
                "falling back to HTML scrape"
            )
            results = self._scrape_html(shopee_product_id, keyword, product_key, own_url)

        results.sort(key=lambda c: c.competitor_price)
        logger.info(
            f"[CompetitorScraper] product_id={shopee_product_id} "
            f"keyword={keyword!r}: {len(results)} competitors"
        )
        return results[: self._max]

    # ── Strategy 1: Shopee search API (JSON) ─────────────────────────────────

    def _scrape_api(
        self,
        shopee_product_id: int,
        keyword: str,
        product_key: Optional[str],
        own_url: Optional[str],
    ) -> List[CompetitorListing]:
        url = _SEARCH_API.format(
            keyword=quote_plus(keyword),
            limit=self._max + 5,  # fetch a few extra to filter our own listing
        )
        now = datetime.utcnow()
        listings: List[CompetitorListing] = []

        try:
            response = self._session.get(url, headers=_API_HEADERS, timeout=12)
            time.sleep(self._delay)

            if response.status_code != 200:
                return []

            data = response.json()
            items = data.get("items") or []

            for item in items:
                parsed = self._parse_api_item(item, shopee_product_id, product_key, now)
                if parsed is None:
                    continue
                if own_url and parsed.competitor_url == own_url:
                    continue
                listings.append(parsed)

        except Exception as exc:
            logger.warning(f"[CompetitorScraper] API scrape failed: {exc}")

        return listings

    @staticmethod
    def _parse_api_item(
        item: Dict[str, Any],
        shopee_product_id: int,
        product_key: Optional[str],
        scraped_at: datetime,
    ) -> Optional[CompetitorListing]:
        """Parse one item dict from the Shopee search API response."""
        try:
            item_data = item.get("item_basic", item)

            # Title
            title = str(item_data.get("name", "")).strip()
            if not title:
                return None

            # Price — Shopee stores price × 100_000 as an integer
            raw_price = item_data.get("price") or item_data.get("price_min")
            if raw_price is None:
                return None
            price_php = float(raw_price) / 100_000.0
            if price_php <= 0:
                return None

            # Stock
            stock = item_data.get("stock") or item_data.get("item_stock")
            stock_int = int(stock) if stock is not None else None

            # Seller rating (item-level overall_rating or seller_rating)
            rating_raw = item_data.get("item_rating", {})
            rating = None
            if isinstance(rating_raw, dict):
                rating = rating_raw.get("rating_star") or rating_raw.get("rating_total")
                if rating:
                    rating = float(rating)

            # URL
            shop_id  = item_data.get("shopid", "")
            item_id  = item_data.get("itemid", "")
            url = (
                f"https://shopee.ph/product/{shop_id}/{item_id}"
                if shop_id and item_id
                else ""
            )

            return CompetitorListing(
                shopee_product_id=shopee_product_id,
                product_key=product_key,
                competitor_title=title,
                competitor_price=round(price_php, 2),
                competitor_stock=stock_int,
                seller_rating=rating,
                competitor_url=url,
                scraped_at=scraped_at,
            )

        except Exception as exc:
            logger.debug(f"[CompetitorScraper] API item parse error: {exc}")
            return None

    # ── Strategy 2: HTML fallback ─────────────────────────────────────────────

    def _scrape_html(
        self,
        shopee_product_id: int,
        keyword: str,
        product_key: Optional[str],
        own_url: Optional[str],
    ) -> List[CompetitorListing]:
        url = _SEARCH_HTML.format(keyword=quote_plus(keyword))
        now = datetime.utcnow()
        listings: List[CompetitorListing] = []

        try:
            response = self._session.get(url, timeout=12)
            time.sleep(self._delay)

            if response.status_code != 200:
                return []

            soup = BeautifulSoup(response.text, "html.parser")

            # Shopee renders product cards with data-sqe="item" or
            # class patterns containing "shopee-search-item-result__item"
            cards = soup.select('[data-sqe="item"]')
            if not cards:
                cards = soup.select('[class*="search-item-result__item"]')

            for card in cards[: self._max + 5]:
                parsed = self._parse_html_card(card, shopee_product_id, product_key, now)
                if parsed is None:
                    continue
                if own_url and parsed.competitor_url == own_url:
                    continue
                listings.append(parsed)

        except Exception as exc:
            logger.warning(f"[CompetitorScraper] HTML scrape failed: {exc}")

        return listings

    @staticmethod
    def _parse_html_card(
        card: BeautifulSoup,
        shopee_product_id: int,
        product_key: Optional[str],
        scraped_at: datetime,
    ) -> Optional[CompetitorListing]:
        """Parse one product card from the Shopee search HTML."""
        try:
            # Title from aria-label or inner text of heading
            title = ""
            title_el = card.select_one('[class*="item-name"]') or card.select_one("h3")
            if title_el:
                title = title_el.get_text(strip=True)
            if not title:
                return None

            # Price — look for ₱ symbol or price class
            price_php: Optional[float] = None
            for el in card.select('[class*="price"]'):
                text = el.get_text(strip=True)
                text = re.sub(r'[₱,\s]', '', text)
                try:
                    price_php = float(text.split(".")[0] + "." + (text.split(".")[1][:2] if "." in text else "0"))
                    break
                except (ValueError, IndexError):
                    continue
            if not price_php:
                return None

            # URL
            url = ""
            link = card.select_one("a[href]")
            if link:
                href = link.get("href", "")
                url = href if href.startswith("http") else "https://shopee.ph" + href

            return CompetitorListing(
                shopee_product_id=shopee_product_id,
                product_key=product_key,
                competitor_title=title,
                competitor_price=round(price_php, 2),
                competitor_stock=None,
                seller_rating=None,
                competitor_url=url,
                scraped_at=scraped_at,
            )

        except Exception as exc:
            logger.debug(f"[CompetitorScraper] HTML card parse error: {exc}")
            return None
