"""Rakuten Japan scraper for the Shopee arbitrage system."""

import time
from typing import List, Optional
import requests
from bs4 import BeautifulSoup

from src.config.settings import settings
from src.database.models import JapanProduct, JapanSource, StockStatus
from src.utils.logger import logger
from src.utils.retry import retry_on_network_error


from src.utils.scraper_utils import (
    AdaptiveDelay,
    create_session,
    is_blocked,
    search_rakuten_api,
)


class RakutenScraper:
    """Scraper for Rakuten Japan (rakuten.co.jp) products.

    Prefers the official Rakuten Ichiba API (free tier, 30 req/s) when
    ``RAKUTEN_APP_ID`` is configured.  Falls back to HTML scraping
    if the API key is missing or the API call fails.
    """

    def __init__(self):
        """Initialize the Rakuten scraper."""
        self.base_url = "https://search.rakuten.co.jp/search/mall"
        self.session = create_session(timeout=15)
        self._delay = AdaptiveDelay(base_delay=settings.REQUEST_DELAY_SECONDS)

    @retry_on_network_error()
    def search(self, query: str, limit: Optional[int] = None) -> List[JapanProduct]:
        """
        Search for products on Rakuten Japan.

        Args:
            query: The search query string.
            limit: Maximum number of results to return. Uses JAPAN_RESULTS_LIMIT if None.

        Returns:
            List of JapanProduct objects found on Rakuten Japan.
        """
        if limit is None:
            limit = settings.JAPAN_RESULTS_LIMIT

        try:
            logger.info(f"Searching Rakuten Japan for: {query}")

            # ── Try official API first ───────────────────────────────────
            api_items = search_rakuten_api(query, limit=limit)
            if api_items:
                products = self._parse_api_items(api_items, limit)
                if products:
                    logger.info(
                        f"Rakuten API returned {len(products)} products"
                    )
                    return products

            # ── Fallback: HTML scraping ──────────────────────────────────
            url = f"{self.base_url}/{query}/"
            response = self.session.get(url, timeout=15)
            response.raise_for_status()

            if is_blocked(response):
                self._delay.on_failure()
                logger.warning(
                    "Rakuten returned a block page — "
                    "set RAKUTEN_APP_ID to use the official API instead"
                )
                return []

            self._delay.on_success()
            self._delay.wait()

            products = self._parse_results(response.text, limit)
            logger.info(f"Found {len(products)} products on Rakuten Japan")
            return products

        except requests.RequestException as e:
            self._delay.on_failure()
            logger.error(f"Error searching Rakuten Japan: {e}")
            raise

    def _parse_results(self, html: str, limit: int) -> List[JapanProduct]:
        """
        Parse search results from Rakuten Japan HTML.

        Args:
            html: The raw HTML content from the search page.
            limit: Maximum number of products to parse.

        Returns:
            List of parsed JapanProduct objects.
        """
        soup = BeautifulSoup(html, "html.parser")
        products = []

        # Try multiple selectors for product cards on Rakuten
        cards = soup.select("div.searchresultitem")
        if not cards:
            cards = soup.select("div.item")

        for card in cards[:limit]:
            try:
                product = self._parse_card(card)
                if product:
                    products.append(product)

            except Exception as e:
                logger.warning(f"Error parsing Rakuten card: {e}")
                continue

        return products

    def _parse_card(self, card: BeautifulSoup) -> Optional[JapanProduct]:
        """
        Parse a single product card from Rakuten Japan.

        Args:
            card: BeautifulSoup element representing a product card.

        Returns:
            Parsed JapanProduct object or None if parsing fails.
        """
        try:
            # Title: extract from .title a
            title_elem = card.select_one(".title a")
            if not title_elem:
                return None
            title = title_elem.get_text(strip=True)
            if not title:
                return None

            # Product URL from title link href
            product_url = title_elem.get("href")
            if product_url and not product_url.startswith("http"):
                product_url = "https://search.rakuten.co.jp" + product_url

            # Price: try .price first, then span.important
            price_str = None
            price_elem = card.select_one(".price")
            if price_elem:
                price_str = price_elem.get_text(strip=True)
            else:
                important_elem = card.select_one("span.important")
                if important_elem:
                    price_str = important_elem.get_text(strip=True)

            if not price_str:
                logger.debug(f"No price found for {title}")
                return None

            # Clean price: strip 円 ¥ ￥
            price_str = price_str.replace("円", "").replace("￥", "").replace("¥", "").strip()
            try:
                price = float(price_str)
            except ValueError:
                logger.debug(f"Invalid price format: {price_str}")
                return None

            # Image URL
            img_elem = card.select_one("img")
            image_url = img_elem.get("src") if img_elem else None
            if image_url and not image_url.startswith("http"):
                image_url = "https:" + image_url if image_url.startswith("//") else image_url

            # Stock detection: check for "在庫なし" or "売り切れ" in the entire card text
            card_text = card.get_text()
            if "在庫なし" in card_text or "売り切れ" in card_text:
                stock_status = StockStatus.OUT_OF_STOCK
            else:
                stock_status = StockStatus.IN_STOCK

            # Generate source_id from URL if available
            source_id = product_url.split("/")[-1] if product_url else None

            return JapanProduct(
                title=title,
                price=price,
                currency="JPY",
                image_url=image_url,
                product_url=product_url,
                source=JapanSource.RAKUTEN,
                source_id=source_id,
                stock_status=stock_status,
                condition="new",
            )

        except Exception as e:
            logger.warning(f"Error parsing Rakuten card: {e}")
            return None

    # ── Rakuten API item parser ──────────────────────────────────────────

    def _parse_api_items(
        self, items: list, limit: int
    ) -> List[JapanProduct]:
        """Convert Rakuten API response items to JapanProduct list."""
        products: List[JapanProduct] = []
        for wrapper in items[:limit]:
            try:
                item = wrapper.get("Item", wrapper)
                title = item.get("itemName", "")
                price = float(item.get("itemPrice", 0))
                if not title or price <= 0:
                    continue

                image_urls = item.get("mediumImageUrls", [])
                image_url = (
                    image_urls[0].get("imageUrl") if image_urls else None
                )

                availability = item.get("availability", 1)
                stock_status = (
                    StockStatus.IN_STOCK
                    if availability
                    else StockStatus.OUT_OF_STOCK
                )

                products.append(
                    JapanProduct(
                        title=title,
                        price=price,
                        currency="JPY",
                        image_url=image_url,
                        product_url=item.get("itemUrl", ""),
                        source=JapanSource.RAKUTEN,
                        source_id=item.get("itemCode", ""),
                        stock_status=stock_status,
                        condition="new",
                    )
                )
            except Exception as exc:
                logger.debug(f"Error parsing Rakuten API item: {exc}")
                continue
        return products
