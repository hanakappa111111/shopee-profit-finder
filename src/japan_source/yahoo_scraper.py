"""Yahoo Shopping Japan scraper for the Shopee arbitrage system."""

import time
from typing import List, Optional
import requests
from bs4 import BeautifulSoup

from src.config.settings import settings
from src.database.models import JapanProduct, JapanSource, StockStatus
from src.utils.logger import logger
from src.utils.retry import retry_on_network_error


from src.utils.scraper_utils import AdaptiveDelay, create_session, is_blocked


class YahooShoppingScraper:
    """Scraper for Yahoo Shopping Japan products."""

    def __init__(self):
        """Initialize the Yahoo Shopping scraper."""
        self.base_url = "https://shopping.yahoo.co.jp/search"
        self.session = create_session(timeout=15)
        self._delay = AdaptiveDelay(base_delay=settings.REQUEST_DELAY_SECONDS)

    @retry_on_network_error()
    def search(self, query: str, limit: Optional[int] = None) -> List[JapanProduct]:
        """
        Search for products on Yahoo Shopping Japan.

        Args:
            query: The search query string.
            limit: Maximum number of results to return. Uses JAPAN_RESULTS_LIMIT if None.

        Returns:
            List of JapanProduct objects found on Yahoo Shopping Japan.
        """
        if limit is None:
            limit = settings.JAPAN_RESULTS_LIMIT

        try:
            logger.info(f"Searching Yahoo Shopping Japan for: {query}")

            params = {"p": query, "seller": "all"}
            response = self.session.get(self.base_url, params=params, timeout=15)
            response.raise_for_status()

            if is_blocked(response):
                self._delay.on_failure()
                logger.warning("Yahoo Shopping returned a block page")
                return []

            self._delay.on_success()
            self._delay.wait()

            products = self._parse_results(response.text, limit)
            logger.info(f"Found {len(products)} products on Yahoo Shopping Japan")
            return products

        except requests.RequestException as e:
            self._delay.on_failure()
            logger.error(f"Error searching Yahoo Shopping Japan: {e}")
            raise

    def _parse_results(self, html: str, limit: int) -> List[JapanProduct]:
        """
        Parse search results from Yahoo Shopping Japan HTML.

        Args:
            html: The raw HTML content from the search page.
            limit: Maximum number of products to parse.

        Returns:
            List of parsed JapanProduct objects.
        """
        soup = BeautifulSoup(html, "html.parser")
        products = []

        # Try multiple selectors for product cards on Yahoo Shopping
        cards = soup.select("li.SearchResult__item")
        if not cards:
            cards = soup.select("div.LoopList__item")
        if not cards:
            # Fallback to any li with Result in class name
            cards = soup.select('li[class*="Result"]')

        for card in cards[:limit]:
            try:
                product = self._parse_card(card)
                if product:
                    products.append(product)

            except Exception as e:
                logger.warning(f"Error parsing Yahoo Shopping card: {e}")
                continue

        return products

    def _parse_card(self, card: BeautifulSoup) -> Optional[JapanProduct]:
        """
        Parse a single product card from Yahoo Shopping Japan.

        Args:
            card: BeautifulSoup element representing a product card.

        Returns:
            Parsed JapanProduct object or None if parsing fails.
        """
        try:
            # Title: try .SearchResult__title a first, then h3 a
            title_elem = card.select_one(".SearchResult__title a")
            if not title_elem:
                title_elem = card.select_one("h3 a")

            if not title_elem:
                return None

            title = title_elem.get_text(strip=True)
            if not title:
                return None

            # Product URL from title link href
            product_url = title_elem.get("href")
            if product_url and not product_url.startswith("http"):
                product_url = "https://shopping.yahoo.co.jp" + product_url

            # Price: try .SearchResult__price first, then span[class*="Price"]
            price_str = None
            price_elem = card.select_one(".SearchResult__price")
            if price_elem:
                price_str = price_elem.get_text(strip=True)
            else:
                price_elem = card.select_one('span[class*="Price"]')
                if price_elem:
                    price_str = price_elem.get_text(strip=True)

            if not price_str:
                logger.debug(f"No price found for {title}")
                return None

            # Clean price: strip 円 ¥ ￥ 税込
            price_str = price_str.replace("円", "").replace("￥", "").replace("¥", "") \
                                  .replace("税込", "").replace("(税込)", "").strip()
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

            # Stock status detection
            card_text = card.get_text()
            if "在庫なし" in card_text or "売り切れ" in card_text or "販売終了" in card_text:
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
                source=JapanSource.YAHOO_SHOPPING,
                source_id=source_id,
                stock_status=stock_status,
                condition="new",
            )

        except Exception as e:
            logger.warning(f"Error parsing Yahoo Shopping card: {e}")
            return None
