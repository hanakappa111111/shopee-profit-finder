"""Amazon Japan scraper for the Shopee arbitrage system."""

import time
from typing import List, Optional
import requests
from bs4 import BeautifulSoup

from src.config.settings import settings
from src.database.models import JapanProduct, JapanSource, StockStatus
from src.utils.logger import logger
from src.utils.retry import retry_on_network_error


from src.utils.scraper_utils import AdaptiveDelay, create_session, is_blocked


class AmazonJapanScraper:
    """Scraper for Amazon Japan (amazon.co.jp) products."""

    def __init__(self):
        """Initialize the Amazon Japan scraper."""
        self.base_url = "https://www.amazon.co.jp/s"
        self.session = create_session(timeout=15)
        self._delay = AdaptiveDelay(base_delay=settings.REQUEST_DELAY_SECONDS)

    @retry_on_network_error()
    def search(self, query: str, limit: Optional[int] = None) -> List[JapanProduct]:
        """
        Search for products on Amazon Japan.

        Args:
            query: The search query string.
            limit: Maximum number of results to return. Uses JAPAN_RESULTS_LIMIT if None.

        Returns:
            List of JapanProduct objects found on Amazon Japan.
        """
        if limit is None:
            limit = settings.JAPAN_RESULTS_LIMIT

        try:
            logger.info(f"Searching Amazon Japan for: {query}")

            url = f"{self.base_url}?k={query}&i=toys"
            response = self.session.get(url, timeout=15)
            response.raise_for_status()

            if is_blocked(response):
                self._delay.on_failure()
                logger.warning(
                    "Amazon Japan returned a CAPTCHA / block page — "
                    "consider reducing request frequency or using a proxy"
                )
                return []

            self._delay.on_success()
            self._delay.wait()

            products = self._parse_results(response.text, limit)
            logger.info(f"Found {len(products)} products on Amazon Japan")
            return products

        except requests.RequestException as e:
            self._delay.on_failure()
            logger.error(f"Error searching Amazon Japan: {e}")
            raise

    def _parse_results(self, html: str, limit: int) -> List[JapanProduct]:
        """
        Parse search results from Amazon Japan HTML.

        Args:
            html: The raw HTML content from the search page.
            limit: Maximum number of products to parse.

        Returns:
            List of parsed JapanProduct objects.
        """
        soup = BeautifulSoup(html, "html.parser")
        products = []

        # Amazon uses div[data-component-type="s-search-result"] for product cards
        cards = soup.select('div[data-component-type="s-search-result"]')

        for card in cards[:limit]:
            try:
                # Extract ASIN from the card
                asin = card.get("data-asin")
                if not asin:
                    continue

                product = self._parse_card(card, asin)
                if product:
                    products.append(product)

            except Exception as e:
                logger.warning(f"Error parsing Amazon card: {e}")
                continue

        return products

    def _parse_card(self, card: BeautifulSoup, asin: str) -> Optional[JapanProduct]:
        """
        Parse a single product card from Amazon Japan.

        Args:
            card: BeautifulSoup element representing a product card.
            asin: Amazon Standard Identification Number.

        Returns:
            Parsed JapanProduct object or None if parsing fails.
        """
        try:
            # Title: extract from h2 a span
            title_elem = card.select_one("h2 a span")
            if not title_elem:
                return None
            title = title_elem.get_text(strip=True)
            if not title:
                return None

            # Price: try .a-price-whole + .a-price-fraction first, then .a-offscreen
            price_str = None
            whole = card.select_one(".a-price-whole")
            fraction = card.select_one(".a-price-fraction")
            if whole and fraction:
                whole_text = whole.get_text(strip=True)
                frac_text = fraction.get_text(strip=True)
                price_str = f"{whole_text}{frac_text}"
            else:
                offscreen = card.select_one(".a-offscreen")
                if offscreen:
                    price_str = offscreen.get_text(strip=True)

            if not price_str:
                logger.debug(f"No price found for {title}")
                return None

            # Clean price: strip ￥ ¥ ,
            price_str = price_str.replace("￥", "").replace("¥", "").replace(",", "").strip()
            try:
                price = float(price_str)
            except ValueError:
                logger.debug(f"Invalid price format: {price_str}")
                return None

            # Image URL
            img_elem = card.select_one("img.s-image")
            image_url = img_elem.get("src") if img_elem else None

            # Product URL from h2 a href
            link_elem = card.select_one("h2 a")
            product_url = link_elem.get("href") if link_elem else None
            if product_url and not product_url.startswith("http"):
                product_url = "https://www.amazon.co.jp" + product_url

            # Stock status: check #availability span text and add-to-cart button
            availability_elem = card.select_one("#availability span")
            stock_text = availability_elem.get_text(strip=True) if availability_elem else ""

            # Check for add-to-cart button presence
            add_to_cart = card.select_one('button[aria-label*="カートに追加"]') or \
                         card.select_one('button[aria-label*="Add to Cart"]')

            if "在庫なし" in stock_text or "売り切れ" in stock_text:
                stock_status = StockStatus.OUT_OF_STOCK
            elif add_to_cart or "在庫あり" in stock_text:
                stock_status = StockStatus.IN_STOCK
            else:
                stock_status = StockStatus.UNKNOWN

            return JapanProduct(
                title=title,
                price=price,
                currency="JPY",
                image_url=image_url,
                product_url=product_url,
                source=JapanSource.AMAZON_JP,
                source_id=asin,
                stock_status=stock_status,
                condition="new",
            )

        except Exception as e:
            logger.warning(f"Error parsing Amazon card with ASIN {asin}: {e}")
            return None
