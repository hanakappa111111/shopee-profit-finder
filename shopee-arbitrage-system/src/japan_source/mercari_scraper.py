"""Mercari Japan scraper for the Shopee arbitrage system."""

import json
import time
from typing import List, Optional
import requests
from bs4 import BeautifulSoup

from src.config.settings import settings
from src.database.models import JapanProduct, JapanSource, StockStatus
from src.utils.logger import logger
from src.utils.retry import retry_on_network_error


_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class MercariScraper:
    """
    Scraper for Mercari Japan (second-hand marketplace).

    Note: Mercari is heavily JavaScript-rendered, so this scraper attempts basic
    HTML parsing first but may return limited results. For full support, consider
    using a browser automation tool like Playwright.
    """

    def __init__(self):
        """Initialize the Mercari scraper."""
        self.base_url = "https://www.mercari.com/jp/search"
        self.session = requests.Session()
        self.session.headers.update(_HEADERS)

    @retry_on_network_error()
    def search(self, query: str, limit: Optional[int] = None) -> List[JapanProduct]:
        """
        Search for products on Mercari Japan.

        Args:
            query: The search query string.
            limit: Maximum number of results to return. Uses JAPAN_RESULTS_LIMIT if None.

        Returns:
            List of JapanProduct objects found on Mercari Japan.
        """
        if limit is None:
            limit = settings.JAPAN_RESULTS_LIMIT

        try:
            logger.info(f"Searching Mercari Japan for: {query}")

            params = {"keyword": query, "status": "on_sale"}
            response = self.session.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()

            time.sleep(settings.REQUEST_DELAY_SECONDS)

            # Try JSON-LD parsing first
            products = self._parse_json_ld(response.text)

            # Fallback to DOM parsing if no JSON-LD found
            if not products:
                products = self._parse_dom(response.text, limit)

            # Warn if no products found (likely due to JS rendering)
            if not products:
                logger.warning(
                    "Mercari requires JS rendering; consider using Playwright for full support"
                )

            logger.info(f"Found {len(products)} products on Mercari Japan")
            return products[:limit]

        except requests.RequestException as e:
            logger.error(f"Error searching Mercari Japan: {e}")
            raise

    def _parse_json_ld(self, html: str) -> List[JapanProduct]:
        """
        Parse JSON-LD Product schema from Mercari HTML.

        Args:
            html: The raw HTML content from the search page.

        Returns:
            List of parsed JapanProduct objects from JSON-LD.
        """
        products = []
        soup = BeautifulSoup(html, "html.parser")

        # Find all JSON-LD script tags
        scripts = soup.find_all("script", {"type": "application/ld+json"})

        for script in scripts:
            try:
                data = json.loads(script.string)

                # Handle both single Product and ItemList with items
                if isinstance(data, dict):
                    if data.get("@type") == "Product":
                        product = self._parse_product_json(data)
                        if product:
                            products.append(product)
                    elif data.get("@type") == "ItemList":
                        # Parse items from ItemList
                        items = data.get("itemListElement", [])
                        for item in items:
                            if isinstance(item, dict) and item.get("@type") == "Product":
                                product = self._parse_product_json(item)
                                if product:
                                    products.append(product)

            except (json.JSONDecodeError, TypeError, KeyError) as e:
                logger.debug(f"Error parsing JSON-LD: {e}")
                continue

        return products

    def _parse_product_json(self, data: dict) -> Optional[JapanProduct]:
        """
        Parse a single JSON-LD Product schema object.

        Args:
            data: The JSON-LD Product object.

        Returns:
            Parsed JapanProduct object or None if parsing fails.
        """
        try:
            # Extract name
            title = data.get("name")
            if not title:
                return None

            # Extract URL
            product_url = data.get("url")

            # Extract image
            image_url = None
            image = data.get("image")
            if isinstance(image, str):
                image_url = image
            elif isinstance(image, list) and image:
                image_url = image[0]
            elif isinstance(image, dict):
                image_url = image.get("url")

            # Extract price from offers
            price = None
            offers = data.get("offers", {})
            if isinstance(offers, dict):
                price_str = offers.get("price")
            elif isinstance(offers, list) and offers:
                price_str = offers[0].get("price")
            else:
                price_str = None

            if price_str:
                try:
                    price = float(str(price_str))
                except ValueError:
                    return None

            if not price:
                return None

            # Extract availability
            availability = offers.get("availability", "") if isinstance(offers, dict) else ""
            if "OutOfStock" in availability or "Discontinued" in availability:
                stock_status = StockStatus.OUT_OF_STOCK
            else:
                stock_status = StockStatus.IN_STOCK

            # Generate source_id from URL
            source_id = product_url.split("/")[-1] if product_url else None

            return JapanProduct(
                title=title,
                price=price,
                currency="JPY",
                image_url=image_url,
                product_url=product_url,
                source=JapanSource.MERCARI,
                source_id=source_id,
                stock_status=stock_status,
                condition="used",
            )

        except Exception as e:
            logger.warning(f"Error parsing Mercari JSON-LD product: {e}")
            return None

    def _parse_dom(self, html: str, limit: int) -> List[JapanProduct]:
        """
        Fallback DOM parsing for Mercari products.

        Args:
            html: The raw HTML content from the search page.
            limit: Maximum number of products to parse.

        Returns:
            List of parsed JapanProduct objects from DOM.
        """
        soup = BeautifulSoup(html, "html.parser")
        products = []

        # Try multiple selectors for product cards on Mercari
        cards = soup.select('[data-testid*="item"]')
        if not cards:
            cards = soup.select('li[class*="item"]')

        for card in cards[:limit]:
            try:
                product = self._parse_card(card)
                if product:
                    products.append(product)

            except Exception as e:
                logger.warning(f"Error parsing Mercari DOM card: {e}")
                continue

        return products

    def _parse_card(self, card: BeautifulSoup) -> Optional[JapanProduct]:
        """
        Parse a single product card from Mercari DOM.

        Args:
            card: BeautifulSoup element representing a product card.

        Returns:
            Parsed JapanProduct object or None if parsing fails.
        """
        try:
            # Title: look for heading or link text
            title_elem = card.select_one("h3") or card.select_one("a")
            if not title_elem:
                return None

            title = title_elem.get_text(strip=True)
            if not title:
                return None

            # Product URL
            link_elem = card.select_one("a")
            product_url = link_elem.get("href") if link_elem else None
            if product_url and not product_url.startswith("http"):
                product_url = "https://www.mercari.com" + product_url

            # Price: look for price-related text
            price_str = None
            price_spans = card.find_all("span")
            for span in price_spans:
                text = span.get_text(strip=True)
                if "¥" in text or "￥" in text:
                    price_str = text
                    break

            if not price_str:
                logger.debug(f"No price found for {title}")
                return None

            # Clean price
            price_str = price_str.replace("¥", "").replace("￥", "").strip()
            try:
                price = float(price_str)
            except ValueError:
                logger.debug(f"Invalid price format: {price_str}")
                return None

            # Image URL
            img_elem = card.select_one("img")
            image_url = img_elem.get("src") if img_elem else None

            # Stock: Mercari uses "on_sale" filter, so items are typically in stock
            stock_status = StockStatus.IN_STOCK

            # Generate source_id from URL
            source_id = product_url.split("/")[-1] if product_url else None

            return JapanProduct(
                title=title,
                price=price,
                currency="JPY",
                image_url=image_url,
                product_url=product_url,
                source=JapanSource.MERCARI,
                source_id=source_id,
                stock_status=stock_status,
                condition="used",
            )

        except Exception as e:
            logger.warning(f"Error parsing Mercari card: {e}")
            return None


class JapanSourceSearcher:
    """Aggregates and deduplicates results from all Japan e-commerce platforms."""

    def __init__(self):
        """Initialize all platform scrapers."""
        from src.japan_source.amazon_scraper import AmazonJapanScraper
        from src.japan_source.rakuten_scraper import RakutenScraper
        from src.japan_source.yahoo_scraper import YahooShoppingScraper

        self._amazon = AmazonJapanScraper()
        self._rakuten = RakutenScraper()
        self._yahoo = YahooShoppingScraper()
        self._mercari = MercariScraper()

    def search(self, query: str, limit: Optional[int] = None) -> List[JapanProduct]:
        """
        Search all Japan platforms for products.

        Aggregates results from Amazon, Rakuten, Yahoo Shopping, and Mercari,
        deduplicates by product URL, and sorts by price in ascending order.

        Args:
            query: The search query string.
            limit: Maximum number of results to return. Uses JAPAN_RESULTS_LIMIT if None.

        Returns:
            List of JapanProduct objects sorted by price (lowest first).
        """
        if limit is None:
            limit = settings.JAPAN_RESULTS_LIMIT

        logger.info(f"Searching all Japan sources for: {query}")

        all_products = []
        seen_urls = set()

        # Search all platforms
        platforms = [
            ("Amazon", self._amazon),
            ("Rakuten", self._rakuten),
            ("Yahoo Shopping", self._yahoo),
            ("Mercari", self._mercari),
        ]

        for platform_name, scraper in platforms:
            try:
                products = scraper.search(query, limit=limit)
                logger.info(f"{platform_name} returned {len(products)} products")

                # Deduplicate by URL
                for product in products:
                    if product.product_url and product.product_url not in seen_urls:
                        all_products.append(product)
                        seen_urls.add(product.product_url)
                    elif not product.product_url:
                        # Include products without URLs (but track by source_id as fallback)
                        all_products.append(product)

            except Exception as e:
                logger.error(f"Error searching {platform_name}: {e}")
                continue

        # Sort by price (ascending)
        all_products.sort(key=lambda p: p.price)

        logger.info(f"Aggregated search returned {len(all_products)} unique products")
        return all_products[:limit]
