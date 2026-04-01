"""
Playwright-based web scraper for Shopee market data.

This module implements a high-performance, anti-detection market scraper that collects
product listings from Shopee Philippines. It uses Playwright for browser automation
and includes retry logic, API interception, and DOM parsing fallbacks.
"""

import asyncio
import time
from typing import List
from datetime import datetime

from playwright.async_api import async_playwright, Browser, Page, BrowserContext

from src.config.settings import settings
from src.database.models import ShopeeProduct
from src.database.database import db
from src.utils.logger import logger


class ShopeeMarketScraper:
    """
    Playwright-based scraper for Shopee market data with anti-detection features.

    This scraper automates the collection of product listings from Shopee.
    It handles pagination, API interception, DOM parsing, and automatic retries
    with exponential backoff. Browser lifecycle is managed via async context manager.

    Attributes:
        browser (Browser | None): Playwright browser instance.
        context (BrowserContext | None): Browser context for page management.
        page (Page | None): Active browser page.
    """

    def __init__(self) -> None:
        """Initialize the scraper with no active browser."""
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None

    async def __aenter__(self) -> "ShopeeMarketScraper":
        """Async context manager entry: start browser."""
        await self._start_browser()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit: close browser."""
        await self._close_browser()

    async def _start_browser(self) -> None:
        """
        Launch Chromium browser with anti-detection configuration.

        Sets up a headless Chromium instance with anti-bot detection arguments,
        custom user-agent, and script injection to hide webdriver property.

        Raises:
            Exception: If browser launch fails.
        """
        playwright = await async_playwright().start()

        anti_detection_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-gpu",
            "--single-process",
        ]

        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        self.browser = await playwright.chromium.launch(
            headless=settings.PLAYWRIGHT_HEADLESS,
            args=anti_detection_args,
        )

        self.context = await self.browser.new_context(user_agent=user_agent)
        self.page = await self.context.new_page()

        # Inject script to hide webdriver property
        await self.page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => false});"
        )

        logger.info("Browser started with anti-detection configuration")

    async def _close_browser(self) -> None:
        """
        Close browser and cleanup resources.

        Properly closes the page, context, and browser instances.
        """
        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        logger.info("Browser closed")

    async def scrape_all_keywords(self) -> List[ShopeeProduct]:
        """
        Scrape products for all configured search keywords.

        Iterates through settings.SEARCH_KEYWORDS, scrapes each keyword up to
        settings.MAX_PAGES_PER_KEYWORD pages, deduplicates by URL, and saves
        all products to the database.

        Returns:
            List[ShopeeProduct]: Deduplicated list of scraped products.
        """
        all_products: List[ShopeeProduct] = []
        seen_urls: set = set()

        for keyword in settings.SEARCH_KEYWORDS:
            logger.info(f"Scraping keyword: {keyword}")
            products = await self._scrape_keyword(
                keyword, settings.MAX_PAGES_PER_KEYWORD
            )

            for product in products:
                if product.url not in seen_urls:
                    all_products.append(product)
                    seen_urls.add(product.url)

        logger.info(f"Total products scraped (deduplicated): {len(all_products)}")
        await self.save_products(all_products)

        return all_products

    async def _scrape_keyword(
        self, keyword: str, max_pages: int
    ) -> List[ShopeeProduct]:
        """
        Scrape products for a single keyword across multiple pages.

        Iterates through pages 0 to max_pages-1, constructing search URLs
        and calling _scrape_page_with_retry for each page.

        Args:
            keyword (str): Search keyword to scrape.
            max_pages (int): Maximum number of pages to scrape.

        Returns:
            List[ShopeeProduct]: Products found for this keyword.
        """
        products: List[ShopeeProduct] = []

        for page_num in range(max_pages):
            # Shopee uses ?page=X in query params
            url = (
                f"{settings.SHOPEE_BASE_URL}/search"
                f"?keyword={keyword}&page={page_num}"
            )

            page_products = await self._scrape_page_with_retry(url, keyword)
            products.extend(page_products)

            if page_products:
                logger.info(
                    f"Keyword '{keyword}', page {page_num}: "
                    f"scraped {len(page_products)} products"
                )
            else:
                logger.info(f"Keyword '{keyword}', page {page_num}: no products found")

            # Rate limiting
            await asyncio.sleep(settings.REQUEST_DELAY_SECONDS)

        return products

    async def _scrape_page_with_retry(
        self, url: str, keyword: str
    ) -> List[ShopeeProduct]:
        """
        Scrape a single page with automatic retry logic.

        Retries with exponential backoff (up to settings.RETRY_MAX_ATTEMPTS times)
        if scraping fails.

        Args:
            url (str): URL to scrape.
            keyword (str): Search keyword context.

        Returns:
            List[ShopeeProduct]: Products found on the page.
        """
        for attempt in range(settings.RETRY_MAX_ATTEMPTS):
            try:
                products = await self._scrape_page(url, keyword)
                return products
            except Exception as e:
                if attempt < settings.RETRY_MAX_ATTEMPTS - 1:
                    backoff_time = (
                        settings.RETRY_BACKOFF_SECONDS * (2 ** attempt)
                    )
                    logger.warning(
                        f"Scrape attempt {attempt + 1} failed for {url}: {e}. "
                        f"Retrying in {backoff_time}s..."
                    )
                    await asyncio.sleep(backoff_time)
                else:
                    logger.error(f"Failed to scrape {url} after {attempt + 1} attempts: {e}")
                    return []

        return []

    async def _scrape_page(self, url: str, keyword: str) -> List[ShopeeProduct]:
        """
        Scrape a single page using API interception or DOM parsing.

        First attempts to intercept the Shopee internal API response
        (api/v4/search/search_items), then falls back to DOM parsing if needed.

        Args:
            url (str): URL to scrape.
            keyword (str): Search keyword context.

        Returns:
            List[ShopeeProduct]: Products found on the page.

        Raises:
            Exception: If navigation or parsing fails.
        """
        assert self.page is not None, "Page not initialized"

        api_data = None

        async def handle_route(route):
            """Intercept and capture API responses."""
            nonlocal api_data
            response = await route.fetch()
            if "api/v4/search/search_items" in route.request.url:
                try:
                    api_data = await response.json()
                except Exception as e:
                    logger.warning(f"Failed to parse API response: {e}")
            await route.continue_()

        await self.page.route("**/*", handle_route)

        # Set timeout and navigate
        await self.page.goto(url, timeout=settings.PLAYWRIGHT_TIMEOUT_MS)
        await self.page.wait_for_timeout(2000)

        products: List[ShopeeProduct] = []

        # Try API interception first
        if api_data:
            items = api_data.get("data", {}).get("items", [])
            products = self._parse_api_items(items, keyword)
            logger.info(f"Parsed {len(products)} products from API")
        else:
            # Fall back to DOM parsing
            products = await self._parse_dom_products(self.page, keyword)
            logger.info(f"Parsed {len(products)} products from DOM")

        return products

    def _parse_api_items(
        self, items: list, keyword: str
    ) -> List[ShopeeProduct]:
        """
        Parse products from Shopee internal API response.

        Extracts product information from the standardized API JSON format.
        Prices are provided as ×100000 (multiply by 100000 for actual price in cents).
        Images use Shopee's CDN pattern: cf.shopee.ph/file/{image_id}.
        URLs follow pattern: shopee.ph/product/{shopid}/{itemid}.

        Args:
            items (list): List of item objects from API response.
            keyword (str): Search keyword context.

        Returns:
            List[ShopeeProduct]: Parsed product objects.
        """
        products: List[ShopeeProduct] = []

        for item in items:
            try:
                shopid = item.get("shopid")
                itemid = item.get("itemid")
                name = item.get("name", "")
                price = item.get("price")  # Price × 100000
                image_id = item.get("image")
                sales = item.get("sales", 0)
                rating = item.get("rating", 0)

                # Convert price from API format (× 100000)
                actual_price = price / 100000 if price else 0

                # Build image URL
                image_url = (
                    f"cf.shopee.ph/file/{image_id}" if image_id else ""
                )

                # Build product URL
                url = f"shopee.ph/product/{shopid}/{itemid}"

                product = ShopeeProduct(
                    name=name,
                    price=actual_price,
                    url=url,
                    image_url=image_url,
                    sales_count=sales,
                    rating=rating,
                    keyword=keyword,
                    scraped_at=datetime.utcnow(),
                )

                products.append(product)

            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Failed to parse API item: {e}")
                continue

        return products

    async def _parse_dom_products(
        self, page: Page, keyword: str
    ) -> List[ShopeeProduct]:
        """
        Parse products from page DOM as fallback.

        Uses CSS selectors to extract product information from rendered HTML.
        This is a fallback when API interception fails.

        Args:
            page (Page): Playwright page object.
            keyword (str): Search keyword context.

        Returns:
            List[ShopeeProduct]: Parsed product objects.
        """
        products: List[ShopeeProduct] = []

        try:
            # Selector for product cards (adjust based on actual Shopee structure)
            product_cards = await page.query_selector_all('[data-testid="product-item"]')

            for card in product_cards:
                try:
                    name_elem = await card.query_selector("a[title]")
                    name = (
                        await name_elem.get_attribute("title")
                        if name_elem
                        else ""
                    )

                    price_elem = await card.query_selector(".price")
                    price_text = (
                        await price_elem.inner_text() if price_elem else "0"
                    )
                    price = float(price_text.replace("₱", "").replace(",", ""))

                    url_elem = await card.query_selector("a[href]")
                    url = (
                        await url_elem.get_attribute("href")
                        if url_elem
                        else ""
                    )

                    image_elem = await card.query_selector("img")
                    image_url = (
                        await image_elem.get_attribute("src")
                        if image_elem
                        else ""
                    )

                    sales_elem = await card.query_selector(".sales")
                    sales_text = (
                        await sales_elem.inner_text() if sales_elem else "0"
                    )
                    sales = int(
                        sales_text.split()[0].replace("K", "").replace(",", "")
                    )

                    product = ShopeeProduct(
                        name=name,
                        price=price,
                        url=url,
                        image_url=image_url,
                        sales_count=sales,
                        rating=0,
                        keyword=keyword,
                        scraped_at=datetime.utcnow(),
                    )

                    products.append(product)

                except (AttributeError, ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse DOM product card: {e}")
                    continue

        except Exception as e:
            logger.error(f"DOM parsing failed: {e}")

        return products

    async def save_products(self, products: List[ShopeeProduct]) -> int:
        """
        Save products to database.

        Upserts products using db.upsert_product (updates if exists, inserts if new).

        Args:
            products (List[ShopeeProduct]): Products to save.

        Returns:
            int: Number of products saved.
        """
        count = 0
        for product in products:
            try:
                db.upsert_product(product)
                count += 1
            except Exception as e:
                logger.error(f"Failed to save product {product.name}: {e}")

        logger.info(f"Saved {count}/{len(products)} products to database")
        return count


def run_market_scraper() -> List[ShopeeProduct]:
    """
    Synchronous wrapper to run the market scraper.

    Uses asyncio.run to execute the async scraper and return results.

    Returns:
        List[ShopeeProduct]: All scraped products.
    """
    return asyncio.run(_async_run_scraper())


async def _async_run_scraper() -> List[ShopeeProduct]:
    """
    Internal async function for market scraper execution.

    Returns:
        List[ShopeeProduct]: All scraped products.
    """
    async with ShopeeMarketScraper() as scraper:
        products = await scraper.scrape_all_keywords()
    return products
