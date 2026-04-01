"""
Inventory monitoring module.

Tracks stock levels across Japan product sources (Amazon, Rakuten, Yahoo)
and triggers alerts when stock status changes or products go out of stock.
"""

import time
from typing import Callable, Optional

import requests
from bs4 import BeautifulSoup

from src.config.settings import settings
from src.database.database import db
from src.database.models import JapanSource, StockAlert, StockStatus
from src.utils.logger import logger

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def _oos_detected(text: str) -> bool:
    """
    Detect out-of-stock markers in Japanese and English text.

    Args:
        text: Text to search for stock status indicators.

    Returns:
        True if out-of-stock indicators found.
    """
    oos_keywords = [
        "在庫なし",  # Japanese: no stock
        "売り切れ",  # Japanese: sold out
        "品切れ",  # Japanese: out of stock
        "out of stock",
        "sold out",
    ]
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in oos_keywords)


def _fetch_amazon_stock(url: str) -> StockStatus:
    """
    Fetch stock status from Amazon product page.

    Args:
        url: Amazon product URL.

    Returns:
        StockStatus.IN_STOCK or StockStatus.OUT_OF_STOCK.
    """
    try:
        response = requests.get(url, headers=_HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        # Check availability text
        availability = soup.find("span", class_="a-size-base")
        if availability and "in stock" in availability.get_text().lower():
            return StockStatus.IN_STOCK

        # Check for add-to-cart button
        add_to_cart = soup.find("button", id="add-to-cart-button")
        if add_to_cart and not add_to_cart.get("disabled"):
            return StockStatus.IN_STOCK

        if _oos_detected(soup.get_text()):
            return StockStatus.OUT_OF_STOCK

        logger.warning(f"Could not determine Amazon stock for {url}")
        return StockStatus.UNKNOWN

    except Exception as e:
        logger.error(f"Error fetching Amazon stock: {e}")
        return StockStatus.UNKNOWN


def _fetch_rakuten_stock(url: str) -> StockStatus:
    """
    Fetch stock status from Rakuten product page.

    Args:
        url: Rakuten product URL.

    Returns:
        StockStatus.IN_STOCK or StockStatus.OUT_OF_STOCK.
    """
    try:
        response = requests.get(url, headers=_HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        # Check for sold-out class
        if soup.find(class_="soldout"):
            return StockStatus.OUT_OF_STOCK

        # Check cart button text
        cart_button = soup.find("button", id="cart_button")
        if cart_button:
            button_text = cart_button.get_text().lower()
            if "sold out" in button_text or "品切れ" in button_text:
                return StockStatus.OUT_OF_STOCK
            if "add to cart" in button_text or "カートに入れる" in button_text:
                return StockStatus.IN_STOCK

        if _oos_detected(soup.get_text()):
            return StockStatus.OUT_OF_STOCK

        logger.warning(f"Could not determine Rakuten stock for {url}")
        return StockStatus.UNKNOWN

    except Exception as e:
        logger.error(f"Error fetching Rakuten stock: {e}")
        return StockStatus.UNKNOWN


def _fetch_yahoo_stock(url: str) -> StockStatus:
    """
    Fetch stock status from Yahoo Shopping product page.

    Args:
        url: Yahoo Shopping product URL.

    Returns:
        StockStatus.IN_STOCK or StockStatus.OUT_OF_STOCK.
    """
    try:
        response = requests.get(url, headers=_HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        # Check availability element
        availability = soup.find(class_="availability")
        if availability:
            text = availability.get_text().lower()
            if "in stock" in text:
                return StockStatus.IN_STOCK
            if "out of stock" in text or _oos_detected(text):
                return StockStatus.OUT_OF_STOCK

        if _oos_detected(soup.get_text()):
            return StockStatus.OUT_OF_STOCK

        logger.warning(f"Could not determine Yahoo stock for {url}")
        return StockStatus.UNKNOWN

    except Exception as e:
        logger.error(f"Error fetching Yahoo stock: {e}")
        return StockStatus.UNKNOWN


def _fetch_stock(url: str, source: JapanSource) -> StockStatus:
    """
    Route stock fetching to appropriate source-specific function.

    Args:
        url: Product URL.
        source: Data source identifier.

    Returns:
        StockStatus.
    """
    if source == JapanSource.AMAZON:
        return _fetch_amazon_stock(url)
    elif source == JapanSource.RAKUTEN:
        return _fetch_rakuten_stock(url)
    elif source == JapanSource.YAHOO:
        return _fetch_yahoo_stock(url)
    else:
        logger.warning(f"Unknown source: {source}")
        return StockStatus.UNKNOWN


class InventoryMonitor:
    """
    Monitors inventory levels across Japan product sources.

    Tracks stock status changes and alerts when items go out of stock,
    automatically updating associated Shopee listings.
    """

    def __init__(
        self, alert_callback: Optional[Callable[[StockAlert], None]] = None
    ) -> None:
        """
        Initialize inventory monitor.

        Args:
            alert_callback: Optional callback for stock alerts.
        """
        self.alert_callback = alert_callback or self._default_alert_handler
        logger.info("InventoryMonitor initialized")

    def check_all(self) -> list[StockAlert]:
        """
        Check stock status for all monitored products.

        Fetches current stock, compares with stored status, and fires
        alerts for changes. Automatically updates Shopee listings when
        items go out of stock.

        Returns:
            List of StockAlert objects for detected changes.
        """
        alerts = []
        sources = db.get_all_sources()
        logger.info(f"Checking stock for {len(sources)} products")

        for source in sources:
            try:
                current_status = _fetch_stock(source.url, source.source)
                old_status = source.stock_status

                if current_status != old_status:
                    alert = self.check_single(
                        source.url, old_status, source.source
                    )
                    if alert:
                        alerts.append(alert)
                        self.alert_callback(alert)

                        # Handle out-of-stock by updating listings
                        if current_status == StockStatus.OUT_OF_STOCK:
                            self._handle_out_of_stock(source.url)

                # Rate limiting
                time.sleep(1.5)

            except Exception as e:
                logger.error(f"Error checking stock for {source.url}: {e}")

        logger.info(
            f"Checked {len(sources)} products, found {len(alerts)} status changes"
        )
        return alerts

    def check_single(
        self, url: str, old_status: StockStatus, source: JapanSource
    ) -> Optional[StockAlert]:
        """
        Check stock status for a single product.

        Args:
            url: Product URL.
            old_status: Previous stock status.
            source: Data source identifier.

        Returns:
            StockAlert if status changed, None otherwise.
        """
        try:
            current_status = _fetch_stock(url, source)

            if current_status != old_status:
                alert = StockAlert(
                    url=url,
                    source=source,
                    old_status=old_status,
                    new_status=current_status,
                )

                # Update DB
                db.update_source_stock_status(url, current_status)

                logger.info(
                    f"Stock status changed for {url}: {old_status.value} -> {current_status.value}"
                )

                return alert

            return None

        except Exception as e:
            logger.error(f"Error checking single product {url}: {e}")
            return None

    def _handle_out_of_stock(self, japan_url: str) -> None:
        """
        Handle out-of-stock by updating associated Shopee listings.

        Sets stock to 0 for all active listings using this Japan source.

        Args:
            japan_url: Japan product URL that went out of stock.
        """
        try:
            listings = db.get_listings_by_japan_source(japan_url)
            logger.info(
                f"Found {len(listings)} active listings for OOS product {japan_url}"
            )

            for listing in listings:
                if listing.status != "sold_out":
                    db.update_listing(listing.id, stock=0, status="sold_out")
                    logger.info(f"Updated listing {listing.id} to sold_out")

        except Exception as e:
            logger.error(f"Error handling out-of-stock for {japan_url}: {e}")

    def _default_alert_handler(self, alert: StockAlert) -> None:
        """
        Default alert handler: log warning with status change.

        Args:
            alert: StockAlert to handle.
        """
        logger.warning(
            f"Stock alert: {alert.url} "
            f"{alert.old_status.value} -> {alert.new_status.value}"
        )
