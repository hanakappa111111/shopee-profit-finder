"""
Price monitoring module.

Tracks price changes across Japan product sources (Amazon, Rakuten, Yahoo)
and triggers alerts when price changes exceed the defined threshold.
"""

import time
from typing import Callable, Optional

import requests
from bs4 import BeautifulSoup

from src.config.settings import settings
from src.database.database import db
from src.database.models import JapanSource, PriceAlert
from src.utils.logger import logger

# Price change threshold: 5%
_DEFAULT_CHANGE_THRESHOLD = 0.05

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def _fetch_amazon_price(url: str) -> Optional[float]:
    """
    Fetch price from Amazon product page.

    Tries multiple price selectors to handle different page layouts.

    Args:
        url: Amazon product URL.

    Returns:
        Price in currency units or None if not found.
    """
    try:
        response = requests.get(url, headers=_HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        # Try multiple selectors
        selectors = [
            "span#priceblock_ourprice",
            "span#priceblock_dealprice",
            "span.a-price .a-offscreen",
            "span#price_inside_buybox",
        ]

        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text().strip()
                # Extract numeric value
                price_str = "".join(c for c in text if c.isdigit() or c == ".")
                if price_str:
                    return float(price_str)

        logger.warning(f"Could not extract Amazon price from {url}")
        return None

    except Exception as e:
        logger.error(f"Error fetching Amazon price: {e}")
        return None


def _fetch_rakuten_price(url: str) -> Optional[float]:
    """
    Fetch price from Rakuten product page.

    Args:
        url: Rakuten product URL.

    Returns:
        Price in currency units or None if not found.
    """
    try:
        response = requests.get(url, headers=_HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        # Try common Rakuten price selectors
        selectors = [
            "span.price2",
            "span.price--default",
        ]

        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text().strip()
                # Extract numeric value
                price_str = "".join(c for c in text if c.isdigit() or c == ".")
                if price_str:
                    return float(price_str)

        logger.warning(f"Could not extract Rakuten price from {url}")
        return None

    except Exception as e:
        logger.error(f"Error fetching Rakuten price: {e}")
        return None


def _fetch_yahoo_price(url: str) -> Optional[float]:
    """
    Fetch price from Yahoo Shopping product page.

    Args:
        url: Yahoo Shopping product URL.

    Returns:
        Price in currency units or None if not found.
    """
    try:
        response = requests.get(url, headers=_HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        # Try common Yahoo price selectors
        selectors = [
            "span.ProductPrice",
            "dd.Price__value",
        ]

        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text().strip()
                # Extract numeric value
                price_str = "".join(c for c in text if c.isdigit() or c == ".")
                if price_str:
                    return float(price_str)

        logger.warning(f"Could not extract Yahoo price from {url}")
        return None

    except Exception as e:
        logger.error(f"Error fetching Yahoo price: {e}")
        return None


def _fetch_price(url: str, source: JapanSource) -> Optional[float]:
    """
    Route price fetching to appropriate source-specific function.

    Args:
        url: Product URL.
        source: Data source identifier.

    Returns:
        Price or None if unable to fetch.
    """
    if source == JapanSource.AMAZON:
        return _fetch_amazon_price(url)
    elif source == JapanSource.RAKUTEN:
        return _fetch_rakuten_price(url)
    elif source == JapanSource.YAHOO:
        return _fetch_yahoo_price(url)
    else:
        logger.warning(f"Unknown source: {source}")
        return None


class PriceMonitor:
    """
    Monitors price changes across Japan product sources.

    Tracks price fluctuations and alerts when changes exceed a configured
    threshold, useful for dynamic repricing strategies.
    """

    def __init__(
        self,
        change_threshold: float = _DEFAULT_CHANGE_THRESHOLD,
        alert_callback: Optional[Callable[[PriceAlert], None]] = None,
    ) -> None:
        """
        Initialize price monitor.

        Args:
            change_threshold: Fractional threshold for price alerts (default 0.05 = 5%).
            alert_callback: Optional callback for price alerts.
        """
        self.change_threshold = change_threshold
        self.alert_callback = alert_callback or self._default_alert_handler
        logger.info(
            f"PriceMonitor initialized with {change_threshold*100:.1f}% threshold"
        )

    def check_all(self) -> list[PriceAlert]:
        """
        Check price changes for all monitored products.

        Fetches current prices, records them in database, and fires alerts
        for significant changes exceeding the threshold.

        Returns:
            List of PriceAlert objects for detected changes.
        """
        alerts = []
        sources = db.get_all_sources()
        logger.info(f"Checking prices for {len(sources)} products")

        for source in sources:
            try:
                current_price = _fetch_price(source.url, source.source)

                if current_price is not None:
                    old_price = source.price
                    db.record_price(source.url, current_price)

                    alert = self.check_single(
                        source.url, old_price, source.source
                    )
                    if alert:
                        alerts.append(alert)
                        self.alert_callback(alert)

                # Rate limiting
                time.sleep(1.5)

            except Exception as e:
                logger.error(f"Error checking price for {source.url}: {e}")

        logger.info(
            f"Checked {len(sources)} products, found {len(alerts)} price changes"
        )
        return alerts

    def check_single(
        self, url: str, old_price: Optional[float], source: JapanSource
    ) -> Optional[PriceAlert]:
        """
        Check price change for a single product.

        Args:
            url: Product URL.
            old_price: Previous price.
            source: Data source identifier.

        Returns:
            PriceAlert if change exceeds threshold, None otherwise.
        """
        try:
            current_price = _fetch_price(url, source)

            if current_price is None:
                return None

            if old_price is None:
                # First time seeing this price
                db.record_price(url, current_price)
                return None

            # Calculate percentage change
            change = (current_price - old_price) / old_price
            change_pct = abs(change)

            if change_pct >= self.change_threshold:
                alert = PriceAlert(
                    url=url,
                    source=source,
                    old_price=old_price,
                    new_price=current_price,
                    change_percent=change,
                )

                # Update DB
                db.record_price(url, current_price)

                logger.info(
                    f"Price change detected for {url}: "
                    f"¥{old_price:.2f} -> ¥{current_price:.2f} ({change*100:+.1f}%)"
                )

                return alert

            return None

        except Exception as e:
            logger.error(f"Error checking single product price {url}: {e}")
            return None

    def _default_alert_handler(self, alert: PriceAlert) -> None:
        """
        Default alert handler: log with directional arrow.

        Args:
            alert: PriceAlert to handle.
        """
        direction = "↑" if alert.change_percent > 0 else "↓"
        logger.warning(
            f"Price alert: {alert.url} "
            f"¥{alert.old_price:.2f} -> ¥{alert.new_price:.2f} "
            f"({alert.change_percent*100:+.1f}%) {direction}"
        )
