"""Supplier Price Monitor — detect Japan-side price changes.

For each active ProductMatch the monitor:

1. Fetches the current price from the Japan supplier page.
2. Stores a new row in ``supplier_snapshots``.
3. Compares the new price against the most recent snapshot.
4. If the price changed by more than ``PRICE_CHANGE_THRESHOLD_PCT``:
   a. Recalculates profit using the profit engine formula.
   b. If the new profit falls below ``MIN_PROFIT_JPY``, delegates to
      :class:`~src.supplier_monitor.shopee_protection.ShopeeProtection`.
   c. If profit is still positive but lower, adjusts the Shopee price.

Design constraints
------------------
* **Read-only** for all tables except ``supplier_snapshots``.
  Price-/status-changes to ``listings`` are handled exclusively by
  :class:`~src.supplier_monitor.shopee_protection.ShopeeProtection`.
* Uses the existing ``get_active_matches_with_sources()`` DB helper so no
  raw SQL lives in this module.
* Rate-limited: ``settings.REQUEST_DELAY_SECONDS`` between HTTP requests.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import requests
from bs4 import BeautifulSoup

from src.config.settings import settings
from src.database.models import JapanSource, PriceAlert, StockStatus
from src.profit.profit_engine import get_php_to_jpy_rate
from src.utils.logger import logger
from src.utils.retry import retry_on_network_error

if TYPE_CHECKING:
    from src.database.database import Database

# ── Constants ─────────────────────────────────────────────────────────────────

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8",
}

# Minimum absolute % price change that triggers a profit recalculation and
# possible protection action.  Configurable via settings.
_DEFAULT_PRICE_CHANGE_PCT = 5.0


# ── Per-source price fetchers ──────────────────────────────────────────────────

@retry_on_network_error(max_attempts=2)
def _fetch_amazon_price(url: str) -> Optional[float]:
    """Scrape the current JPY price from an Amazon Japan product page."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        selectors = [
            "span#priceblock_ourprice",
            "span#priceblock_dealprice",
            "span.a-price .a-offscreen",
            "span#price_inside_buybox",
            ".a-price-whole",
        ]
        for sel in selectors:
            el = soup.select_one(sel)
            if el:
                raw = el.get_text(strip=True)
                clean = raw.replace("￥", "").replace("¥", "").replace(",", "").strip()
                # strip non-numeric except decimal point
                numeric = "".join(c for c in clean if c.isdigit() or c == ".")
                if numeric:
                    return float(numeric)

        logger.debug(f"[PriceMonitor] No Amazon price found at {url}")
        return None
    except Exception as exc:
        logger.warning(f"[PriceMonitor] Amazon fetch failed ({url}): {exc}")
        return None


@retry_on_network_error(max_attempts=2)
def _fetch_rakuten_price(url: str) -> Optional[float]:
    """Scrape the current JPY price from a Rakuten Japan product page."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        for sel in ["span.price2", "span.price--default", "span.item_price"]:
            el = soup.select_one(sel)
            if el:
                raw = el.get_text(strip=True)
                clean = raw.replace("円", "").replace("￥", "").replace("¥", "").replace(",", "").strip()
                numeric = "".join(c for c in clean if c.isdigit() or c == ".")
                if numeric:
                    return float(numeric)

        logger.debug(f"[PriceMonitor] No Rakuten price found at {url}")
        return None
    except Exception as exc:
        logger.warning(f"[PriceMonitor] Rakuten fetch failed ({url}): {exc}")
        return None


@retry_on_network_error(max_attempts=2)
def _fetch_yahoo_price(url: str) -> Optional[float]:
    """Scrape the current JPY price from a Yahoo Shopping Japan product page."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        for sel in ["span.ProductPrice", "dd.Price__value", "span.ItemPrice"]:
            el = soup.select_one(sel)
            if el:
                raw = el.get_text(strip=True)
                clean = raw.replace("円", "").replace("￥", "").replace("¥", "") \
                           .replace("税込", "").replace(",", "").strip()
                numeric = "".join(c for c in clean if c.isdigit() or c == ".")
                if numeric:
                    return float(numeric)

        logger.debug(f"[PriceMonitor] No Yahoo price found at {url}")
        return None
    except Exception as exc:
        logger.warning(f"[PriceMonitor] Yahoo fetch failed ({url}): {exc}")
        return None


@retry_on_network_error(max_attempts=2)
def _fetch_mercari_price(url: str) -> Optional[float]:
    """Scrape the current JPY price from a Mercari Japan product page."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        for sel in ['span[data-testid="price"]', "span.merPrice", ".item-price"]:
            el = soup.select_one(sel)
            if el:
                raw = el.get_text(strip=True)
                clean = raw.replace("¥", "").replace("￥", "").replace(",", "").strip()
                numeric = "".join(c for c in clean if c.isdigit() or c == ".")
                if numeric:
                    return float(numeric)

        logger.debug(f"[PriceMonitor] No Mercari price found at {url}")
        return None
    except Exception as exc:
        logger.warning(f"[PriceMonitor] Mercari fetch failed ({url}): {exc}")
        return None


def _fetch_price(url: str, source: str) -> Optional[float]:
    """Route price fetching to the correct source-specific function.

    Args:
        url:    Japan product URL.
        source: ``JapanSource`` enum value string.

    Returns:
        Price in JPY or ``None`` if scraping failed.
    """
    if source == JapanSource.AMAZON_JP.value:
        return _fetch_amazon_price(url)
    elif source == JapanSource.RAKUTEN.value:
        return _fetch_rakuten_price(url)
    elif source == JapanSource.YAHOO_SHOPPING.value:
        return _fetch_yahoo_price(url)
    elif source == JapanSource.MERCARI.value:
        return _fetch_mercari_price(url)
    else:
        logger.warning(f"[PriceMonitor] Unknown source {source!r} for {url}")
        return None


# ── Main class ────────────────────────────────────────────────────────────────

class SupplierPriceMonitor:
    """Monitor Japan supplier prices and trigger protection when profits erode.

    Parameters
    ----------
    db:
        Open :class:`~src.database.database.Database` instance.
    price_change_threshold_pct:
        Minimum absolute % price change that triggers profit recalculation.
        Defaults to ``settings.PROFIT_RECALC_THRESHOLD_PCT``.
    min_profit_jpy:
        Listings whose recalculated profit falls below this threshold are
        paused via ``ShopeeProtection``.
        Defaults to ``settings.MIN_PROFIT_YEN``.
    request_delay:
        Seconds between HTTP requests to respect rate limits.
        Defaults to ``settings.REQUEST_DELAY_SECONDS``.
    """

    def __init__(
        self,
        db: "Database",
        price_change_threshold_pct: Optional[float] = None,
        min_profit_jpy: Optional[float] = None,
        request_delay: Optional[float] = None,
    ) -> None:
        self._db = db
        self._threshold_pct = (
            price_change_threshold_pct
            if price_change_threshold_pct is not None
            else getattr(settings, "PROFIT_RECALC_THRESHOLD_PCT", _DEFAULT_PRICE_CHANGE_PCT)
        )
        self._min_profit = (
            min_profit_jpy
            if min_profit_jpy is not None
            else settings.MIN_PROFIT_YEN
        )
        self._delay = (
            request_delay
            if request_delay is not None
            else settings.REQUEST_DELAY_SECONDS
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self) -> List[PriceAlert]:
        """Execute one full price-monitoring cycle.

        Iterates over every active ProductMatch, fetches the current Japan
        supplier price, records a snapshot, and triggers protection actions
        when the price change erodes profitability below the minimum threshold.

        Returns:
            List of :class:`~src.database.models.PriceAlert` objects generated
            during this run (one per match where a significant change was found).
        """
        matches = self._db.get_active_matches_with_sources()
        logger.info(f"[PriceMonitor] Checking {len(matches)} active matches")

        alerts: List[PriceAlert] = []

        for match in matches:
            try:
                alert = self._check_match(match)
                if alert:
                    alerts.append(alert)
            except Exception as exc:
                logger.error(
                    f"[PriceMonitor] Error processing match "
                    f"japan_url={match.get('japan_url')!r}: {exc}",
                    exc_info=True,
                )
            finally:
                time.sleep(self._delay)

        logger.info(
            f"[PriceMonitor] Cycle complete — "
            f"{len(matches)} checked, {len(alerts)} alerts"
        )
        return alerts

    # ── Internal logic ────────────────────────────────────────────────────────

    def _check_match(
        self,
        match: Dict[str, Any],
    ) -> Optional[PriceAlert]:
        """Check one match for price changes and act if necessary.

        Returns a PriceAlert if the price changed by more than the threshold,
        otherwise None.
        """
        japan_url    = match["japan_url"]
        japan_source = match["japan_source"]
        product_key  = match.get("japan_product_key")

        # Fetch current price from supplier page
        current_price = _fetch_price(japan_url, japan_source)
        if current_price is None:
            logger.debug(f"[PriceMonitor] Could not fetch price for {japan_url}")
            return None

        # Always record a snapshot (full time-series preserved)
        self._db.insert_supplier_snapshot(
            supplier_url=japan_url,
            price_jpy=current_price,
            stock_status=match.get("japan_stock", StockStatus.UNKNOWN.value),
            product_key=product_key,
        )

        # Compare against the second-most-recent snapshot (the one before we
        # just inserted) to detect the change direction.
        history = self._db.get_supplier_snapshots(japan_url, limit=2)
        if len(history) < 2:
            # First ever snapshot — nothing to compare against yet.
            return None

        previous_price = history[1]["price_jpy"]  # oldest of the two

        if previous_price <= 0:
            return None

        change_pct = abs((current_price - previous_price) / previous_price) * 100.0

        if change_pct < self._threshold_pct:
            return None  # Change is within tolerance

        # Build alert
        alert = PriceAlert(
            japan_product_url=japan_url,
            old_price_jpy=previous_price,
            new_price_jpy=current_price,
            change_percent=round((current_price - previous_price) / previous_price * 100, 2),
        )

        logger.info(
            f"[PriceMonitor] Price change {change_pct:.1f}% detected — "
            f"url={japan_url} ¥{previous_price:.0f} → ¥{current_price:.0f}"
        )

        # Recalculate profit with the new price
        self._handle_price_change(match, current_price, alert)

        return alert

    def _handle_price_change(
        self,
        match: Dict[str, Any],
        new_price_jpy: float,
        alert: PriceAlert,
    ) -> None:
        """Recalculate profit and delegate protective actions.

        If the new profit falls below ``min_profit_jpy``, pauses all Shopee
        listings linked to this Japan product.  If profit decreased but is
        still positive, adjusts the Shopee listing price upwards.
        """
        from src.supplier_monitor.shopee_protection import ShopeeProtection

        shopee_product_id = match["shopee_product_id"]
        japan_product_id  = match["japan_product_id"]
        japan_url         = match["japan_url"]

        # Retrieve the latest profit analysis row for this pair
        profit_row = self._db.get_profit_analysis_for_match(
            shopee_product_id=shopee_product_id,
            japan_product_id=japan_product_id,
        )

        if profit_row is None:
            logger.debug(
                f"[PriceMonitor] No profit_analysis row for pair "
                f"shopee={shopee_product_id} japan={japan_product_id} — skipping"
            )
            return

        # Re-derive profit with the updated supplier price.
        # Uses the same formula as profit_engine but inline to avoid a circular
        # import of ProfitEngine (which takes MatchResult objects, not raw dicts).
        exchange_rate         = get_php_to_jpy_rate()
        fee_rate              = profit_row.get("fee_rate", settings.SHOPEE_FEE_RATE)
        shopee_price          = profit_row.get("shopee_price", 0.0)
        domestic_shipping     = profit_row.get("domestic_shipping_cost", settings.DOMESTIC_SHIPPING_YEN)
        safety_margin         = profit_row.get("safety_margin", settings.SAFETY_MARGIN_YEN)

        shopee_fee            = shopee_price * fee_rate
        net_revenue_jpy       = (shopee_price - shopee_fee) * exchange_rate
        new_cost_jpy          = new_price_jpy + domestic_shipping
        new_profit            = net_revenue_jpy - new_cost_jpy - safety_margin
        new_roi               = new_profit / new_cost_jpy if new_cost_jpy > 0 else 0.0

        logger.info(
            f"[PriceMonitor] Recalculated profit for japan_url={japan_url!r}: "
            f"¥{profit_row.get('profit', 0):.0f} → ¥{new_profit:.0f} "
            f"(ROI {new_roi * 100:.1f}%)"
        )

        protection = ShopeeProtection(db=self._db)

        # Find listings linked to this Japan source URL
        listings = self._db.get_listings_by_source_url(japan_url)

        for listing in listings:
            listing_id = listing["id"]
            listing_status = listing.get("status", "")

            if new_profit < self._min_profit:
                # Profit dropped below minimum — pause listing
                protection.pause_listing(
                    listing_id=listing_id,
                    reason=(
                        f"Supplier price increased ¥{alert.old_price_jpy:.0f} → "
                        f"¥{alert.new_price_jpy:.0f}; "
                        f"recalculated profit ¥{new_profit:.0f} < min ¥{self._min_profit:.0f}"
                    ),
                    current_status=listing_status,
                )
            elif new_profit < profit_row.get("profit", 0.0):
                # Profit decreased but still positive — adjust Shopee price
                from src.profit.profit_engine import ProfitEngine
                engine = ProfitEngine()
                suggested = engine.suggested_shopee_price(
                    japan_price_jpy=new_price_jpy,
                    target_roi=settings.MIN_ROI_PERCENT,
                )
                protection.adjust_price(
                    listing_id=listing_id,
                    new_price=suggested,
                    reason=(
                        f"Supplier price rose ¥{alert.old_price_jpy:.0f} → "
                        f"¥{alert.new_price_jpy:.0f}; "
                        f"suggested ₱{suggested:.2f} to maintain min ROI"
                    ),
                    current_status=listing_status,
                )
