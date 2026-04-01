"""Supplier Inventory Monitor — detect Japan-side stock transitions.

For each active ProductMatch the monitor:

1. Fetches the current stock status from the Japan supplier page.
2. Stores a new row in ``supplier_snapshots`` (same table as price monitor,
   so the full time-series of both price and stock lives in one place).
3. Compares the new stock status against the most recent snapshot.
4. If the supplier transitioned to OUT_OF_STOCK:
   → delegates to :class:`~src.supplier_monitor.shopee_protection.ShopeeProtection`
     to pause all linked Shopee listings.
5. If the supplier came back IN_STOCK (restock event):
   → logs the restock for operator review (does NOT auto-resume listings,
     since a manual decision is safer here).

Design constraints
------------------
* Deduplicates actions: if a listing is already ``paused`` or ``sold_out``
  this module does not send a second pause request.
* Rate-limited via ``settings.REQUEST_DELAY_SECONDS``.
* No raw SQL — all DB access goes through :class:`~src.database.database.Database`
  methods.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import requests
from bs4 import BeautifulSoup

from src.config.settings import settings
from src.database.models import JapanSource, StockAlert, StockStatus
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

# Japanese / English out-of-stock keyword patterns
_OOS_KEYWORDS = [
    "在庫なし",    # no stock
    "売り切れ",    # sold out
    "品切れ",      # out of stock
    "販売終了",    # sales ended
    "out of stock",
    "sold out",
    "unavailable",
]


def _oos_detected(text: str) -> bool:
    """Return True when any out-of-stock indicator is present in *text*."""
    lower = text.lower()
    return any(kw in lower for kw in _OOS_KEYWORDS)


# ── Per-source stock fetchers ──────────────────────────────────────────────────

@retry_on_network_error(max_attempts=2)
def _fetch_amazon_stock(url: str) -> StockStatus:
    """Fetch stock status from an Amazon Japan product page."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        # Prefer the explicit availability div
        avail = soup.select_one("#availability span") or soup.select_one("#availability")
        if avail:
            avail_text = avail.get_text(strip=True).lower()
            if "in stock" in avail_text or "在庫あり" in avail_text:
                return StockStatus.IN_STOCK
            if _oos_detected(avail_text):
                return StockStatus.OUT_OF_STOCK

        # Presence of an active add-to-cart button → in stock
        atc = soup.select_one('input#add-to-cart-button, button#add-to-cart-button')
        if atc and not atc.get("disabled"):
            return StockStatus.IN_STOCK

        if _oos_detected(soup.get_text()):
            return StockStatus.OUT_OF_STOCK

        return StockStatus.UNKNOWN

    except Exception as exc:
        logger.warning(f"[InventoryMonitor] Amazon stock fetch failed ({url}): {exc}")
        return StockStatus.UNKNOWN


@retry_on_network_error(max_attempts=2)
def _fetch_rakuten_stock(url: str) -> StockStatus:
    """Fetch stock status from a Rakuten Japan product page."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        if soup.find(class_="soldout"):
            return StockStatus.OUT_OF_STOCK

        cart_btn = soup.find("button", id="cart_button") or soup.select_one(".cart-button")
        if cart_btn:
            btn_text = cart_btn.get_text(strip=True).lower()
            if "sold out" in btn_text or "品切れ" in btn_text or "売り切れ" in btn_text:
                return StockStatus.OUT_OF_STOCK
            if "カートに入れる" in btn_text or "add to cart" in btn_text:
                return StockStatus.IN_STOCK

        if _oos_detected(soup.get_text()):
            return StockStatus.OUT_OF_STOCK

        return StockStatus.UNKNOWN

    except Exception as exc:
        logger.warning(f"[InventoryMonitor] Rakuten stock fetch failed ({url}): {exc}")
        return StockStatus.UNKNOWN


@retry_on_network_error(max_attempts=2)
def _fetch_yahoo_stock(url: str) -> StockStatus:
    """Fetch stock status from a Yahoo Shopping Japan product page."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        avail = soup.find(class_="availability") or soup.select_one(".StockLabel")
        if avail:
            avail_text = avail.get_text(strip=True).lower()
            if "in stock" in avail_text or "在庫あり" in avail_text:
                return StockStatus.IN_STOCK
            if _oos_detected(avail_text):
                return StockStatus.OUT_OF_STOCK

        if _oos_detected(soup.get_text()):
            return StockStatus.OUT_OF_STOCK

        return StockStatus.UNKNOWN

    except Exception as exc:
        logger.warning(f"[InventoryMonitor] Yahoo stock fetch failed ({url}): {exc}")
        return StockStatus.UNKNOWN


@retry_on_network_error(max_attempts=2)
def _fetch_mercari_stock(url: str) -> StockStatus:
    """Fetch stock status from a Mercari Japan listing page.

    Mercari items are either on sale (IN_STOCK) or sold (OUT_OF_STOCK).
    """
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        # Sold badge
        sold_badge = soup.select_one('[data-testid="soldout-badge"]') or \
                     soup.select_one(".soldout-badge") or \
                     soup.find("span", string=lambda t: t and "sold" in t.lower())
        if sold_badge:
            return StockStatus.OUT_OF_STOCK

        if _oos_detected(soup.get_text()):
            return StockStatus.OUT_OF_STOCK

        # Buy button present → in stock
        buy_btn = soup.select_one('[data-testid="buy-button"]') or \
                  soup.select_one(".buy-button")
        if buy_btn:
            return StockStatus.IN_STOCK

        return StockStatus.UNKNOWN

    except Exception as exc:
        logger.warning(f"[InventoryMonitor] Mercari stock fetch failed ({url}): {exc}")
        return StockStatus.UNKNOWN


def _fetch_stock(url: str, source: str) -> StockStatus:
    """Route stock fetching to the correct source-specific function.

    Args:
        url:    Japan product URL.
        source: ``JapanSource`` enum value string.

    Returns:
        Current :class:`~src.database.models.StockStatus`.
    """
    if source == JapanSource.AMAZON_JP.value:
        return _fetch_amazon_stock(url)
    elif source == JapanSource.RAKUTEN.value:
        return _fetch_rakuten_stock(url)
    elif source == JapanSource.YAHOO_SHOPPING.value:
        return _fetch_yahoo_stock(url)
    elif source == JapanSource.MERCARI.value:
        return _fetch_mercari_stock(url)
    else:
        logger.warning(f"[InventoryMonitor] Unknown source {source!r} for {url}")
        return StockStatus.UNKNOWN


# ── Main class ────────────────────────────────────────────────────────────────

class SupplierInventoryMonitor:
    """Monitor Japan supplier stock status and pause listings on OOS events.

    Parameters
    ----------
    db:
        Open :class:`~src.database.database.Database` instance.
    request_delay:
        Seconds between HTTP requests.
        Defaults to ``settings.REQUEST_DELAY_SECONDS``.
    """

    def __init__(
        self,
        db: "Database",
        request_delay: Optional[float] = None,
    ) -> None:
        self._db = db
        self._delay = (
            request_delay
            if request_delay is not None
            else settings.REQUEST_DELAY_SECONDS
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def run(self) -> List[StockAlert]:
        """Execute one full inventory-monitoring cycle.

        Iterates over every active ProductMatch, fetches the current Japan
        supplier stock status, records a snapshot, and triggers protection
        actions on OOS transitions.

        Returns:
            List of :class:`~src.database.models.StockAlert` objects generated
            during this run (one per match where a stock transition occurred).
        """
        matches = self._db.get_active_matches_with_sources()
        logger.info(f"[InventoryMonitor] Checking {len(matches)} active matches")

        alerts: List[StockAlert] = []

        for match in matches:
            try:
                alert = self._check_match(match)
                if alert:
                    alerts.append(alert)
            except Exception as exc:
                logger.error(
                    f"[InventoryMonitor] Error processing match "
                    f"japan_url={match.get('japan_url')!r}: {exc}",
                    exc_info=True,
                )
            finally:
                time.sleep(self._delay)

        logger.info(
            f"[InventoryMonitor] Cycle complete — "
            f"{len(matches)} checked, {len(alerts)} stock alerts"
        )
        return alerts

    # ── Internal logic ────────────────────────────────────────────────────────

    def _check_match(
        self,
        match: Dict[str, Any],
    ) -> Optional[StockAlert]:
        """Check one match for stock status changes.

        Returns a :class:`~src.database.models.StockAlert` if the supplier
        transitioned to or from out-of-stock, otherwise ``None``.
        """
        japan_url    = match["japan_url"]
        japan_source = match["japan_source"]
        product_key  = match.get("japan_product_key")

        # Fetch current status
        current_status = _fetch_stock(japan_url, japan_source)

        # Retrieve latest snapshot to determine previous status
        prev_snapshot = self._db.get_latest_supplier_snapshot(japan_url)
        prev_status_str = (
            prev_snapshot["stock_status"] if prev_snapshot else None
        )

        # Record snapshot (always append, never upsert)
        # Use the latest scraped price from the DB row if we did not re-fetch it
        stored_price = match.get("japan_price_jpy", 0.0)
        self._db.insert_supplier_snapshot(
            supplier_url=japan_url,
            price_jpy=stored_price,
            stock_status=current_status.value,
            product_key=product_key,
        )

        # Also update the canonical stock field in the sources table
        self._db.update_source_stock(japan_url, current_status.value)

        # If this is the first snapshot there is nothing to compare against
        if prev_status_str is None:
            return None

        prev_status = StockStatus(prev_status_str) if prev_status_str in StockStatus._value2member_map_ else StockStatus.UNKNOWN

        # No transition → nothing to do
        if current_status == prev_status:
            return None

        logger.info(
            f"[InventoryMonitor] Stock transition detected for {japan_url}: "
            f"{prev_status.value} → {current_status.value}"
        )

        # Find all Shopee listings linked to this supplier URL
        linked_listings = self._db.get_listings_by_source_url(japan_url)
        listing_ids     = [lst["id"] for lst in linked_listings]

        alert = StockAlert(
            japan_product_url=japan_url,
            old_status=prev_status,
            new_status=current_status,
            affected_listing_ids=listing_ids,
        )

        self._handle_stock_transition(
            japan_url=japan_url,
            old_status=prev_status,
            new_status=current_status,
            linked_listings=linked_listings,
        )

        return alert

    def _handle_stock_transition(
        self,
        japan_url: str,
        old_status: StockStatus,
        new_status: StockStatus,
        linked_listings: List[Dict[str, Any]],
    ) -> None:
        """Apply protective actions based on the stock transition.

        * IN_STOCK → OUT_OF_STOCK : pause all linked active Shopee listings.
        * OUT_OF_STOCK → IN_STOCK  : log restock event for operator review
                                     (does NOT auto-resume listings).
        * Any → UNKNOWN            : log a warning, no action taken.
        """
        from src.supplier_monitor.shopee_protection import ShopeeProtection

        protection = ShopeeProtection(db=self._db)

        if new_status == StockStatus.OUT_OF_STOCK:
            for listing in linked_listings:
                protection.pause_listing(
                    listing_id=listing["id"],
                    reason=(
                        f"Supplier went out of stock: {japan_url} "
                        f"({old_status.value} → {new_status.value})"
                    ),
                    current_status=listing.get("status", ""),
                )

        elif new_status == StockStatus.IN_STOCK and old_status == StockStatus.OUT_OF_STOCK:
            # Restock: log for operator, do not auto-resume (could be a brief
            # reappearance; human review is safer before re-activating listings)
            logger.info(
                f"[InventoryMonitor] Restock detected for {japan_url} "
                f"({len(linked_listings)} listings affected). "
                f"Manual review recommended before re-activating listings."
            )
            for listing in linked_listings:
                logger.info(
                    f"[InventoryMonitor]  → listing_id={listing['id']} "
                    f"status={listing.get('status')} — NOT auto-resumed"
                )

        else:
            logger.warning(
                f"[InventoryMonitor] Unhandled stock transition "
                f"{old_status.value} → {new_status.value} for {japan_url}"
            )
