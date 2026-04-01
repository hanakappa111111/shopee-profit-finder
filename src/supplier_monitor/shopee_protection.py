"""Shopee listing protection actions for the supplier monitor.

When a Japan-side supplier event occurs (price spike, stock-out, restock),
:class:`ShopeeProtection` applies the appropriate defensive action to the
linked Shopee listings.

Design constraints
------------------
* **Idempotent / deduplicated**: each method checks ``current_status`` and
  skips the DB write if the listing is already in the target state.
* **Audit trail**: every action is logged at INFO level with the reason so
  operators can review the decision history.
* No raw SQL — all DB access goes through the :class:`~src.database.database.Database`
  methods (``get_listing_by_id``, ``update_listing``).
* ``ShopeeProtection`` takes a ``db`` constructor argument so it can be
  instantiated inside lazy imports without pulling in a module-level
  singleton (which would create circular-import risk).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.database.models import ListingStatus
from src.utils.logger import logger

if TYPE_CHECKING:
    from src.database.database import Database


# Statuses that mean the listing is already inactive — no second pause needed.
_ALREADY_INACTIVE = {
    ListingStatus.PAUSED.value,
    ListingStatus.SOLD_OUT.value,
    ListingStatus.DELETED.value,
}


class ShopeeProtection:
    """Apply protective actions to Shopee listings.

    Parameters
    ----------
    db:
        Open :class:`~src.database.database.Database` instance.
    """

    def __init__(self, db: "Database") -> None:
        self._db = db

    # ── Public actions ────────────────────────────────────────────────────────

    def pause_listing(
        self,
        listing_id: int,
        reason: str,
        current_status: str,
    ) -> bool:
        """Pause a Shopee listing (set status → PAUSED, stock → 0).

        Skips the update if the listing is already paused, sold-out, or
        deleted (idempotency guard).

        Parameters
        ----------
        listing_id:
            Primary key in the ``listings`` table.
        reason:
            Human-readable explanation written to the log (e.g. supplier OOS).
        current_status:
            The status string read from the DB row at call time.  Used to
            decide whether the action should be skipped.

        Returns
        -------
        bool
            ``True`` if the listing was actually updated, ``False`` if skipped.
        """
        if current_status in _ALREADY_INACTIVE:
            logger.debug(
                f"[ShopeeProtection] pause_listing skipped for listing_id={listing_id} "
                f"(already {current_status!r})"
            )
            return False

        try:
            self._db.update_listing(
                listing_id,
                status=ListingStatus.PAUSED.value,
                stock=0,
            )
            logger.info(
                f"[ShopeeProtection] PAUSED listing_id={listing_id} "
                f"(was {current_status!r}). Reason: {reason}"
            )
            return True

        except Exception as exc:
            logger.error(
                f"[ShopeeProtection] Failed to pause listing_id={listing_id}: {exc}",
                exc_info=True,
            )
            return False

    def adjust_price(
        self,
        listing_id: int,
        new_price: float,
        reason: str,
        current_status: str,
    ) -> bool:
        """Adjust the Shopee listing price without changing its status.

        Only acts on listings that are currently ACTIVE.  If the listing is
        paused, sold-out, or deleted the adjustment is skipped — there is no
        point repricing a listing that buyers cannot see.

        Parameters
        ----------
        listing_id:
            Primary key in the ``listings`` table.
        new_price:
            New PHP listing price (must be > 0).
        reason:
            Human-readable explanation written to the log.
        current_status:
            Status string from the DB row.

        Returns
        -------
        bool
            ``True`` if the price was updated, ``False`` if skipped.
        """
        if new_price <= 0:
            logger.warning(
                f"[ShopeeProtection] adjust_price skipped for listing_id={listing_id} "
                f"— new_price={new_price:.2f} is not positive"
            )
            return False

        if current_status != ListingStatus.ACTIVE.value:
            logger.debug(
                f"[ShopeeProtection] adjust_price skipped for listing_id={listing_id} "
                f"(status={current_status!r}, not ACTIVE)"
            )
            return False

        try:
            self._db.update_listing(listing_id, price=new_price)
            logger.info(
                f"[ShopeeProtection] PRICE ADJUSTED listing_id={listing_id} "
                f"→ ₱{new_price:.2f}. Reason: {reason}"
            )
            return True

        except Exception as exc:
            logger.error(
                f"[ShopeeProtection] Failed to adjust price for listing_id={listing_id}: {exc}",
                exc_info=True,
            )
            return False

    def delist_listing(
        self,
        listing_id: int,
        reason: str,
        current_status: str,
    ) -> bool:
        """Mark a Shopee listing as DELETED and zero its stock.

        Skips if already DELETED.  Use this for permanent supplier exits
        (e.g. product discontinued) rather than temporary OOS events —
        for temporary events prefer :meth:`pause_listing`.

        Parameters
        ----------
        listing_id:
            Primary key in the ``listings`` table.
        reason:
            Human-readable explanation written to the log.
        current_status:
            Status string from the DB row.

        Returns
        -------
        bool
            ``True`` if the listing was updated, ``False`` if skipped.
        """
        if current_status == ListingStatus.DELETED.value:
            logger.debug(
                f"[ShopeeProtection] delist_listing skipped for listing_id={listing_id} "
                f"(already DELETED)"
            )
            return False

        try:
            self._db.update_listing(
                listing_id,
                status=ListingStatus.DELETED.value,
                stock=0,
            )
            logger.info(
                f"[ShopeeProtection] DELISTED listing_id={listing_id} "
                f"(was {current_status!r}). Reason: {reason}"
            )
            return True

        except Exception as exc:
            logger.error(
                f"[ShopeeProtection] Failed to delist listing_id={listing_id}: {exc}",
                exc_info=True,
            )
            return False
