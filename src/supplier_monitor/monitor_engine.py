"""Supplier Monitor Engine — orchestrate price and inventory monitoring jobs.

This module is the single public entry-point for the supplier monitoring
sub-system.  The scheduler calls :func:`get_monitor_engine` to obtain the
shared :class:`MonitorEngine` singleton and then invokes either
:meth:`~MonitorEngine.run_price_check` or
:meth:`~MonitorEngine.run_inventory_check` depending on which job fired.

Architecture
------------
::

    job_scheduler.py
        ├── _run_supplier_price_monitor()   → engine.run_price_check()
        └── _run_supplier_inventory_monitor() → engine.run_inventory_check()

    MonitorEngine
        ├── SupplierPriceMonitor.run()     → PriceAlert list
        └── SupplierInventoryMonitor.run() → StockAlert list

Both monitors share the same ``Database`` instance (passed at construction
time) so all operations within one job cycle share a single SQLite
connection pool.

Singleton lifecycle
-------------------
The module-level :func:`get_monitor_engine` function creates the engine
lazily on first call and caches it for the process lifetime.  Passing
``db=None`` on first call will auto-create a ``Database`` instance from
``settings.DB_PATH``.
"""

from __future__ import annotations

from typing import List, Optional, TYPE_CHECKING

from src.database.models import PriceAlert, StockAlert
from src.utils.logger import logger

if TYPE_CHECKING:
    from src.database.database import Database


class MonitorEngine:
    """Orchestrate supplier price and inventory monitoring runs.

    Parameters
    ----------
    db:
        Open :class:`~src.database.database.Database` instance shared
        across both monitors.
    """

    def __init__(self, db: "Database") -> None:
        self._db = db

    # ── Public API ────────────────────────────────────────────────────────────

    def run_price_check(self) -> List[PriceAlert]:
        """Execute one full supplier price monitoring cycle.

        Delegates to :class:`~src.supplier_monitor.price_monitor.SupplierPriceMonitor`.

        Returns
        -------
        List[PriceAlert]
            Alerts generated during this run (one per match where a
            significant price change was detected).
        """
        from src.supplier_monitor.price_monitor import SupplierPriceMonitor

        logger.info("[MonitorEngine] Starting supplier price check …")
        monitor = SupplierPriceMonitor(db=self._db)
        alerts = monitor.run()
        logger.info(
            f"[MonitorEngine] Price check complete — {len(alerts)} alert(s) generated"
        )
        return alerts

    def run_inventory_check(self) -> List[StockAlert]:
        """Execute one full supplier inventory monitoring cycle.

        Delegates to :class:`~src.supplier_monitor.inventory_monitor.SupplierInventoryMonitor`.

        Returns
        -------
        List[StockAlert]
            Alerts generated during this run (one per match where a
            stock transition was detected).
        """
        from src.supplier_monitor.inventory_monitor import SupplierInventoryMonitor

        logger.info("[MonitorEngine] Starting supplier inventory check …")
        monitor = SupplierInventoryMonitor(db=self._db)
        alerts = monitor.run()
        logger.info(
            f"[MonitorEngine] Inventory check complete — {len(alerts)} alert(s) generated"
        )
        return alerts


# ── Module-level singleton ────────────────────────────────────────────────────

_engine: Optional[MonitorEngine] = None


def get_monitor_engine(db: Optional["Database"] = None) -> MonitorEngine:
    """Return (or create) the shared :class:`MonitorEngine` singleton.

    Parameters
    ----------
    db:
        Pass an open :class:`~src.database.database.Database` on first call
        to inject a specific instance (useful in tests).  On subsequent calls
        the cached engine is returned regardless of this argument.

        If ``None`` on the very first call the engine auto-creates a
        ``Database`` from ``settings.DB_PATH``.

    Returns
    -------
    MonitorEngine
        The singleton engine instance.
    """
    global _engine

    if _engine is None:
        if db is None:
            from src.database.database import Database
            from src.config.settings import settings

            db = Database(settings.DB_PATH)

        _engine = MonitorEngine(db=db)
        logger.debug("[MonitorEngine] Singleton created")

    return _engine
