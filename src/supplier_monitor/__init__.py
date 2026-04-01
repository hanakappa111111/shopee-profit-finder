"""supplier_monitor — Japan-side supplier price and inventory monitoring.

Public surface
--------------
:class:`~src.supplier_monitor.monitor_engine.MonitorEngine`
    Orchestrates price and inventory monitoring runs.

:func:`~src.supplier_monitor.monitor_engine.get_monitor_engine`
    Returns (or creates) the shared MonitorEngine singleton.

:class:`~src.supplier_monitor.price_monitor.SupplierPriceMonitor`
    Detects Japan-side price changes and triggers Shopee protection actions.

:class:`~src.supplier_monitor.inventory_monitor.SupplierInventoryMonitor`
    Detects Japan-side stock transitions and pauses linked Shopee listings.

:class:`~src.supplier_monitor.shopee_protection.ShopeeProtection`
    Applies defensive listing actions (pause, reprice, delist).
"""

from src.supplier_monitor.monitor_engine import MonitorEngine, get_monitor_engine

__all__ = [
    "MonitorEngine",
    "get_monitor_engine",
]
