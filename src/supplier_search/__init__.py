"""Japan Supplier Search AI module.

Public surface
--------------
SupplierSearchEngine
    Orchestrates supplier discovery across Japanese marketplaces, reading
    seeds from ``research_candidates`` and ``related_product_candidates``,
    and persisting results to ``sources`` (JapanProducts).

get_supplier_search_engine
    Returns (and lazily creates) the module-level singleton engine.

QueryBuilder
    Generates optimised search queries from seed product data.

SearchQuery
    Dataclass representing a single search query with priority and strategy.

Marketplace adapters
    AmazonAdapter, RakutenAdapter, YahooAdapter, MercariAdapter —
    thin wrappers around existing ``japan_source`` scrapers that add
    product_key enrichment and marketplace-specific query tuning.
"""

from __future__ import annotations

from src.supplier_search.query_builder import QueryBuilder, SearchQuery
from src.supplier_search.search_engine import (
    SupplierSearchEngine,
    get_supplier_search_engine,
)
from src.supplier_search.amazon_scraper import AmazonAdapter
from src.supplier_search.rakuten_scraper import RakutenAdapter
from src.supplier_search.yahoo_scraper import YahooAdapter
from src.supplier_search.mercari_scraper import MercariAdapter

__all__ = [
    "SupplierSearchEngine",
    "get_supplier_search_engine",
    "QueryBuilder",
    "SearchQuery",
    "AmazonAdapter",
    "RakutenAdapter",
    "YahooAdapter",
    "MercariAdapter",
]
