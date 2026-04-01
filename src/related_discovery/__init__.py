"""Related Product Discovery AI module.

Public surface
--------------
DiscoveryEngine
    Orchestrates all three expansion strategies and persists results to
    ``related_product_candidates``.

get_discovery_engine
    Returns (and lazily creates) the module-level singleton engine.

BrandExpander
    Generates brand-sibling search keywords.

SeriesExpander
    Generates sequential series-code keywords (e.g. OP01 → OP02, OP03 …).

KeywordExpander
    Generates accessory / companion keywords via edition-type affinity and
    title-token extraction.
"""

from __future__ import annotations

from src.related_discovery.brand_expansion import BrandExpander
from src.related_discovery.series_expansion import SeriesExpander
from src.related_discovery.keyword_expansion import KeywordExpander
from src.related_discovery.discovery_engine import (
    DiscoveryEngine,
    get_discovery_engine,
)

__all__ = [
    "BrandExpander",
    "SeriesExpander",
    "KeywordExpander",
    "DiscoveryEngine",
    "get_discovery_engine",
]
