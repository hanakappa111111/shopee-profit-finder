"""Competition Analyzer AI module.

Public surface
--------------
AnalyzerEngine
    Orchestrates competitor scraping, price analysis, and recommendation
    persistence.  Reads from ``profit_analysis`` / ``products``; writes to
    ``competitor_listings`` and ``price_recommendations``.

get_analyzer_engine
    Returns (and lazily creates) the module-level singleton.

CompetitorScraper
    Collects competing Shopee listings for a given product / keyword.

analyse_prices / analyse_prices_from_listings
    Pure-statistics functions returning a :class:`PriceDistribution`.

PriceDistribution
    Dataclass holding min/median/max/std_dev and related statistics.

PriceStrategyEngine
    Computes an optimal listing price from market data and cost constraints.

StrategyInput / build_strategy_input_from_profit_row
    Helpers for constructing the cost-structure input to the strategy engine.
"""

from __future__ import annotations

from src.competition_analyzer.competitor_scraper import CompetitorScraper
from src.competition_analyzer.price_analysis import (
    PriceDistribution,
    analyse_prices,
    analyse_prices_from_listings,
)
from src.competition_analyzer.price_strategy import (
    PriceStrategyEngine,
    StrategyInput,
    build_strategy_input_from_profit_row,
)
from src.competition_analyzer.analyzer_engine import (
    AnalyzerEngine,
    get_analyzer_engine,
)

__all__ = [
    # Orchestrator
    "AnalyzerEngine",
    "get_analyzer_engine",
    # Scraper
    "CompetitorScraper",
    # Analysis
    "PriceDistribution",
    "analyse_prices",
    "analyse_prices_from_listings",
    # Strategy
    "PriceStrategyEngine",
    "StrategyInput",
    "build_strategy_input_from_profit_row",
]
