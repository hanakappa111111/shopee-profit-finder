"""research_pipeline — On-demand Shopee Profit Research Engine.

This module replaces the always-on automation pipeline with a keyword-driven,
single-execution research flow.  Call :func:`run_research_pipeline` with a
search keyword to execute the full intelligence pipeline once and receive
the top profitable arbitrage opportunities.

Usage
-----
::

    from src.research_pipeline import run_research_pipeline

    results = run_research_pipeline("pokemon card")
    for r in results:
        print(f"{r['product_name']}  ROI={r['roi_percent']:.1f}%")
"""

from src.research_pipeline.pipeline import run_research_pipeline

__all__ = ["run_research_pipeline"]
