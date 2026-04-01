#!/usr/bin/env python3
"""CLI entry point for the Shopee Profit Research Engine.

Usage
-----
::

    # Basic research
    python run_research.py "pokemon card"

    # With options
    python run_research.py "nendoroid" --pages 3 --top 10

    # Multiple keywords
    python run_research.py "one piece card" "bandai figure" "funko pop anime"

    # Export to CSV
    python run_research.py "pokemon card" --output results.csv

    # Export to JSON file
    python run_research.py "pokemon card" --output results.json --json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from typing import List

from src.research_pipeline.pipeline import run_research_pipeline, PipelineReport
from src.utils.logger import logger


def _print_report(report: PipelineReport) -> None:
    """Print a human-readable report to stdout."""

    print()
    print("=" * 78)
    print(f"  SHOPEE PROFIT RESEARCH RESULTS — keyword: {report.keyword!r}")
    print("=" * 78)
    print(
        f"  Scraped: {report.products_scraped} products  |  "
        f"Japan sources: {report.japan_sources_found}  |  "
        f"Matches: {report.matches_found}  |  "
        f"Profitable: {report.profitable_count}"
    )
    print(f"  Elapsed: {report.elapsed_seconds:.1f}s")
    print("-" * 78)

    if not report.results:
        print("  No profitable opportunities found for this keyword.")
        print("=" * 78)
        return

    for i, r in enumerate(report.results, 1):
        print(f"\n  #{i}  {r.product_name[:65]}")
        print(f"       Shopee Price:    ₱{r.shopee_price:,.2f}")
        print(f"       Japan Price:     ¥{r.japan_supplier_price:,.0f}")
        print(f"       Est. Profit:     ¥{r.estimated_profit_jpy:,.0f}")
        print(f"       ROI:             {r.roi_percent:.1f}%")
        print(f"       Supplier:        {r.supplier_url[:70]}")
        if r.competition_price:
            print(f"       Competition:     ₱{r.competition_price:,.2f}")
        print(f"       Match:           {r.match_method} ({r.match_confidence})")
        print(f"       Source:          {r.japan_source}")

    print()
    print("=" * 78)
    print(f"  Total: {len(report.results)} profitable opportunities")
    print("=" * 78)
    print()


def _export_csv(reports: List[PipelineReport], filepath: str) -> None:
    """Export all results to a CSV file."""
    fieldnames = [
        "keyword",
        "product_name",
        "shopee_price",
        "japan_supplier_price",
        "estimated_profit_jpy",
        "roi_percent",
        "supplier_url",
        "shopee_url",
        "competition_price",
        "match_confidence",
        "match_method",
        "japan_source",
    ]

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for report in reports:
            for r in report.results:
                writer.writerow({
                    "keyword": report.keyword,
                    "product_name": r.product_name,
                    "shopee_price": r.shopee_price,
                    "japan_supplier_price": r.japan_supplier_price,
                    "estimated_profit_jpy": round(r.estimated_profit_jpy, 0),
                    "roi_percent": round(r.roi_percent, 2),
                    "supplier_url": r.supplier_url,
                    "shopee_url": r.shopee_url,
                    "competition_price": r.competition_price or "",
                    "match_confidence": r.match_confidence,
                    "match_method": r.match_method,
                    "japan_source": r.japan_source,
                })

    logger.info(f"[Export] CSV saved to {filepath}")


def _export_json(reports: List[PipelineReport], filepath: str) -> None:
    """Export all results to a JSON file."""
    output = [r.to_dict() for r in reports]
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    logger.info(f"[Export] JSON saved to {filepath}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="run_research",
        description="Shopee Profit Research Engine — on-demand keyword research",
    )
    parser.add_argument(
        "keywords",
        nargs="+",
        help="One or more Shopee search keywords to research",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=2,
        help="Max Shopee search pages to scrape per keyword (default: 2)",
    )
    parser.add_argument(
        "--max-products",
        type=int,
        default=50,
        help="Max products per keyword entering the pipeline (default: 50)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Max results per keyword (default: 20)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="output_json",
        help="Output results as JSON instead of formatted text",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="",
        help="Export results to file (.csv or .json). Example: --output results.csv",
    )

    args = parser.parse_args()

    all_reports: List[PipelineReport] = []

    for keyword in args.keywords:
        report = run_research_pipeline(
            keyword,
            max_pages=args.pages,
            max_products=args.max_products,
            top_n=args.top,
        )
        all_reports.append(report)

        if not args.output_json:
            _print_report(report)

    # ── File export ──────────────────────────────────────────────────────
    if args.output:
        ext = os.path.splitext(args.output)[1].lower()
        if ext == ".csv":
            _export_csv(all_reports, args.output)
            print(f"Results exported to {args.output}")
        elif ext == ".json":
            _export_json(all_reports, args.output)
            print(f"Results exported to {args.output}")
        else:
            # Default to CSV
            _export_csv(all_reports, args.output)
            print(f"Results exported to {args.output}")
    elif args.output_json:
        output = [r.to_dict() for r in all_reports]
        print(json.dumps(output, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
