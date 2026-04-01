"""Research AI — snapshot-based trend analysis.

This module reads data that is **already in the database** — specifically the
``product_snapshots`` and ``matches`` tables — to derive velocity and price
stability signals for Shopee products that have already passed through at
least one matching cycle.

Why a separate module?
----------------------
The existing ``market_analyzer.trend_detector.TrendDetector`` operates on raw
product row dicts and external price history lists.  It is designed as a
scraping-time component, not a database-time one.  This module fills a
different role: it answers the question *"given what we already know about
this product from historical snapshots, how strong are its trend signals?"*
without triggering any live scrapes.

Two-phase lookup
----------------
Because ``product_snapshots`` is keyed on ``sources.id`` (Japan products),
we cannot query it directly by a Shopee ``products.id``.  The lookup is:

    Shopee product_id
        → matches.japan_product_id  (from the ``matches`` table)
        → product_snapshots.product_id

Phase 1 finds all Japan product IDs that have ever been matched to this
Shopee product.  Phase 2 aggregates their snapshots over a time window.

Callers that only have the Shopee product ID use
``SnapshotTrendAnalyzer.get_snapshot_stats_for_shopee()``.
Callers that already have a list of Japan IDs use
``SnapshotTrendAnalyzer.get_snapshot_stats()``.

Output dict schema
------------------
``get_snapshot_stats()`` returns ``None`` when fewer than two distinct
captured-at timestamps exist in the window (not enough data to compute a
delta), or a dict with these keys:

    price_mean_jpy  float    Mean Japan price over the window.
    price_cv        float    Coefficient of variation of Japan price (σ / μ).
                             0 = perfectly stable; higher = more volatile.
    sales_delta     int      Increase in cumulative Shopee sales count between
                             the oldest and newest snapshot in the window.
    review_delta    int      Increase in review count over the same window.
    window_days     int      Actual window length used for the query.
    snapshot_count  int      Number of snapshot rows found in the window.
"""

from __future__ import annotations

import statistics
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from src.config.settings import settings
from src.utils.logger import logger

if TYPE_CHECKING:
    from src.database.database import Database


class SnapshotTrendAnalyzer:
    """Derive velocity and stability signals from ``product_snapshots``.

    Parameters
    ----------
    db:
        Open :class:`~src.database.database.Database` instance.
        The analyzer borrows the connection; it does not own or close it.
    window_days:
        Look-back window in days.  Defaults to
        ``settings.RESEARCH_SCORE_WINDOW_DAYS``.
    """

    def __init__(
        self,
        db: "Database",
        window_days: Optional[int] = None,
    ) -> None:
        self._db = db
        self.window_days = window_days or settings.RESEARCH_SCORE_WINDOW_DAYS

    # ── Public API ────────────────────────────────────────────────────────────

    def get_snapshot_stats_for_shopee(
        self,
        shopee_product_id: int,
    ) -> Optional[Dict[str, Any]]:
        """Return snapshot-derived stats for a Shopee product.

        Resolves Japan product IDs via the ``matches`` table, then delegates
        to :meth:`get_snapshot_stats`.

        Parameters
        ----------
        shopee_product_id:
            ``products.id`` of the Shopee product.

        Returns
        -------
        dict or None
            See module docstring for the output schema.  Returns ``None`` when
            the product has no matches yet or when the matched Japan products
            have fewer than two snapshots in the window.
        """
        japan_ids = self._get_japan_ids(shopee_product_id)
        if not japan_ids:
            logger.debug(
                f"[SnapshotTrend] shopee_id={shopee_product_id}: "
                "no matched Japan products found"
            )
            return None

        stats = self.get_snapshot_stats(japan_ids)
        if stats:
            logger.debug(
                f"[SnapshotTrend] shopee_id={shopee_product_id} "
                f"japan_ids={japan_ids} "
                f"snapshots={stats['snapshot_count']} "
                f"price_cv={stats['price_cv']:.4f} "
                f"sales_delta={stats['sales_delta']}"
            )
        return stats

    def get_snapshot_stats(
        self,
        japan_product_ids: List[int],
        window_days: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Aggregate snapshot signals across one or more Japan product IDs.

        When a Shopee product is matched to multiple Japan equivalents (e.g.
        from both Amazon JP and Rakuten), this aggregates across all of them
        to produce a single set of signals.  The price mean and CV are
        computed from all price points combined; sales/review deltas are
        summed across all Japan products.

        Parameters
        ----------
        japan_product_ids:
            List of ``sources.id`` values whose snapshots to query.
        window_days:
            Override the instance window.  Defaults to ``self.window_days``.

        Returns
        -------
        dict or None
            Returns ``None`` if the combined snapshot set has fewer than two
            distinct timestamps (impossible to compute a meaningful delta).
        """
        window = window_days or self.window_days
        if not japan_product_ids:
            return None

        all_rows = self._fetch_snapshots(japan_product_ids, window)
        if len(all_rows) < 2:
            return None

        # Ensure we have at least two distinct timestamps before computing deltas
        timestamps = sorted(set(r["captured_at"] for r in all_rows))
        if len(timestamps) < 2:
            return None

        prices        = [float(r["price_jpy"])    for r in all_rows if r.get("price_jpy") is not None]
        sales_counts  = [int(r["sales_count"])     for r in all_rows if r.get("sales_count") is not None]
        review_counts = [int(r["review_count"])    for r in all_rows if r.get("review_count") is not None]

        # ── Price statistics ──────────────────────────────────────────────────
        if len(prices) >= 2:
            price_mean = statistics.mean(prices)
            price_std  = statistics.pstdev(prices)       # population stdev
            price_cv   = (price_std / price_mean) if price_mean > 0 else 0.0
        elif len(prices) == 1:
            price_mean, price_cv = prices[0], 0.0
        else:
            price_mean, price_cv = 0.0, 0.0

        # ── Velocity signals (delta between oldest and newest snapshot) ────────
        oldest_row = min(all_rows, key=lambda r: r["captured_at"])
        newest_row = max(all_rows, key=lambda r: r["captured_at"])

        sales_delta  = (
            int(newest_row.get("sales_count",  0) or 0) -
            int(oldest_row.get("sales_count",  0) or 0)
        )
        review_delta = (
            int(newest_row.get("review_count", 0) or 0) -
            int(oldest_row.get("review_count", 0) or 0)
        )
        # Clamp to zero — negative deltas indicate data correction, not shrinkage
        sales_delta  = max(sales_delta,  0)
        review_delta = max(review_delta, 0)

        return {
            "price_mean_jpy":  round(price_mean, 2),
            "price_cv":        round(price_cv,   6),
            "sales_delta":     sales_delta,
            "review_delta":    review_delta,
            "window_days":     window,
            "snapshot_count":  len(all_rows),
        }

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _get_japan_ids(self, shopee_product_id: int) -> List[int]:
        """Return all ``sources.id`` values matched to a Shopee product."""
        with self._db.connection() as conn:
            rows = conn.execute(
                "SELECT DISTINCT japan_product_id FROM matches "
                "WHERE shopee_product_id = ?",
                [shopee_product_id],
            ).fetchall()
        return [r["japan_product_id"] for r in rows]

    def _fetch_snapshots(
        self,
        japan_ids: List[int],
        window_days: int,
    ) -> List[Dict[str, Any]]:
        """Fetch snapshot rows for a list of Japan product IDs within a window."""
        if not japan_ids:
            return []

        placeholders = ",".join("?" * len(japan_ids))
        window_str   = f"-{window_days} days"

        sql = f"""
            SELECT product_id, price_jpy, sales_count, review_count, captured_at
            FROM product_snapshots
            WHERE product_id IN ({placeholders})
              AND captured_at >= datetime('now', ?)
            ORDER BY captured_at ASC
        """
        params = japan_ids + [window_str]
        with self._db.connection() as conn:
            return [dict(r) for r in conn.execute(sql, params).fetchall()]
