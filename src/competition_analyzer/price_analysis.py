"""Competition Analyzer AI — Price Analysis.

Pure-logic module that computes price distribution statistics from a list of
competitor prices.  No database access — accepts raw price lists and returns a
:class:`PriceDistribution` dataclass.

Statistics computed
-------------------
* ``min``     — lowest competitor price
* ``max``     — highest competitor price
* ``mean``    — arithmetic mean
* ``median``  — true median (middle value or average of two middle values)
* ``std_dev`` — population standard deviation
* ``p25``     — 25th percentile (first quartile)
* ``p75``     — 75th percentile (third quartile)
* ``iqr``     — interquartile range (p75 − p25)
* ``count``   — number of observations

Example
-------
prices = [7800, 8000, 8200, 8500]
→ min=7800, median=8100, max=8500, mean=8125, std_dev≈261
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import List, Optional, Sequence


@dataclass
class PriceDistribution:
    """Descriptive statistics for a set of competitor prices."""

    count:      int
    min_price:  float
    max_price:  float
    mean_price: float
    median_price: float
    std_dev:    float
    p25:        float
    p75:        float
    iqr:        float
    prices:     List[float] = field(default_factory=list, repr=False)

    # ── Convenience helpers ───────────────────────────────────────────────────

    @property
    def is_sufficient(self) -> bool:
        """True when we have at least 1 price (floor strategy still works)."""
        return self.count >= 1

    @property
    def spread_pct(self) -> float:
        """(max − min) / median × 100 — how spread out the market is."""
        if self.median_price <= 0:
            return 0.0
        return (self.max_price - self.min_price) / self.median_price * 100

    def percentile(self, pct: float) -> float:
        """Return an arbitrary percentile (0–100) using linear interpolation."""
        if not self.prices:
            return 0.0
        return _percentile(self.prices, pct)

    def __str__(self) -> str:
        return (
            f"PriceDistribution("
            f"n={self.count}, "
            f"min={self.min_price:.2f}, "
            f"median={self.median_price:.2f}, "
            f"max={self.max_price:.2f}, "
            f"std={self.std_dev:.2f}"
            f")"
        )


# ── Factory function ──────────────────────────────────────────────────────────

def analyse_prices(prices: Sequence[float]) -> Optional[PriceDistribution]:
    """Compute :class:`PriceDistribution` from a sequence of prices.

    Parameters
    ----------
    prices:
        Raw price values (PHP).  Non-positive values are silently discarded.

    Returns
    -------
    PriceDistribution
        Statistics for the cleaned price list.
    ``None``
        When no valid prices remain after filtering.
    """
    clean = sorted(p for p in prices if p > 0)
    if not clean:
        return None

    n       = len(clean)
    mn      = clean[0]
    mx      = clean[-1]
    mean    = sum(clean) / n
    med     = statistics.median(clean)
    std     = statistics.pstdev(clean) if n > 1 else 0.0
    p25     = _percentile(clean, 25)
    p75     = _percentile(clean, 75)
    iqr     = p75 - p25

    return PriceDistribution(
        count=n,
        min_price=round(mn, 4),
        max_price=round(mx, 4),
        mean_price=round(mean, 4),
        median_price=round(med, 4),
        std_dev=round(std, 4),
        p25=round(p25, 4),
        p75=round(p75, 4),
        iqr=round(iqr, 4),
        prices=list(clean),
    )


def analyse_prices_from_listings(listings: List[dict]) -> Optional[PriceDistribution]:
    """Convenience wrapper: extract ``competitor_price`` from a list of
    DB row dicts (as returned by ``Database.get_competitor_listings()``)
    and delegate to :func:`analyse_prices`.
    """
    prices = [
        float(row["competitor_price"])
        for row in listings
        if row.get("competitor_price") is not None
    ]
    return analyse_prices(prices)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _percentile(sorted_values: List[float], pct: float) -> float:
    """Return the *pct*-th percentile of an already-sorted list."""
    n = len(sorted_values)
    if n == 0:
        return 0.0
    if n == 1:
        return sorted_values[0]

    # Linear interpolation (same as numpy's default)
    idx  = (pct / 100.0) * (n - 1)
    low  = int(math.floor(idx))
    high = int(math.ceil(idx))
    frac = idx - low

    if high >= n:
        return sorted_values[-1]
    return sorted_values[low] + frac * (sorted_values[high] - sorted_values[low])
