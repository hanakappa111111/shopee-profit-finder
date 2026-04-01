"""Related Product Discovery AI — DiscoveryEngine orchestrator.

``DiscoveryEngine`` ties together all three expander strategies:

* :class:`~src.related_discovery.brand_expansion.BrandExpander`
* :class:`~src.related_discovery.series_expansion.SeriesExpander`
* :class:`~src.related_discovery.keyword_expansion.KeywordExpander`

Workflow
--------
1. Fetch high-scoring ``ResearchCandidates`` (default status=``'pending'``,
   ``min_score=settings.DISCOVERY_SEED_MIN_SCORE``).
2. For each seed, run all three expanders in sequence.
3. Merge and deduplicate candidates (unique on
   ``seed_product_id + related_keyword + discovery_method``).
4. Filter by ``min_confidence``.
5. Upsert every surviving candidate to ``related_product_candidates`` via
   ``Database.upsert_related_candidate()``.

Design constraints
------------------
* **No writes to existing tables** — only ``related_product_candidates``.
* Does not import or modify ``profit_engine``, ``matching``,
  ``research_ai``, or ``listing_manager``.
* All three expanders are instantiated once per ``DiscoveryEngine`` to share
  the same DB connection and settings.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from src.config.settings import settings
from src.database.models import DiscoveryMethod, RelatedProductCandidate
from src.utils.logger import logger

if TYPE_CHECKING:
    from src.database.database import Database


class DiscoveryEngine:
    """Orchestrate related-product discovery for all research candidates.

    Parameters
    ----------
    db:
        Open :class:`~src.database.database.Database` instance.
    seed_min_score:
        Minimum ``research_score`` a candidate must have to be used as a seed.
        Defaults to ``settings.DISCOVERY_SEED_MIN_SCORE``.
    min_confidence:
        Minimum confidence for generated keywords.
        Defaults to ``settings.DISCOVERY_MIN_CONFIDENCE``.
    max_keywords_per_seed:
        Hard per-seed cap on persisted keywords.
        Defaults to ``settings.DISCOVERY_MAX_KEYWORDS_PER_SEED``.
    """

    def __init__(
        self,
        db: "Database",
        seed_min_score: Optional[float] = None,
        min_confidence: Optional[float] = None,
        max_keywords_per_seed: Optional[int] = None,
    ) -> None:
        self._db = db
        self._seed_min_score = (
            seed_min_score if seed_min_score is not None
            else settings.DISCOVERY_SEED_MIN_SCORE
        )
        self._min_conf = (
            min_confidence if min_confidence is not None
            else settings.DISCOVERY_MIN_CONFIDENCE
        )
        self._max_kw = max_keywords_per_seed or settings.DISCOVERY_MAX_KEYWORDS_PER_SEED

        # Instantiate expanders once (shared DB reference, same caps)
        from src.related_discovery.brand_expansion import BrandExpander
        from src.related_discovery.series_expansion import SeriesExpander
        from src.related_discovery.keyword_expansion import KeywordExpander

        self._brand_exp   = BrandExpander(
            db=db,
            max_keywords=self._max_kw,
            min_confidence=self._min_conf,
        )
        self._series_exp  = SeriesExpander(
            db=db,
            max_keywords=self._max_kw,
            min_confidence=self._min_conf,
        )
        self._keyword_exp = KeywordExpander(
            db=db,
            max_keywords=self._max_kw,
            min_confidence=self._min_conf,
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def run(
        self,
        seed_status: Optional[str] = "pending",
        seed_limit: int = 200,
    ) -> int:
        """Discover related keywords for all qualifying research candidates.

        Parameters
        ----------
        seed_status:
            Filter seeds by ``research_candidates.status``.  Pass ``None``
            to include all statuses.
        seed_limit:
            Maximum number of seeds to process in this run.

        Returns
        -------
        int
            Total number of ``related_product_candidates`` rows upserted.
        """
        seeds = self._db.get_research_candidates(
            status=seed_status,
            min_score=self._seed_min_score,
            limit=seed_limit,
        )

        if not seeds:
            logger.info("[DiscoveryEngine] No qualifying seeds found — nothing to do.")
            return 0

        logger.info(
            f"[DiscoveryEngine] Processing {len(seeds)} seeds "
            f"(min_score={self._seed_min_score}, status={seed_status!r})"
        )

        total_upserted = 0

        for seed in seeds:
            seed_id = seed.get("shopee_product_id") or seed.get("id")
            if seed_id is None:
                logger.warning("[DiscoveryEngine] Seed row has no usable ID — skipping")
                continue

            # Convert the joined-row dict into the shape that expanders expect
            # (they read 'id', 'title', 'keyword', 'product_key', etc.)
            seed_row = self._normalise_seed_row(seed, seed_id)

            upserted = self._process_seed(seed_row)
            total_upserted += upserted

            logger.debug(
                f"[DiscoveryEngine] seed_id={seed_id} → "
                f"{upserted} keywords upserted"
            )

        logger.info(
            f"[DiscoveryEngine] Run complete — "
            f"{total_upserted} total keywords upserted for {len(seeds)} seeds"
        )
        return total_upserted

    def get_candidates(
        self,
        method: Optional[str] = None,
        min_confidence: float = 0.0,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """Return persisted related-product candidates (read-only helper)."""
        return self._db.get_related_candidates(
            method=method,
            min_confidence=min_confidence,
            limit=limit,
        )

    def get_candidates_for_seed(
        self,
        seed_product_id: int,
        min_confidence: float = 0.0,
    ) -> List[Dict[str, Any]]:
        """Return all related-product candidates for a specific seed."""
        return self._db.get_related_candidates_for_seed(
            seed_product_id=seed_product_id,
            min_confidence=min_confidence,
        )

    def get_summary_stats(self) -> Dict[str, Any]:
        """Return a summary dict for logging / reporting."""
        stats = self._db.get_stats()
        return {
            "total_related_candidates": stats.get("total_related_candidates", 0),
            "total_research_candidates": stats.get("total_research_candidates", 0),
            "seed_min_score": self._seed_min_score,
            "min_confidence": self._min_conf,
            "max_keywords_per_seed": self._max_kw,
        }

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _process_seed(self, seed_row: Dict[str, Any]) -> int:
        """Run all expanders for one seed and upsert the results.

        Returns the number of rows upserted.
        """
        seed_id = seed_row["id"]

        # Run all three expanders
        brand_candidates   = self._brand_exp.expand(seed_row)
        series_candidates  = self._series_exp.expand(seed_row)
        keyword_candidates = self._keyword_exp.expand(seed_row)

        # Merge: deduplicate on (seed_id, keyword, method)
        merged: Dict[Tuple[int, str, str], RelatedProductCandidate] = {}

        for candidate in (*brand_candidates, *series_candidates, *keyword_candidates):
            key = (
                candidate.seed_product_id,
                candidate.related_keyword,
                candidate.discovery_method.value
                if isinstance(candidate.discovery_method, DiscoveryMethod)
                else str(candidate.discovery_method),
            )
            # Keep the highest confidence if duplicate
            if key not in merged or candidate.confidence_score > merged[key].confidence_score:
                merged[key] = candidate

        # Filter by min_confidence
        to_persist = [
            c for c in merged.values()
            if c.confidence_score >= self._min_conf
        ]

        # Respect per-seed cap (take top by confidence)
        to_persist.sort(key=lambda c: c.confidence_score, reverse=True)
        to_persist = to_persist[: self._max_kw]

        # Upsert
        upserted = 0
        for candidate in to_persist:
            try:
                self._db.upsert_related_candidate(candidate)
                upserted += 1
            except Exception as exc:
                logger.warning(
                    f"[DiscoveryEngine] Failed to upsert candidate "
                    f"seed={seed_id} kw={candidate.related_keyword!r}: {exc}"
                )

        return upserted

    @staticmethod
    def _normalise_seed_row(
        row: Dict[str, Any],
        seed_id: int,
    ) -> Dict[str, Any]:
        """Build the standard product-row dict that expanders expect.

        The joined ``get_research_candidates()`` result uses prefixed column
        names (``shopee_title``, ``shopee_url``, …).  Expanders expect the raw
        ``products`` column names (``title``, ``url``, …).
        """
        return {
            "id":                    seed_id,
            "title":                 row.get("shopee_title", row.get("title", "")),
            "price":                 row.get("shopee_price", row.get("price", 0.0)),
            "sales":                 row.get("shopee_sales", row.get("sales", 0)),
            "rating":                row.get("shopee_rating", row.get("rating", 0.0)),
            "url":                   row.get("shopee_url", row.get("url", "")),
            "market":                row.get("shopee_market", row.get("market", "PH")),
            "keyword":               row.get("shopee_keyword", row.get("keyword", "")),
            "product_key":           row.get("shopee_product_key", row.get("product_key")),
            "product_key_confidence": row.get("product_key_confidence", "none"),
            "research_score":        row.get("research_score", 0.0),
        }


# ── Module-level singleton ────────────────────────────────────────────────────
# Lazily initialised on first access so tests can import without a live DB.

_engine_instance: Optional[DiscoveryEngine] = None


def get_discovery_engine(db: Optional["Database"] = None) -> DiscoveryEngine:
    """Return the module-level singleton, creating it on first call.

    Parameters
    ----------
    db:
        Explicit DB instance.  If omitted, the shared singleton from
        ``src.database.database`` is used.
    """
    global _engine_instance
    if _engine_instance is None:
        if db is None:
            from src.database.database import db as _db
            db = _db
        _engine_instance = DiscoveryEngine(db=db)
    return _engine_instance
