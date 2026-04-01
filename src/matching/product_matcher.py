"""
Product matching module — four-strategy pipeline.

Strategy priority (highest → lowest confidence):
  1. product_key exact match  → score 100, method "product_key",
                                 confidence EXACT
  2. EAN-13 barcode match     → score 100, method "barcode",
                                 confidence EXACT
  3. brand + model_code exact → score  95, method "brand_model",
                                 confidence BRAND_MODEL
  4. Multi-metric title fuzzy → score = max(set/sort/partial),
                                 method "title_fuzzy",
                                 confidence HIGH_FUZZY / MEDIUM_FUZZY / LOW_FUZZY

Bugs fixed vs. v1:
  - Dedup key now uses (shopee.product_url, japan.product_url) instead of the
    non-existent .id attribute on Pydantic models.
  - `find_best_match` no longer passes shopee_product=None to MatchResult.
"""

from __future__ import annotations

from typing import Optional

from rapidfuzz import fuzz, process

from src.config.settings import settings
from src.database.models import (
    JapanProduct,
    MatchConfidence,
    MatchResult,
    ShopeeProduct,
)
from src.product_key.generator import ProductKeyGenerator, product_key_generator
from src.utils.logger import logger


class ProductMatcher:
    """Matches Shopee products to Japan market products.

    Four-strategy pipeline:
      1. product_key exact match
      2. EAN-13 barcode match
      3. Brand + model_code exact match
      4. Multi-metric title fuzzy match

    Strategies are applied in priority order. Once a pair is matched by a
    higher-confidence strategy it is never re-processed by a lower strategy.

    Args:
        threshold: Minimum fuzzy similarity score (0–100) to accept a title
                   match.  Defaults to ``settings.MIN_MATCH_SIMILARITY``.
        gen:       ``ProductKeyGenerator`` instance.  Defaults to the module
                   singleton.
    """

    def __init__(
        self,
        threshold: Optional[float] = None,
        gen: Optional[ProductKeyGenerator] = None,
    ) -> None:
        self.threshold = threshold if threshold is not None else settings.MIN_MATCH_SIMILARITY
        self._gen = gen or product_key_generator

    # ─────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _pair_key(shopee: ShopeeProduct, japan: JapanProduct) -> tuple[str, str]:
        """Unique identifier for a (shopee, japan) pair using their URLs."""
        return (shopee.product_url, japan.product_url)

    @staticmethod
    def _fuzzy_score(a: str, b: str) -> float:
        """Multi-metric fuzzy score: max of token_set, token_sort, partial."""
        return max(
            fuzz.token_set_ratio(a, b),
            fuzz.token_sort_ratio(a, b),
            fuzz.partial_ratio(a, b),
        )

    @staticmethod
    def _confidence_from_fuzzy(score: float) -> MatchConfidence:
        if score >= 90:
            return MatchConfidence.HIGH_FUZZY
        if score >= 70:
            return MatchConfidence.MEDIUM_FUZZY
        return MatchConfidence.LOW_FUZZY

    def _make_match(
        self,
        shopee: ShopeeProduct,
        japan: JapanProduct,
        score: float,
        method: str,
        confidence: MatchConfidence,
    ) -> MatchResult:
        return MatchResult(
            shopee_product=shopee,
            japan_product=japan,
            similarity_score=score,
            match_method=method,
            confidence_level=confidence,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Strategy 1 — product_key exact match
    # ─────────────────────────────────────────────────────────────────────────

    def _match_by_product_key(
        self,
        shopee_products: list[ShopeeProduct],
        japan_products: list[JapanProduct],
        seen: set[tuple[str, str]],
    ) -> list[MatchResult]:
        """Match pairs that share the same non-None product_key."""
        # Build index: product_key → [JapanProduct]
        japan_index: dict[str, list[JapanProduct]] = {}
        for jp in japan_products:
            if jp.product_key:
                japan_index.setdefault(jp.product_key, []).append(jp)

        results: list[MatchResult] = []
        for sp in shopee_products:
            if not sp.product_key:
                continue
            for jp in japan_index.get(sp.product_key, []):
                key = self._pair_key(sp, jp)
                if key in seen:
                    continue
                seen.add(key)
                results.append(
                    self._make_match(sp, jp, 100.0, "product_key", MatchConfidence.EXACT)
                )
                logger.debug(
                    "product_key match",
                    key=sp.product_key,
                    shopee=sp.title[:40],
                    japan=jp.title[:40],
                )
        return results

    # ─────────────────────────────────────────────────────────────────────────
    # Strategy 2 — EAN-13 barcode match
    # ─────────────────────────────────────────────────────────────────────────

    def _match_by_barcode(
        self,
        shopee_products: list[ShopeeProduct],
        japan_products: list[JapanProduct],
        seen: set[tuple[str, str]],
    ) -> list[MatchResult]:
        """Match pairs whose titles both contain the same EAN-13 barcode."""
        # Extract barcodes from Japan products
        japan_barcodes: dict[str, list[JapanProduct]] = {}
        for jp in japan_products:
            comp = self._gen.generate(jp.title)
            if comp.barcode:
                japan_barcodes.setdefault(comp.barcode, []).append(jp)

        results: list[MatchResult] = []
        for sp in shopee_products:
            comp = self._gen.generate(sp.title)
            if not comp.barcode:
                continue
            for jp in japan_barcodes.get(comp.barcode, []):
                key = self._pair_key(sp, jp)
                if key in seen:
                    continue
                seen.add(key)
                results.append(
                    self._make_match(sp, jp, 100.0, "barcode", MatchConfidence.EXACT)
                )
                logger.debug(
                    "barcode match",
                    barcode=comp.barcode,
                    shopee=sp.title[:40],
                    japan=jp.title[:40],
                )
        return results

    # ─────────────────────────────────────────────────────────────────────────
    # Strategy 3 — brand + model_code exact match
    # ─────────────────────────────────────────────────────────────────────────

    def _match_by_brand_model(
        self,
        shopee_products: list[ShopeeProduct],
        japan_products: list[JapanProduct],
        seen: set[tuple[str, str]],
    ) -> list[MatchResult]:
        """Match pairs that share the same brand AND model_code."""
        # Pre-extract for all Japan products
        japan_components: list[tuple[JapanProduct, str | None, str | None]] = []
        for jp in japan_products:
            comp = self._gen.generate(jp.title)
            japan_components.append((jp, comp.brand, comp.model_code))

        results: list[MatchResult] = []
        for sp in shopee_products:
            sp_comp = self._gen.generate(sp.title)
            if not sp_comp.brand or not sp_comp.model_code:
                continue  # need both to use this strategy

            for jp, jp_brand, jp_model in japan_components:
                if not jp_brand or not jp_model:
                    continue
                if sp_comp.brand != jp_brand or sp_comp.model_code != jp_model:
                    continue

                key = self._pair_key(sp, jp)
                if key in seen:
                    continue
                seen.add(key)
                results.append(
                    self._make_match(sp, jp, 95.0, "brand_model", MatchConfidence.BRAND_MODEL)
                )
                logger.debug(
                    "brand_model match",
                    brand=sp_comp.brand,
                    model=sp_comp.model_code,
                    shopee=sp.title[:40],
                    japan=jp.title[:40],
                )
        return results

    # ─────────────────────────────────────────────────────────────────────────
    # Strategy 4 — multi-metric title fuzzy
    # ─────────────────────────────────────────────────────────────────────────

    def _match_by_title_fuzzy(
        self,
        shopee_products: list[ShopeeProduct],
        japan_products: list[JapanProduct],
        seen: set[tuple[str, str]],
    ) -> list[MatchResult]:
        """Rapidfuzz process.extract + multi-metric scorer for remaining pairs."""
        japan_titles = [jp.title for jp in japan_products]
        # Build URL → JapanProduct map for O(1) lookup; handle duplicate titles
        # by mapping title to a list and picking the first unused.
        japan_by_title: dict[str, list[JapanProduct]] = {}
        for jp in japan_products:
            japan_by_title.setdefault(jp.title, []).append(jp)

        results: list[MatchResult] = []
        for sp in shopee_products:
            # Use canonical_tokens for richer discriminative matching
            sp_tokens = self._gen.canonical_tokens(sp.title)
            query = sp_tokens if sp_tokens.strip() else sp.title

            extracted = process.extract(
                query,
                japan_titles,
                scorer=fuzz.token_set_ratio,
                limit=5,
            )

            for japan_title, _, _ in extracted:
                for jp in japan_by_title.get(japan_title, []):
                    key = self._pair_key(sp, jp)
                    if key in seen:
                        continue

                    # Compute full multi-metric score on original titles
                    score = self._fuzzy_score(sp.title, jp.title)
                    if score < self.threshold:
                        continue

                    seen.add(key)
                    confidence = self._confidence_from_fuzzy(score)
                    results.append(
                        self._make_match(sp, jp, score, "title_fuzzy", confidence)
                    )
                    break  # only first candidate per (sp, title) slot

        return results

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

    def find_matches(
        self,
        shopee_products: list[ShopeeProduct],
        japan_products: list[JapanProduct],
    ) -> list[MatchResult]:
        """Find all matches between two product lists using the four-strategy pipeline.

        Args:
            shopee_products: Products scraped from Shopee.
            japan_products:  Products scraped from Japanese platforms.

        Returns:
            Deduplicated list of ``MatchResult`` objects sorted by
            similarity_score descending.
        """
        seen: set[tuple[str, str]] = set()

        # Strategy 1 — product_key
        matches: list[MatchResult] = self._match_by_product_key(
            shopee_products, japan_products, seen
        )

        # Strategy 2 — barcode
        matches += self._match_by_barcode(shopee_products, japan_products, seen)

        # Strategy 3 — brand + model
        matches += self._match_by_brand_model(shopee_products, japan_products, seen)

        # Strategy 4 — title fuzzy (for everything not yet matched)
        matches += self._match_by_title_fuzzy(shopee_products, japan_products, seen)

        # Sort: EXACT first, then by similarity_score desc
        _CONF_RANK = {
            MatchConfidence.EXACT: 5,
            MatchConfidence.BRAND_MODEL: 4,
            MatchConfidence.HIGH_FUZZY: 3,
            MatchConfidence.MEDIUM_FUZZY: 2,
            MatchConfidence.LOW_FUZZY: 1,
        }
        matches.sort(
            key=lambda m: (_CONF_RANK.get(m.confidence_level, 0), m.similarity_score),
            reverse=True,
        )

        exact = sum(1 for m in matches if m.match_method in ("product_key", "barcode"))
        brand = sum(1 for m in matches if m.match_method == "brand_model")
        fuzzy = sum(1 for m in matches if m.match_method == "title_fuzzy")
        logger.info(
            f"Matching complete: {len(matches)} matches "
            f"(exact={exact}, brand_model={brand}, fuzzy={fuzzy}) "
            f"from {len(shopee_products)} Shopee × {len(japan_products)} Japan products"
        )
        return matches

    def match_pair(
        self,
        shopee: ShopeeProduct,
        japan: JapanProduct,
    ) -> Optional[MatchResult]:
        """Match a single Shopee↔Japan pair through the full pipeline.

        Args:
            shopee: Single Shopee product.
            japan:  Single Japan product.

        Returns:
            ``MatchResult`` if any strategy succeeds, else ``None``.
        """
        results = self.find_matches([shopee], [japan])
        return results[0] if results else None

    def find_best_match(
        self,
        shopee: ShopeeProduct,
        japan_products: list[JapanProduct],
    ) -> Optional[MatchResult]:
        """Return the single highest-confidence match for a Shopee product.

        Args:
            shopee:          Shopee product to match.
            japan_products:  Candidate Japan products.

        Returns:
            Best ``MatchResult`` or ``None`` if nothing passes the threshold.
        """
        if not japan_products:
            return None
        results = self.find_matches([shopee], japan_products)
        return results[0] if results else None
