"""Related Product Discovery AI — Brand Expansion strategy.

Given a seed Shopee product, BrandExpander generates search keywords for
sibling products that share the same brand.  It does this in two stages:

Stage 1 — DB-confirmed siblings
    Query the ``products`` table for other Shopee products that have the same
    canonical brand (via ``product_key_confidence`` and ``product_key`` prefix
    matching).  Each confirmed sibling contributes a high-confidence keyword
    because we already know demand exists for it on Shopee.

Stage 2 — Synthetic brand × edition cross
    For brands we recognise, generate keywords by crossing the brand name with
    each product-type in the ``BRAND_EDITION_TARGETS`` matrix.  These are
    speculative but cover product categories that may not yet be in the DB.

Confidence assignment
---------------------
    DB-confirmed sibling title (exact)     : 85
    DB-confirmed sibling keyword field     : 78
    Synthetic brand + known edition        : 62
    Synthetic brand + Japanese name        : 58

The module reads from ``products`` and ``research_candidates`` but **never
writes** to any table.  All writes are done by ``DiscoveryEngine``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from src.config.settings import settings
from src.database.models import DiscoveryMethod, RelatedProductCandidate
from src.utils.logger import logger

if TYPE_CHECKING:
    from src.database.database import Database


# ── Brand display-name catalogue ─────────────────────────────────────────────
# Maps canonical brand slug (from ProductKeyGenerator) to human-readable
# names used in keyword construction.  Both English and Japanese forms are
# stored so the Japan scraper can search in either language.

_BRAND_DISPLAY: Dict[str, Dict[str, Any]] = {
    "pokemon": {
        "en": "Pokemon",
        "ja": "ポケモン",
        "alt": ["Pokemon TCG", "Pokemon Card Game"],
    },
    "one_piece": {
        "en": "One Piece",
        "ja": "ワンピース",
        "alt": ["One Piece Card Game", "OPCG"],
    },
    "dragon_ball": {
        "en": "Dragon Ball",
        "ja": "ドラゴンボール",
        "alt": ["Dragon Ball Super Card Game", "DBS Card"],
    },
    "digimon": {
        "en": "Digimon",
        "ja": "デジモン",
        "alt": ["Digimon Card Game"],
    },
    "naruto": {
        "en": "Naruto",
        "ja": "ナルト",
        "alt": ["Naruto Card Game"],
    },
    "bandai": {
        "en": "Bandai",
        "ja": "バンダイ",
        "alt": [],
    },
    "good_smile": {
        "en": "Good Smile Company",
        "ja": "グッドスマイルカンパニー",
        "alt": ["GSC"],
    },
    "nendoroid": {
        "en": "Nendoroid",
        "ja": "ねんどろいど",
        "alt": [],
    },
    "funko": {
        "en": "Funko Pop",
        "ja": "ファンコ",
        "alt": ["Funko"],
    },
    "demon_slayer": {
        "en": "Demon Slayer",
        "ja": "鬼滅の刃",
        "alt": ["Kimetsu no Yaiba"],
    },
    "jujutsu_kaisen": {
        "en": "Jujutsu Kaisen",
        "ja": "呪術廻戦",
        "alt": ["JJK"],
    },
    "my_hero_academia": {
        "en": "My Hero Academia",
        "ja": "僕のヒーローアカデミア",
        "alt": ["BNHA", "Boku no Hero"],
    },
}

# ── Edition targets for brand × edition cross-product ────────────────────────
# For each product type, we list the English phrase to append to the brand.
# These are ordered by descending typical profit margin potential.

_EDITION_TARGETS: List[str] = [
    "Booster Box",
    "Starter Deck",
    "Elite Trainer Box",
    "Collection Box",
    "Special Set",
    "Figure",
    "Plush",
    "Tin",
    "Promo Card",
]

# Cards-focused brands should cross all TCG edition types; figure brands skip cards
_CARD_BRANDS: frozenset[str] = frozenset({
    "pokemon", "one_piece", "dragon_ball", "digimon", "naruto",
})
_FIGURE_BRANDS: frozenset[str] = frozenset({
    "bandai", "good_smile", "nendoroid", "funko",
    "demon_slayer", "jujutsu_kaisen", "my_hero_academia",
})
_FIGURE_EDITION_TARGETS: List[str] = [
    "Figure",
    "Nendoroid",
    "Plush",
    "Statue",
    "Acrylic Stand",
    "Display",
]


class BrandExpander:
    """Generate brand-sibling keywords for a seed Shopee product.

    Parameters
    ----------
    db:
        Open :class:`~src.database.database.Database` instance (read-only use).
    max_keywords:
        Hard cap on keywords produced per seed.  Defaults to
        ``settings.DISCOVERY_MAX_KEYWORDS_PER_SEED``.
    min_confidence:
        Minimum confidence to include a result.  Defaults to
        ``settings.DISCOVERY_MIN_CONFIDENCE``.
    """

    def __init__(
        self,
        db: "Database",
        max_keywords: Optional[int] = None,
        min_confidence: Optional[float] = None,
    ) -> None:
        self._db = db
        self._max = max_keywords or settings.DISCOVERY_MAX_KEYWORDS_PER_SEED
        self._min_conf = min_confidence if min_confidence is not None else settings.DISCOVERY_MIN_CONFIDENCE

    def expand(
        self,
        seed_row: Dict[str, Any],
    ) -> List[RelatedProductCandidate]:
        """Run brand expansion for one seed product row.

        Parameters
        ----------
        seed_row:
            A dict from ``Database.get_products()`` containing at minimum:
            ``id``, ``title``, ``product_key``, ``product_key_confidence``,
            ``keyword``.

        Returns
        -------
        List[RelatedProductCandidate]
            Deduplicated candidates sorted by confidence descending.
            Empty list if the brand cannot be identified.
        """
        seed_id = seed_row.get("id")
        brand   = self._extract_brand_slug(seed_row)

        if not brand:
            logger.debug(
                f"[BrandExpander] seed_id={seed_id}: "
                "brand not recognised — skipping brand expansion"
            )
            return []

        brand_info = _BRAND_DISPLAY.get(brand, {})
        brand_en   = brand_info.get("en", brand.replace("_", " ").title())

        candidates: Dict[str, RelatedProductCandidate] = {}
        now = datetime.utcnow()

        # ── Stage 1: DB-confirmed siblings ────────────────────────────────────
        db_siblings = self._find_db_siblings(seed_id, brand, seed_row)
        for kw, conf in db_siblings:
            if kw not in candidates:
                candidates[kw] = RelatedProductCandidate(
                    seed_product_id=seed_id,
                    related_keyword=kw,
                    discovery_method=DiscoveryMethod.BRAND,
                    confidence_score=conf,
                    created_at=now,
                )

        # ── Stage 2: Synthetic brand × edition ────────────────────────────────
        edition_list = (
            _FIGURE_EDITION_TARGETS if brand in _FIGURE_BRANDS else _EDITION_TARGETS
        )
        for edition in edition_list:
            kw = f"{brand_en} {edition}"
            if kw not in candidates:
                candidates[kw] = RelatedProductCandidate(
                    seed_product_id=seed_id,
                    related_keyword=kw,
                    discovery_method=DiscoveryMethod.BRAND,
                    confidence_score=62.0,
                    created_at=now,
                )

        # ── Stage 3: Japanese brand keyword ───────────────────────────────────
        brand_ja = brand_info.get("ja")
        if brand_ja:
            kw_ja = brand_ja  # Bare Japanese brand for Japan-side search
            if kw_ja not in candidates:
                candidates[kw_ja] = RelatedProductCandidate(
                    seed_product_id=seed_id,
                    related_keyword=kw_ja,
                    discovery_method=DiscoveryMethod.BRAND,
                    confidence_score=58.0,
                    created_at=now,
                )

        # ── Filter, sort, cap ─────────────────────────────────────────────────
        results = [
            c for c in candidates.values()
            if c.confidence_score >= self._min_conf
        ]
        results.sort(key=lambda c: c.confidence_score, reverse=True)
        results = results[: self._max]

        logger.debug(
            f"[BrandExpander] seed_id={seed_id} brand={brand}: "
            f"{len(results)} keywords generated"
        )
        return results

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _extract_brand_slug(self, row: Dict[str, Any]) -> Optional[str]:
        """Derive the canonical brand slug from a product row.

        Priority:
        1. ``product_key`` prefix — if the key was generated with brand info
           embedded, extract it via the product_key_generator.
        2. Keyword field — search the stored search keyword for a brand match.
        3. Title — fall back to title parsing.
        """
        try:
            from src.product_key.generator import product_key_generator
            for field_name in ("title", "keyword"):
                val = str(row.get(field_name) or "")
                if val:
                    brand = product_key_generator.extract_brand(val)
                    if brand:
                        return brand
        except Exception as exc:
            logger.warning(f"[BrandExpander] brand extraction failed: {exc}")
        return None

    def _find_db_siblings(
        self,
        seed_id: int,
        brand: str,
        seed_row: Dict[str, Any],
    ) -> List[tuple[str, float]]:
        """Return (keyword, confidence) pairs for products in the DB with the same brand."""
        results: List[tuple[str, float]] = []

        try:
            # Look for products whose keyword or title contains the brand name.
            # We use get_products() with no price filter to cast a wide net,
            # then re-filter by brand in Python.
            from src.product_key.generator import product_key_generator
            all_products = self._db.get_products(limit=1000)

            for p in all_products:
                if p.get("id") == seed_id:
                    continue  # skip the seed itself

                p_brand = None
                for field_name in ("title", "keyword"):
                    val = str(p.get(field_name) or "")
                    if val:
                        p_brand = product_key_generator.extract_brand(val)
                        if p_brand:
                            break

                if p_brand != brand:
                    continue

                # Use the title as a high-confidence keyword (it's a real product)
                title = str(p.get("title", "")).strip()
                if title and len(title) >= 5:
                    results.append((title[:120], 85.0))

                # Also use the keyword field as a medium-confidence keyword
                keyword = str(p.get("keyword", "")).strip()
                if keyword and keyword != title and len(keyword) >= 3:
                    results.append((keyword, 78.0))

        except Exception as exc:
            logger.warning(f"[BrandExpander] DB sibling lookup failed: {exc}")

        # Deduplicate keeping highest confidence per keyword
        best: Dict[str, float] = {}
        for kw, conf in results:
            if kw not in best or conf > best[kw]:
                best[kw] = conf
        return list(best.items())
