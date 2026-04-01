"""Japan Supplier Search AI — Query Builder.

Generates optimised search queries from seed product data for use in Japanese
marketplace searches.  Four query-generation strategies are applied in
priority order:

1. **Product Key** — If the seed has a barcode or high-confidence product_key,
   extract the raw model code or barcode digits directly.  These yield the
   most precise searches.

2. **Brand + Model** — Combine the canonical brand name (English and Japanese)
   with the extracted model code.  e.g. ``"One Piece OP01"``
   → ``"OP01 Booster Box"`` / ``"ワンピース OP01 ボックス"``.

3. **Normalised Title** — Strip noise words, condense the seed title into a
   compact Japanese-marketplace-friendly search string.

4. **Related Keyword passthrough** — For ``RelatedProductCandidate`` seeds the
   ``related_keyword`` field is already a search-ready phrase.

Deduplication
    All queries are lowered and stripped for comparison, but the *original
    casing* is preserved in the returned list because Japanese characters
    are case-insensitive but mixcased ASCII is significant on some sites.

The module reads from ``product_key.generator`` but **never writes** to any
table.  All DB writes are done by ``SearchEngine``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from src.config.settings import settings
from src.utils.logger import logger


# ── Brand display catalogue (mirrors related_discovery.brand_expansion) ──────
# Keep a lightweight copy so we don't force a circular import.

_BRAND_JA: Dict[str, str] = {
    "pokemon":          "ポケモン",
    "one_piece":        "ワンピース",
    "dragon_ball":      "ドラゴンボール",
    "digimon":          "デジモン",
    "naruto":           "ナルト",
    "bandai":           "バンダイ",
    "good_smile":       "グッドスマイルカンパニー",
    "nendoroid":        "ねんどろいど",
    "funko":            "ファンコ",
    "demon_slayer":     "鬼滅の刃",
    "jujutsu_kaisen":   "呪術廻戦",
    "my_hero_academia": "僕のヒーローアカデミア",
    "attack_on_titan":  "進撃の巨人",
    "spy_x_family":     "スパイファミリー",
    "kotobukiya":       "寿屋",
    "aniplex":          "アニプレックス",
}

_BRAND_EN: Dict[str, str] = {
    "pokemon":          "Pokemon",
    "one_piece":        "One Piece",
    "dragon_ball":      "Dragon Ball",
    "digimon":          "Digimon",
    "naruto":           "Naruto",
    "bandai":           "Bandai",
    "good_smile":       "Good Smile",
    "nendoroid":        "Nendoroid",
    "funko":            "Funko Pop",
    "demon_slayer":     "Demon Slayer",
    "jujutsu_kaisen":   "Jujutsu Kaisen",
    "my_hero_academia": "My Hero Academia",
    "attack_on_titan":  "Attack on Titan",
    "spy_x_family":     "Spy x Family",
    "kotobukiya":       "Kotobukiya",
    "aniplex":          "Aniplex",
}

# ── Noise words stripped from titles to produce compact queries ──────────────

_NOISE_RE = re.compile(
    r'\b('
    r'free\s*shipping|fast\s*shipping|in\s*stock|pre\s*order|'
    r'sealed|brand\s*new|official|authentic|original|new|hot|'
    r'sale|promo|limited|exclusive|best\s*seller|ready\s*stock|'
    r'cod|on\s*hand|order\s*now|shopee|philippines|from\s*japan'
    r')\b',
    re.IGNORECASE,
)

# Series-code regex (same as series_expansion.py)
_SERIES_RE = re.compile(r'\b([A-Z]{1,3})-?(\d{2,3})([a-zA-Z]?)\b', re.IGNORECASE)


@dataclass
class SearchQuery:
    """A single search query ready for marketplace submission."""

    query: str
    priority: int             # Lower = higher priority (1 is best)
    strategy: str             # "product_key" | "brand_model" | "title" | "related_keyword"
    source_seed_id: int = 0   # product ID that generated this query
    marketplace_hint: str = ""  # optional marketplace-specific variant

    def __hash__(self) -> int:
        return hash(self.query.lower().strip())

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SearchQuery):
            return NotImplemented
        return self.query.lower().strip() == other.query.lower().strip()


class QueryBuilder:
    """Generate search queries from seed product data.

    Parameters
    ----------
    max_queries:
        Hard cap on queries produced per seed.
        Defaults to ``settings.SUPPLIER_MAX_QUERIES_PER_SEED``.
    """

    def __init__(self, max_queries: Optional[int] = None) -> None:
        self._max = max_queries or settings.SUPPLIER_MAX_QUERIES_PER_SEED

    # ── Public API ────────────────────────────────────────────────────────────

    def build_from_research_candidate(
        self,
        row: Dict[str, Any],
    ) -> List[SearchQuery]:
        """Build queries from a ``research_candidates`` joined row.

        Expects keys like ``shopee_title``, ``shopee_product_key``, etc.
        """
        seed_id = row.get("shopee_product_id", row.get("id", 0))
        title   = row.get("shopee_title", row.get("title", ""))
        keyword = row.get("shopee_keyword", row.get("keyword", ""))
        pk      = row.get("shopee_product_key", row.get("product_key"))

        return self._generate(
            seed_id=seed_id,
            title=title,
            keyword=keyword,
            product_key=pk,
            related_keyword=None,
        )

    def build_from_related_candidate(
        self,
        row: Dict[str, Any],
    ) -> List[SearchQuery]:
        """Build queries from a ``related_product_candidates`` joined row.

        The ``related_keyword`` field is treated as a ready-made query at
        priority 1.  Additional queries are generated from the seed title
        if available.
        """
        seed_id = row.get("seed_product_id", 0)
        related_kw = row.get("related_keyword", "")

        # Attempt to enrich with seed product context
        title   = row.get("seed_title", row.get("title", ""))
        keyword = row.get("seed_keyword", row.get("keyword", ""))
        pk      = row.get("seed_product_key", row.get("product_key"))

        return self._generate(
            seed_id=seed_id,
            title=title,
            keyword=keyword,
            product_key=pk,
            related_keyword=related_kw,
        )

    # ── Internal query generation pipeline ────────────────────────────────────

    def _generate(
        self,
        seed_id: int,
        title: str,
        keyword: str,
        product_key: Optional[str],
        related_keyword: Optional[str],
    ) -> List[SearchQuery]:
        seen: set[str] = set()
        queries: List[SearchQuery] = []

        def _add(q: str, priority: int, strategy: str, hint: str = "") -> None:
            q = q.strip()
            key = q.lower()
            if not q or len(q) < 3 or key in seen:
                return
            seen.add(key)
            queries.append(SearchQuery(
                query=q,
                priority=priority,
                strategy=strategy,
                source_seed_id=seed_id,
                marketplace_hint=hint,
            ))

        # ── Strategy 0: Related keyword passthrough (highest priority) ────────
        if related_keyword:
            _add(related_keyword, 1, "related_keyword")

        # ── Strategy 1: Product key (barcode / model code) ────────────────────
        if product_key:
            self._queries_from_product_key(product_key, title, _add)

        # ── Strategy 2: Brand + model code ────────────────────────────────────
        brand, model = self._extract_brand_model(title, keyword)
        if brand and model:
            self._queries_from_brand_model(brand, model, title, _add)
        elif model:
            # Model code without brand
            _add(model, 3, "brand_model")

        # ── Strategy 3: Normalised title ──────────────────────────────────────
        norm = self._normalise_title(title)
        if norm and len(norm) >= 5:
            _add(norm, 4, "title")

        # ── Strategy 3b: Keyword field as-is ──────────────────────────────────
        if keyword and keyword != title:
            _add(keyword.strip(), 5, "title")

        # ── Sort by priority, cap ─────────────────────────────────────────────
        queries.sort(key=lambda q: q.priority)
        queries = queries[: self._max]

        logger.debug(
            f"[QueryBuilder] seed_id={seed_id}: "
            f"{len(queries)} queries generated"
        )
        return queries

    # ── Sub-strategies ────────────────────────────────────────────────────────

    def _queries_from_product_key(
        self,
        product_key: str,
        title: str,
        add_fn: Any,
    ) -> None:
        """Extract searchable tokens from the product_key itself."""
        if product_key.startswith("barcode:"):
            barcode = product_key.replace("barcode:", "")
            add_fn(barcode, 1, "product_key")
            return

        # For hash-based keys, we can't reverse the hash; instead extract
        # the model code from the title and use it at product_key priority.
        m = _SERIES_RE.search(title)
        if m:
            code = m.group(0)
            add_fn(code, 2, "product_key")

    def _queries_from_brand_model(
        self,
        brand: str,
        model: str,
        title: str,
        add_fn: Any,
    ) -> None:
        """Generate brand + model combinations in English and Japanese."""
        en_name = _BRAND_EN.get(brand, brand.replace("_", " ").title())
        ja_name = _BRAND_JA.get(brand)

        # English: "One Piece OP01"
        add_fn(f"{en_name} {model}", 2, "brand_model")

        # Detect edition from title for more specific queries
        edition = self._detect_edition_phrase(title)
        if edition:
            # "OP01 Booster Box"
            add_fn(f"{model} {edition}", 2, "brand_model")
            # "One Piece OP01 Booster Box" (may be long but very precise)
            full = f"{en_name} {model} {edition}"
            if len(full) <= 60:
                add_fn(full, 2, "brand_model")

        # Japanese: "ワンピース OP01"
        if ja_name:
            add_fn(f"{ja_name} {model}", 3, "brand_model")
            if edition:
                add_fn(f"{ja_name} {model} {edition}", 3, "brand_model")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _extract_brand_model(
        self,
        title: str,
        keyword: str,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Return (brand_slug, model_code) or (None, None)."""
        brand: Optional[str] = None
        model: Optional[str] = None

        try:
            from src.product_key.generator import product_key_generator
            for text in (title, keyword):
                if not text:
                    continue
                if not brand:
                    brand = product_key_generator.extract_brand(text)
                if not model:
                    m = _SERIES_RE.search(text)
                    if m:
                        model = m.group(0)
        except Exception as exc:
            logger.debug(f"[QueryBuilder] brand/model extraction failed: {exc}")

        return brand, model

    @staticmethod
    def _detect_edition_phrase(title: str) -> Optional[str]:
        """Detect a common edition phrase in *title* (English only)."""
        _EDITION_PHRASES = [
            "Booster Box", "Booster Pack", "Starter Deck", "Structure Deck",
            "Elite Trainer Box", "Collection Box", "Special Set",
            "Premium Set", "Gift Set", "Tin", "Display",
            "Figure", "Nendoroid", "Plush", "Statue", "Acrylic Stand",
        ]
        lower = title.lower()
        for phrase in _EDITION_PHRASES:
            if phrase.lower() in lower:
                return phrase
        return None

    @staticmethod
    def _normalise_title(title: str) -> str:
        """Strip noise from a Shopee title for use as a search query."""
        if not title:
            return ""
        # Remove noise marketing phrases
        cleaned = _NOISE_RE.sub(" ", title)
        # Collapse whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        # Strip trailing punctuation / dashes
        cleaned = re.sub(r'[\s\-–—,;:]+$', '', cleaned)
        # Truncate to 80 chars for search box limits
        if len(cleaned) > 80:
            cleaned = cleaned[:80].rsplit(" ", 1)[0]
            cleaned = re.sub(r'[\s\-–—,;:]+$', '', cleaned)
        return cleaned
