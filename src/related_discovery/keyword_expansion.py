"""Related Product Discovery AI — Keyword Expansion strategy.

``KeywordExpander`` generates search keywords for *accessory* or *companion*
products that are commonly bought alongside the seed product.

Two complementary techniques are used:

Edition-type affinity matrix
    Map the seed's edition type (detected from its title/keyword) to a list
    of related product types that are commonly purchased together.
    Example: a "Booster Box" seed → suggest "Sleeve", "Playmat", "Deck Box".
    Confidence depends on affinity strength: strong = 80, moderate = 65.

Title-token extraction
    Strip generic words, brand names and model codes from the seed title;
    keep meaningful noun tokens.  Each token (or short phrase) becomes a
    standalone Japan-side search keyword.
    Confidence: 70 for 2+-token phrases, 55 for single tokens.

The module reads from ``products`` for context but **never writes** — all
writes are done by ``DiscoveryEngine``.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, FrozenSet, List, Optional, Tuple, TYPE_CHECKING

from src.config.settings import settings
from src.database.models import DiscoveryMethod, RelatedProductCandidate
from src.utils.logger import logger

if TYPE_CHECKING:
    from src.database.database import Database


# ── Edition-type affinity matrix ─────────────────────────────────────────────
# Maps a canonical edition slug (from product_key_generator) to a list of
# (keyword_template, confidence) pairs.  Templates may include the brand name
# via the placeholder ``{brand_en}``.

_AFFINITY: Dict[str, List[Tuple[str, float]]] = {
    "booster_box": [
        ("Card Sleeve",          80.0),
        ("Deck Box",             80.0),
        ("Playmat",              75.0),
        ("Card Binder",          75.0),
        ("Starter Deck",         65.0),
        ("Elite Trainer Box",    65.0),
    ],
    "starter_deck": [
        ("Booster Box",          80.0),
        ("Card Sleeve",          75.0),
        ("Deck Box",             75.0),
        ("Card Binder",          70.0),
        ("Playmat",              65.0),
    ],
    "elite_trainer_box": [
        ("Booster Box",          80.0),
        ("Card Sleeve",          75.0),
        ("Deck Box",             70.0),
        ("Playmat",              70.0),
        ("Card Binder",          65.0),
    ],
    "collection_box": [
        ("Booster Box",          75.0),
        ("Card Sleeve",          70.0),
        ("Figure",               65.0),
    ],
    "special_set": [
        ("Booster Box",          75.0),
        ("Card Sleeve",          70.0),
        ("Deck Box",             65.0),
    ],
    "premium_set": [
        ("Booster Box",          75.0),
        ("Card Sleeve",          70.0),
        ("Figure",               65.0),
    ],
    "figure": [
        ("Nendoroid",            75.0),
        ("Plush",                70.0),
        ("Acrylic Stand",        65.0),
        ("Display Stand",        65.0),
        ("Statue",               65.0),
    ],
    "plush": [
        ("Figure",               75.0),
        ("Nendoroid",            65.0),
        ("Plush",                65.0),   # different character of same brand
    ],
    "tin": [
        ("Booster Box",          75.0),
        ("Card Sleeve",          70.0),
        ("Deck Box",             65.0),
    ],
    "display": [
        ("Booster Box",          80.0),
        ("Card Sleeve",          75.0),
        ("Deck Box",             65.0),
    ],
    "promo": [
        ("Booster Box",          70.0),
        ("Card Sleeve",          65.0),
        ("Deck Box",             60.0),
    ],
    "booster_box_generic": [       # fallback when edition is just "box"
        ("Card Sleeve",          70.0),
        ("Deck Box",             70.0),
        ("Playmat",              65.0),
    ],
}

# ── Stop-words for token extraction ──────────────────────────────────────────
# These words are stripped before generating token-based keywords.

_STOP_WORDS: FrozenSet[str] = frozenset({
    # English generic
    "the", "a", "an", "of", "for", "and", "or", "in", "on", "at", "by",
    "with", "to", "from", "is", "it", "be", "as", "are", "was",
    # Common TCG / collectible words that add no discriminating signal
    "card", "cards", "game", "trading", "collectible", "official", "new",
    "sealed", "japanese", "japan", "english", "set", "pack", "box",
    "booster", "starter", "deck", "elite", "trainer", "collection",
    "figure", "toy", "item", "product", "lot", "bundle", "version",
    # Units / formatting
    "1", "2", "3", "4", "5", "6", "x", "pcs", "pieces",
})

# Regex to identify a series/model code token (skip in token extraction)
_MODEL_CODE_RE = re.compile(r'\b[A-Z]{1,3}-?\d{2,3}[a-zA-Z]?\b', re.IGNORECASE)

# Max token length (characters) to include as a standalone search term
_MAX_TOKEN_LEN: int = 40
_MIN_TOKEN_LEN: int = 3


class KeywordExpander:
    """Generate accessory / companion keywords for a seed Shopee product.

    Parameters
    ----------
    db:
        Open :class:`~src.database.database.Database` instance (read-only use).
    max_keywords:
        Hard cap on candidates per seed.
        Defaults to ``settings.DISCOVERY_MAX_KEYWORDS_PER_SEED``.
    min_confidence:
        Minimum confidence to include.
        Defaults to ``settings.DISCOVERY_MIN_CONFIDENCE``.
    """

    def __init__(
        self,
        db: "Database",
        max_keywords: Optional[int] = None,
        min_confidence: Optional[float] = None,
    ) -> None:
        self._db       = db
        self._max      = max_keywords or settings.DISCOVERY_MAX_KEYWORDS_PER_SEED
        self._min_conf = (
            min_confidence if min_confidence is not None
            else settings.DISCOVERY_MIN_CONFIDENCE
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def expand(self, seed_row: Dict[str, Any]) -> List[RelatedProductCandidate]:
        """Run keyword expansion for one seed product row.

        Parameters
        ----------
        seed_row:
            Dict from ``Database.get_products()`` containing at minimum:
            ``id``, ``title``, ``keyword``, ``product_key``,
            ``product_key_confidence``.

        Returns
        -------
        List[RelatedProductCandidate]
            Deduplicated candidates sorted by confidence descending.
        """
        seed_id = seed_row.get("id")
        now     = datetime.utcnow()

        candidates: Dict[str, RelatedProductCandidate] = {}

        # ── Technique 1: affinity-based accessory keywords ────────────────────
        affinity_pairs = self._affinity_keywords(seed_row)
        for kw, conf in affinity_pairs:
            if conf >= self._min_conf and kw not in candidates:
                candidates[kw] = RelatedProductCandidate(
                    seed_product_id=seed_id,
                    related_keyword=kw,
                    discovery_method=DiscoveryMethod.KEYWORD,
                    confidence_score=conf,
                    created_at=now,
                )

        # ── Technique 2: title-token extraction ───────────────────────────────
        token_pairs = self._token_keywords(seed_row)
        for kw, conf in token_pairs:
            if conf >= self._min_conf and kw not in candidates:
                candidates[kw] = RelatedProductCandidate(
                    seed_product_id=seed_id,
                    related_keyword=kw,
                    discovery_method=DiscoveryMethod.KEYWORD,
                    confidence_score=conf,
                    created_at=now,
                )

        # ── Sort, cap, return ─────────────────────────────────────────────────
        results = sorted(
            candidates.values(),
            key=lambda c: c.confidence_score,
            reverse=True,
        )[: self._max]

        logger.debug(
            f"[KeywordExpander] seed_id={seed_id}: "
            f"{len(results)} keywords generated"
        )
        return results

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _affinity_keywords(
        self,
        row: Dict[str, Any],
    ) -> List[Tuple[str, float]]:
        """Detect the seed's edition type and return affinity pairs.

        Affinity keywords are optionally prefixed with the brand name if we
        can identify it, making the Japan-side search more precise.
        """
        edition_slug = self._detect_edition(row)
        if not edition_slug:
            return []

        affinity_list = _AFFINITY.get(edition_slug, [])
        if not affinity_list:
            return []

        # Try to prepend the English brand name for extra precision
        brand_prefix = self._detect_brand_en(row)

        results: List[Tuple[str, float]] = []
        for template, conf in affinity_list:
            kw = f"{brand_prefix} {template}".strip() if brand_prefix else template
            results.append((kw, conf))

        return results

    def _token_keywords(
        self,
        row: Dict[str, Any],
    ) -> List[Tuple[str, float]]:
        """Extract meaningful tokens from title / keyword fields."""
        results: List[Tuple[str, float]] = []

        for field_name in ("title", "keyword"):
            text = str(row.get(field_name) or "")
            if not text:
                continue

            tokens = self._tokenise(text)
            if not tokens:
                continue

            # Multi-token phrase (2–3 words): higher confidence
            if len(tokens) >= 2:
                phrase = " ".join(tokens[:3])
                if _MIN_TOKEN_LEN <= len(phrase) <= _MAX_TOKEN_LEN:
                    results.append((phrase, 70.0))

            # Individual tokens: lower confidence
            for tok in tokens:
                if _MIN_TOKEN_LEN <= len(tok) <= _MAX_TOKEN_LEN:
                    results.append((tok, 55.0))

        # Deduplicate keeping highest confidence
        best: Dict[str, float] = {}
        for kw, conf in results:
            if kw not in best or conf > best[kw]:
                best[kw] = conf
        return list(best.items())

    # ── Sub-helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _detect_edition(row: Dict[str, Any]) -> Optional[str]:
        """Return a canonical edition slug or ``None``."""
        try:
            from src.product_key.generator import product_key_generator
            for field_name in ("title", "keyword"):
                val = str(row.get(field_name) or "")
                if val:
                    result = product_key_generator.generate(val)
                    if result and result.edition_code:
                        return result.edition_code
        except Exception as exc:
            logger.debug(f"[KeywordExpander] edition detection failed: {exc}")
        return None

    @staticmethod
    def _detect_brand_en(row: Dict[str, Any]) -> Optional[str]:
        """Return a short English brand name for use as a keyword prefix."""
        # Import lazily to avoid circular imports at load time
        try:
            from src.product_key.generator import product_key_generator
            from src.related_discovery.brand_expansion import _BRAND_DISPLAY
            for field_name in ("title", "keyword"):
                val = str(row.get(field_name) or "")
                if val:
                    slug = product_key_generator.extract_brand(val)
                    if slug:
                        info = _BRAND_DISPLAY.get(slug)
                        if info:
                            return info.get("en", "")
        except Exception as exc:
            logger.debug(f"[KeywordExpander] brand-en detection failed: {exc}")
        return None

    @staticmethod
    def _tokenise(text: str) -> List[str]:
        """Tokenise *text* for keyword extraction.

        Steps:
        1. Remove model codes (e.g. "OP01", "SV-04").
        2. Lower-case and split on non-alpha/non-digit characters.
        3. Filter stop-words and very short/long tokens.
        4. Return at most 6 meaningful tokens.
        """
        # Remove model codes first
        cleaned = _MODEL_CODE_RE.sub(" ", text)

        # Lower-case; split on anything that's not alphanumeric or a space
        tokens_raw = re.split(r'[^a-zA-Z0-9\s]', cleaned.lower())
        tokens_flat = []
        for part in tokens_raw:
            tokens_flat.extend(part.split())

        # Filter
        result = [
            t for t in tokens_flat
            if (
                len(t) >= _MIN_TOKEN_LEN
                and len(t) <= _MAX_TOKEN_LEN
                and t not in _STOP_WORDS
                and not t.isdigit()
            )
        ]
        return result[:6]
