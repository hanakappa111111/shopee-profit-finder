"""
Universal Product Key Generator.

Produces a normalised, cross-platform product identity key that resolves
the same physical product across different title formats and languages.

Resolution examples
-------------------
"OP01 Booster Box"             →  pk:a3f8c2d1e4b70000  (brand=one_piece,
"One Piece OP-01 Booster Box"  →  pk:a3f8c2d1e4b70000   model=OP01,
"ワンピースカード OP01 ブースターパック"  →  pk:a3f8c2d1e4b70000   edition=booster_box)

Key hierarchy
-------------
1. Barcode (EAN-13)            → "barcode:4902428123456"   (confidence: barcode)
2. brand + model + edition     → "pk:<sha256[:16]>"         (confidence: high)
3. brand + model               → "pk:<sha256[:16]>"         (confidence: medium_high)
4. brand + edition             → "pk:<sha256[:16]>"         (confidence: medium)
5. model alone                 → "pk:<sha256[:16]>"         (confidence: low)
6. Cannot determine            → None                       (confidence: none)
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Optional

from src.utils.logger import logger


# ── Brand catalogue ───────────────────────────────────────────────────────────
# Maps every known variant (English / Japanese / abbreviation) to a canonical
# ASCII slug.  Keys are already lower-cased for matching.

_BRAND_MAP: dict[str, str] = {
    # ── Pokemon ──────────────────────────────────────────────────────────────
    "pokemon": "pokemon",
    "pokémon": "pokemon",
    "pocket monsters": "pokemon",
    "ポケモン": "pokemon",
    "ポケットモンスター": "pokemon",
    "pokemon card game": "pokemon",
    "pokemon tcg": "pokemon",
    "ptcg": "pokemon",
    "pcg": "pokemon",
    # ── One Piece ────────────────────────────────────────────────────────────
    "one piece": "one_piece",
    "onepiece": "one_piece",
    "ワンピース": "one_piece",
    "ワンピースカード": "one_piece",
    "one piece card game": "one_piece",
    "opcg": "one_piece",
    "op": "one_piece",  # only used when adjacent to a model code, e.g. "OP01"
    # ── Dragon Ball ──────────────────────────────────────────────────────────
    "dragon ball": "dragon_ball",
    "dragonball": "dragon_ball",
    "ドラゴンボール": "dragon_ball",
    "dragon ball super card game": "dragon_ball",
    "dbs": "dragon_ball",
    "dbscg": "dragon_ball",
    # ── Digimon ──────────────────────────────────────────────────────────────
    "digimon": "digimon",
    "デジモン": "digimon",
    "digimon card game": "digimon",
    # ── Naruto ───────────────────────────────────────────────────────────────
    "naruto": "naruto",
    "ナルト": "naruto",
    "naruto card game": "naruto",
    # ── Bandai (generic collectibles) ────────────────────────────────────────
    "bandai": "bandai",
    "バンダイ": "bandai",
    # ── Good Smile / Nendoroid ───────────────────────────────────────────────
    "good smile": "good_smile",
    "goodsmile": "good_smile",
    "gsc": "good_smile",
    "グッドスマイルカンパニー": "good_smile",
    "グッドスマイル": "good_smile",
    "nendoroid": "nendoroid",
    "ねんどろいど": "nendoroid",
    # ── Kotobukiya ───────────────────────────────────────────────────────────
    "kotobukiya": "kotobukiya",
    "寿屋": "kotobukiya",
    # ── Funko ────────────────────────────────────────────────────────────────
    "funko": "funko",
    "funko pop": "funko",
    # ── Aniplex ──────────────────────────────────────────────────────────────
    "aniplex": "aniplex",
    "アニプレックス": "aniplex",
    # ── My Hero Academia ─────────────────────────────────────────────────────
    "my hero academia": "my_hero_academia",
    "僕のヒーローアカデミア": "my_hero_academia",
    "ヒロアカ": "my_hero_academia",
    "bnha": "my_hero_academia",
    # ── Attack on Titan ──────────────────────────────────────────────────────
    "attack on titan": "attack_on_titan",
    "進撃の巨人": "attack_on_titan",
    "aot": "attack_on_titan",
    # ── Demon Slayer ─────────────────────────────────────────────────────────
    "demon slayer": "demon_slayer",
    "鬼滅の刃": "demon_slayer",
    "kimetsu": "demon_slayer",
    "kimetsu no yaiba": "demon_slayer",
    # ── Spy x Family ─────────────────────────────────────────────────────────
    "spy x family": "spy_x_family",
    "スパイファミリー": "spy_x_family",
    # ── Jujutsu Kaisen ───────────────────────────────────────────────────────
    "jujutsu kaisen": "jujutsu_kaisen",
    "呪術廻戦": "jujutsu_kaisen",
    "jjk": "jujutsu_kaisen",
}

# Sorted by descending length so longer phrases match before shorter substrings.
_BRAND_KEYS_SORTED: list[str] = sorted(_BRAND_MAP, key=len, reverse=True)


# ── Edition / product-type catalogue ─────────────────────────────────────────

_EDITION_MAP: dict[str, str] = {
    # Booster box
    "booster box": "booster_box",
    "booster pack box": "booster_box",
    "booster set": "booster_box",
    "ブースターパック": "booster_box",
    "ブースター box": "booster_box",
    "ブースターbox": "booster_box",
    "booster": "booster_box",
    # ── Starter / Structure deck ──────────────────────────────────────────────
    "starter deck": "starter_deck",
    "starter set": "starter_deck",
    "structure deck": "starter_deck",
    "スターターデッキ": "starter_deck",
    "スタートデッキ": "starter_deck",
    "構築済みデッキ": "starter_deck",
    # ── Elite Trainer Box ─────────────────────────────────────────────────────
    "elite trainer box": "elite_trainer_box",
    "etb": "elite_trainer_box",
    # ── Special / Premium set ─────────────────────────────────────────────────
    "special set": "special_set",
    "スペシャルセット": "special_set",
    "premium set": "premium_set",
    "プレミアムセット": "premium_set",
    "premium collection": "premium_set",
    "gift set": "gift_set",
    "ギフトセット": "gift_set",
    # ── Collection / Collector box ────────────────────────────────────────────
    "collection box": "collection_box",
    "collector box": "collection_box",
    "collectors box": "collection_box",
    "コレクターズ": "collection_box",
    "コレクション": "collection_box",
    # ── Single card ───────────────────────────────────────────────────────────
    "single card": "single_card",
    "single": "single_card",
    "シングル": "single_card",
    "1枚": "single_card",
    # ── Figure ────────────────────────────────────────────────────────────────
    "figure": "figure",
    "figures": "figure",
    "フィギュア": "figure",
    "statue": "figure",
    "statuette": "figure",
    # ── Plush ─────────────────────────────────────────────────────────────────
    "plush": "plush",
    "plushie": "plush",
    "ぬいぐるみ": "plush",
    "stuffed toy": "plush",
    # ── Display / sealed case ─────────────────────────────────────────────────
    "display": "display",
    "sealed case": "display",
    "sealed box": "display",
    # ── Promo ─────────────────────────────────────────────────────────────────
    "promo": "promo",
    "promotional": "promo",
    "プロモ": "promo",
    # ── Tin ───────────────────────────────────────────────────────────────────
    "tin": "tin",
    "缶": "tin",
    # ── Bundle ────────────────────────────────────────────────────────────────
    "bundle": "bundle",
    "バンドル": "bundle",
    # Generic fallback: bare "box" after nothing else matched
    "box": "booster_box",
}

_EDITION_KEYS_SORTED: list[str] = sorted(_EDITION_MAP, key=len, reverse=True)


# ── Model number patterns ─────────────────────────────────────────────────────
# Ordered from most specific to least specific.

_MODEL_PATTERNS: list[tuple[str, re.Pattern]] = [
    # EAN-13 barcode: exactly 13 digits
    ("barcode", re.compile(r'(?<!\d)(\d{13})(?!\d)')),
    # TCG set codes:  OP01 / OP-01 / SV-01 / BT01 / EB01 / ST01 / PRE01
    ("tcg_set", re.compile(r'\b([A-Z]{1,3}-?\d{2,3}[a-zA-Z]?)\b', re.IGNORECASE)),
    # Catalogue number: RG-001 / MG-001S / HG001
    ("catalogue", re.compile(r'\b([A-Z]{1,3}G-?\d{3,}[A-Z]?)\b', re.IGNORECASE)),
    # Pure numeric edition used in Pokemon: 151, 165, etc. (3 digits only)
    ("numeric_edition", re.compile(r'(?<!\d)(1[0-9]{2})(?!\d)')),
]


# ── Normalisation helpers ─────────────────────────────────────────────────────

def _unicode_normalise(text: str) -> str:
    """NFKC-normalise Unicode so full-width chars become ASCII equivalents."""
    return unicodedata.normalize("NFKC", text)


def _strip_noise(text: str) -> str:
    """Remove decorative brackets, pipes, slashes and collapse whitespace."""
    text = re.sub(r'[【】「」『』《》〈〉()（）\[\]{}/|\\]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def _normalise_title(title: str) -> str:
    """Full pipeline: unicode → strip noise → lowercase."""
    return _strip_noise(_unicode_normalise(title)).lower()


# ── ProductKeyComponents dataclass ────────────────────────────────────────────

@dataclass
class ProductKeyComponents:
    """Decomposed product identity components and the resulting product_key.

    Attributes:
        brand:       Canonical brand slug, e.g. "one_piece".
        model_code:  Normalised model/set code, e.g. "OP01".
        edition_code: Product type slug, e.g. "booster_box".
        barcode:     EAN-13 string if detected.
        product_key: Final key, e.g. "pk:a3f8c2d1" or "barcode:4902428…".
        confidence:  Reliability level: barcode | high | medium_high |
                     medium | low | none.
        raw_title:   Original title used to generate the key (for debugging).
    """

    brand: Optional[str] = None
    model_code: Optional[str] = None
    edition_code: Optional[str] = None
    barcode: Optional[str] = None
    product_key: Optional[str] = None
    confidence: str = "none"
    raw_title: str = ""

    def as_dict(self) -> dict:
        return {
            "brand": self.brand,
            "model_code": self.model_code,
            "edition_code": self.edition_code,
            "barcode": self.barcode,
            "product_key": self.product_key,
            "confidence": self.confidence,
        }


# ── ProductKeyGenerator ───────────────────────────────────────────────────────

class ProductKeyGenerator:
    """Generates and compares universal product keys across marketplaces.

    Usage
    -----
    gen = ProductKeyGenerator()
    components = gen.generate("One Piece OP-01 Booster Box")
    # components.product_key → "pk:a3f8c2d1e4b70000"
    # components.confidence  → "high"

    same_product = gen.same_product(title_a, title_b)
    # True if both titles resolve to the same product_key
    """

    # ── Brand extraction ──────────────────────────────────────────────────────

    @staticmethod
    def extract_brand(title: str) -> Optional[str]:
        """Return canonical brand slug from *title*, or None if unrecognised.

        Matching is done on the NFKC-normalised, lower-cased title against
        all entries in the brand catalogue sorted longest-first so that
        "one piece card game" is preferred over the substring "one piece".
        """
        norm = _normalise_title(title)
        for key in _BRAND_KEYS_SORTED:
            # Whole-word-aware search (handles Japanese strings too)
            if key in norm:
                return _BRAND_MAP[key]
        return None

    # ── Model code extraction ─────────────────────────────────────────────────

    @staticmethod
    def extract_model_code(title: str) -> Optional[str]:
        """Extract TCG set code or catalogue number from *title*.

        Returns the normalised code (uppercase, hyphen stripped) if found,
        or None.  Barcode (EAN-13) is *not* returned here — use
        `extract_barcode()` instead.
        """
        norm = _unicode_normalise(title)
        for pattern_name, pattern in _MODEL_PATTERNS:
            if pattern_name == "barcode":
                continue  # handled separately
            match = pattern.search(norm)
            if match:
                raw = match.group(1)
                # Normalise: uppercase, remove internal hyphens
                code = raw.upper().replace("-", "")
                return code
        return None

    # ── Barcode extraction ────────────────────────────────────────────────────

    @staticmethod
    def extract_barcode(title: str) -> Optional[str]:
        """Extract EAN-13 barcode from *title* if present."""
        norm = _unicode_normalise(title)
        match = _MODEL_PATTERNS[0][1].search(norm)
        return match.group(1) if match else None

    # ── Edition / product-type extraction ────────────────────────────────────

    @staticmethod
    def extract_edition(title: str) -> Optional[str]:
        """Return canonical edition/product-type slug from *title*."""
        norm = _normalise_title(title)
        for key in _EDITION_KEYS_SORTED:
            if key in norm:
                return _EDITION_MAP[key]
        return None

    # ── Hash generation ───────────────────────────────────────────────────────

    @staticmethod
    def _make_hash(components: list[str]) -> str:
        """SHA-256 of pipe-joined sorted components → first 16 hex chars."""
        payload = "|".join(sorted(filter(None, components)))
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        return "pk:" + digest[:16]

    # ── Main entry point ──────────────────────────────────────────────────────

    def generate(self, title: str) -> ProductKeyComponents:
        """Analyse *title* and produce a ``ProductKeyComponents`` instance.

        The key generation follows this priority ladder:

        1.  **Barcode** (EAN-13 found in title)
            → ``barcode:4902428123456``, confidence=barcode
        2.  **brand + model_code + edition**
            → ``pk:<hash>``, confidence=high
        3.  **brand + model_code**
            → ``pk:<hash>``, confidence=medium_high
        4.  **brand + edition**
            → ``pk:<hash>``, confidence=medium
        5.  **model_code alone** (rare; only unique TCG set codes qualify)
            → ``pk:<hash>``, confidence=low
        6.  **None** — not enough signal
            confidence=none
        """
        components = ProductKeyComponents(raw_title=title)

        components.barcode = self.extract_barcode(title)
        components.brand = self.extract_brand(title)
        components.model_code = self.extract_model_code(title)
        components.edition_code = self.extract_edition(title)

        # ── Priority 1: barcode ───────────────────────────────────────────────
        if components.barcode:
            components.product_key = f"barcode:{components.barcode}"
            components.confidence = "barcode"

        # ── Priority 2: brand + model + edition ───────────────────────────────
        elif components.brand and components.model_code and components.edition_code:
            components.product_key = self._make_hash([
                components.brand,
                components.model_code,
                components.edition_code,
            ])
            components.confidence = "high"

        # ── Priority 3: brand + model ─────────────────────────────────────────
        elif components.brand and components.model_code:
            components.product_key = self._make_hash([
                components.brand,
                components.model_code,
            ])
            components.confidence = "medium_high"

        # ── Priority 4: brand + edition ───────────────────────────────────────
        elif components.brand and components.edition_code:
            components.product_key = self._make_hash([
                components.brand,
                components.edition_code,
            ])
            components.confidence = "medium"

        # ── Priority 5: model_code alone (only high-entropy codes) ────────────
        elif components.model_code and len(components.model_code) >= 4:
            components.product_key = self._make_hash([components.model_code])
            components.confidence = "low"

        # ── Priority 6: no key possible ───────────────────────────────────────
        else:
            components.confidence = "none"

        logger.debug(
            "product_key generated",
            title=title[:60],
            key=components.product_key,
            confidence=components.confidence,
            brand=components.brand,
            model=components.model_code,
            edition=components.edition_code,
        )
        return components

    # ── Comparison helpers ────────────────────────────────────────────────────

    def same_product(self, title_a: str, title_b: str) -> bool:
        """Return True if both titles resolve to the same non-None product_key.

        Only considers keys with confidence >= medium_high.
        """
        comp_a = self.generate(title_a)
        comp_b = self.generate(title_b)
        if comp_a.product_key is None or comp_b.product_key is None:
            return False
        if comp_a.confidence in ("none", "low") or comp_b.confidence in ("none", "low"):
            return False
        return comp_a.product_key == comp_b.product_key

    def key_from_components(
        self,
        brand: Optional[str] = None,
        model_code: Optional[str] = None,
        edition_code: Optional[str] = None,
        barcode: Optional[str] = None,
    ) -> Optional[str]:
        """Generate a product_key directly from already-extracted components.

        Useful when callers have already performed extraction and only need
        the hash (e.g. when inserting into the database without a raw title).
        """
        if barcode:
            return f"barcode:{barcode}"
        parts = [c for c in [brand, model_code, edition_code] if c]
        if len(parts) >= 2:
            return self._make_hash(parts)
        if len(parts) == 1 and model_code and len(model_code) >= 4:
            return self._make_hash([model_code])
        return None

    # ── Fuzzy fallback ────────────────────────────────────────────────────────

    def canonical_tokens(self, title: str) -> str:
        """Return a compact, normalised token string for fuzzy comparison.

        Strips brand names, model codes, and edition words from the title
        so that only distinguishing tokens (e.g. character names, version
        numbers) remain.  Used as input to rapidfuzz when product_key
        matching fails.

        Example:
            "Pikachu Ex Special Collection Box" → "pikachu ex special collection"
        """
        comp = self.generate(title)
        norm = _normalise_title(title)

        # Remove brand tokens
        if comp.brand:
            # Map canonical slug back to its representative surface form for removal
            for key, val in _BRAND_MAP.items():
                if val == comp.brand:
                    norm = norm.replace(key, " ")

        # Remove model code
        if comp.model_code:
            # Remove both with and without hyphen variants
            norm = re.sub(re.escape(comp.model_code), " ", norm, flags=re.IGNORECASE)
            with_hyphen = re.sub(r'([A-Z]+)(\d+)', r'\1-\2', comp.model_code)
            norm = re.sub(re.escape(with_hyphen), " ", norm, flags=re.IGNORECASE)

        # Remove edition tokens
        if comp.edition_code:
            for key, val in _EDITION_MAP.items():
                if val == comp.edition_code:
                    norm = norm.replace(key, " ")

        # Collapse and return
        return re.sub(r'\s+', ' ', norm).strip()


# ── Module-level singleton ────────────────────────────────────────────────────

product_key_generator = ProductKeyGenerator()
