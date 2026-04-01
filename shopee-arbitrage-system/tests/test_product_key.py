"""
Unit tests for the Universal Product Key Generator.

Run with:
    cd shopee-arbitrage-system
    python tests/test_product_key.py -v

No external dependencies required — uses only stdlib + the generator module.
"""

from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path

# ---------------------------------------------------------------------------
# Bootstrap: allow "from src.product_key.generator import ..." to work when
# the test is run from the project root without installing the full stack.
#
# Strategy:
#   - Add the project root to sys.path so "src" resolves as a real package.
#   - Stub only leaf deps pulled in transitively by generator.py:
#       loguru              → no-op logger object
#       src.config          → dummy module
#       src.config.settings → dummy settings object
#       src.utils           → real sub-package namespace (not replaced)
#       src.utils.logger    → no-op logger module (bypasses loguru add())
#   - We do NOT replace the real "src" package itself, which would break
#     all intra-package imports.
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


class _NoOpLogger:
    """Drop-in no-op for all loguru logger methods."""
    def __getattr__(self, name: str):   # noqa: ANN001
        return lambda *a, **kw: None


_noop = _NoOpLogger()

# ── 1. Stub loguru ────────────────────────────────────────────────────────
if "loguru" not in sys.modules:
    _loguru_mod = types.ModuleType("loguru")
    _loguru_mod.logger = _noop          # type: ignore[attr-defined]
    sys.modules["loguru"] = _loguru_mod
else:
    # loguru is installed; make its logger a no-op so setup_logger() is silent
    import loguru
    loguru.logger.__class__.__getattr__ = lambda self, n: (lambda *a, **kw: None)  # type: ignore[method-assign]

# ── 2. Stub src.config.settings ──────────────────────────────────────────
class _FakeSettings:
    LOG_LEVEL = "DEBUG"
    LOG_DIR = _PROJECT_ROOT / "logs"
    LOG_ROTATION = "1 day"
    LOG_RETENTION = "7 days"


if "src.config" not in sys.modules:
    _cfg_mod = types.ModuleType("src.config")
    sys.modules["src.config"] = _cfg_mod

if "src.config.settings" not in sys.modules:
    _settings_mod = types.ModuleType("src.config.settings")
    _settings_mod.settings = _FakeSettings()   # type: ignore[attr-defined]
    sys.modules["src.config.settings"] = _settings_mod

# ── 3. Stub src.utils.logger (skip the real logger.py entirely) ───────────
# This must be set BEFORE importing anything from src.product_key because
# generator.py does: "from src.utils.logger import logger"
if "src.utils" not in sys.modules:
    # Create a real-looking namespace without executing utils/__init__.py
    _utils_mod = types.ModuleType("src.utils")
    sys.modules["src.utils"] = _utils_mod

if "src.utils.logger" not in sys.modules:
    _logger_mod = types.ModuleType("src.utils.logger")
    _logger_mod.logger = _noop          # type: ignore[attr-defined]
    sys.modules["src.utils.logger"] = _logger_mod

# ── Now safe to import the module under test ──────────────────────────────
from src.product_key.generator import (   # noqa: E402
    ProductKeyComponents,
    ProductKeyGenerator,
)

GEN = ProductKeyGenerator()


# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────

def key_of(title: str) -> str | None:
    return GEN.generate(title).product_key


def confidence_of(title: str) -> str:
    return GEN.generate(title).confidence


# ─────────────────────────────────────────────────────────────────────────────
# 1. Brand extraction
# ─────────────────────────────────────────────────────────────────────────────

class TestBrandExtraction(unittest.TestCase):

    def test_pokemon_english(self):
        self.assertEqual(GEN.extract_brand("Pokemon Card Game SV01"), "pokemon")

    def test_pokemon_japanese(self):
        self.assertEqual(GEN.extract_brand("ポケモンカードゲーム SV01 ブースターパック"), "pokemon")

    def test_one_piece_english(self):
        self.assertEqual(GEN.extract_brand("One Piece Card Game OP01 Booster Box"), "one_piece")

    def test_one_piece_japanese(self):
        self.assertEqual(GEN.extract_brand("ワンピースカード OP-01 ブースターパック BOX"), "one_piece")

    def test_dragon_ball_english(self):
        self.assertEqual(GEN.extract_brand("Dragon Ball Super Card Game BT01"), "dragon_ball")

    def test_nendoroid(self):
        self.assertEqual(GEN.extract_brand("Nendoroid 001 Miku Hatsune"), "nendoroid")

    def test_bandai_generic(self):
        self.assertEqual(GEN.extract_brand("Bandai Figure Collection"), "bandai")

    def test_unknown_brand(self):
        self.assertIsNone(GEN.extract_brand("Mystery Box XYZ 001"))

    def test_demon_slayer_japanese(self):
        self.assertEqual(GEN.extract_brand("鬼滅の刃 フィギュア 炭治郎"), "demon_slayer")

    def test_jjk_abbreviation(self):
        self.assertEqual(GEN.extract_brand("JJK Figure Gojo Satoru"), "jujutsu_kaisen")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Model code extraction
# ─────────────────────────────────────────────────────────────────────────────

class TestModelCodeExtraction(unittest.TestCase):

    def test_op01_plain(self):
        self.assertEqual(GEN.extract_model_code("OP01 Booster Box"), "OP01")

    def test_op01_hyphenated(self):
        self.assertEqual(GEN.extract_model_code("One Piece OP-01 Booster Box"), "OP01")

    def test_sv01(self):
        self.assertEqual(GEN.extract_model_code("Pokemon SV-01 Scarlet & Violet"), "SV01")

    def test_bt01(self):
        self.assertEqual(GEN.extract_model_code("Digimon Card Game BT-01"), "BT01")

    def test_numeric_151(self):
        self.assertEqual(GEN.extract_model_code("Pokemon 151 Booster Box"), "151")

    def test_no_model_code(self):
        self.assertIsNone(GEN.extract_model_code("Random Anime Figure"))

    def test_catalogue_rg(self):
        result = GEN.extract_model_code("Gundam RG-001 Strike")
        self.assertIsNotNone(result)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Edition extraction
# ─────────────────────────────────────────────────────────────────────────────

class TestEditionExtraction(unittest.TestCase):

    def test_booster_box_english(self):
        self.assertEqual(GEN.extract_edition("OP01 Booster Box"), "booster_box")

    def test_booster_pack_japanese(self):
        self.assertEqual(GEN.extract_edition("ワンピースカード OP01 ブースターパック"), "booster_box")

    def test_starter_deck(self):
        self.assertEqual(GEN.extract_edition("One Piece Starter Deck ST01"), "starter_deck")

    def test_starter_deck_japanese(self):
        self.assertEqual(GEN.extract_edition("スターターデッキ OP01"), "starter_deck")

    def test_elite_trainer_box(self):
        self.assertEqual(GEN.extract_edition("Pokemon SV ETB Elite Trainer Box"), "elite_trainer_box")

    def test_figure(self):
        self.assertEqual(GEN.extract_edition("Nendoroid Pikachu Figure"), "figure")

    def test_plush_japanese(self):
        self.assertEqual(GEN.extract_edition("ピカチュウ ぬいぐるみ"), "plush")

    def test_no_edition(self):
        self.assertIsNone(GEN.extract_edition("Pokemon Scarlet Violet Collection"))


# ─────────────────────────────────────────────────────────────────────────────
# 4. Barcode extraction
# ─────────────────────────────────────────────────────────────────────────────

class TestBarcodeExtraction(unittest.TestCase):

    def test_ean13_in_title(self):
        self.assertEqual(
            GEN.extract_barcode("Pokemon Card 4902428123456 SV01"),
            "4902428123456",
        )

    def test_no_barcode(self):
        self.assertIsNone(GEN.extract_barcode("OP01 Booster Box"))

    def test_partial_digits_not_matched(self):
        # 12 digits → not EAN-13
        self.assertIsNone(GEN.extract_barcode("490242812345"))


# ─────────────────────────────────────────────────────────────────────────────
# 5. Product key generation — the core requirement
# ─────────────────────────────────────────────────────────────────────────────

class TestProductKeyGeneration(unittest.TestCase):
    """Verify the three canonical titles all resolve to the same product_key."""

    TITLE_A = "OP01 Booster Box"
    TITLE_B = "One Piece OP-01 Booster Box"
    TITLE_C = "ワンピースカード OP01 ブースターパック"

    def test_title_a_generates_key(self):
        self.assertIsNotNone(key_of(self.TITLE_A))

    def test_title_b_generates_key(self):
        self.assertIsNotNone(key_of(self.TITLE_B))

    def test_title_c_generates_key(self):
        self.assertIsNotNone(key_of(self.TITLE_C))

    def test_all_three_resolve_to_same_key(self):
        """THE PRIMARY INVARIANT: same product → same key regardless of language."""
        key_a = key_of(self.TITLE_A)
        key_b = key_of(self.TITLE_B)
        key_c = key_of(self.TITLE_C)
        self.assertEqual(key_a, key_b, f"A≠B:\n  A={key_a!r}\n  B={key_b!r}")
        self.assertEqual(key_b, key_c, f"B≠C:\n  B={key_b!r}\n  C={key_c!r}")

    def test_key_starts_with_pk_prefix(self):
        k = key_of(self.TITLE_B)
        self.assertTrue(k.startswith("pk:"), f"Expected 'pk:' prefix, got {k!r}")

    def test_confidence_is_high(self):
        for title in [self.TITLE_A, self.TITLE_B, self.TITLE_C]:
            with self.subTest(title=title):
                self.assertEqual(confidence_of(title), "high")

    def test_barcode_key_format(self):
        k = key_of("Pokemon Card 4902428123456 Booster Box")
        self.assertEqual(k, "barcode:4902428123456")

    def test_barcode_confidence(self):
        self.assertEqual(confidence_of("Pokemon Card 4902428123456 Booster Box"), "barcode")

    def test_barcode_overrides_brand_model(self):
        k = key_of("One Piece OP01 4902428123456 Booster Box")
        self.assertEqual(k, "barcode:4902428123456")

    def test_different_products_different_keys(self):
        k1 = key_of("One Piece OP01 Booster Box")
        k2 = key_of("One Piece OP02 Booster Box")
        self.assertNotEqual(k1, k2)

    def test_different_editions_different_keys(self):
        k1 = key_of("One Piece OP01 Booster Box")
        k2 = key_of("One Piece OP01 Starter Deck")
        self.assertNotEqual(k1, k2)

    def test_key_is_deterministic(self):
        k1 = key_of("Pokemon SV01 Booster Box")
        k2 = key_of("Pokemon SV01 Booster Box")
        self.assertEqual(k1, k2)

    def test_pokemon_sv01_cross_language(self):
        k_en = key_of("Pokemon SV01 Scarlet & Violet Booster Box")
        k_jp = key_of("ポケモンカードゲーム SV-01 ブースターパック BOX")
        self.assertEqual(k_en, k_jp, f"SV01 cross-lang:\n  EN={k_en!r}\n  JP={k_jp!r}")

    def test_medium_high_confidence_brand_model(self):
        c = confidence_of("One Piece OP03 Collection")
        self.assertEqual(c, "medium_high")

    def test_none_key_for_unrecognised(self):
        self.assertIsNone(key_of("Random collectible item"))

    def test_confidence_none_for_unrecognised(self):
        self.assertEqual(confidence_of("Random collectible item"), "none")


# ─────────────────────────────────────────────────────────────────────────────
# 6. same_product() helper
# ─────────────────────────────────────────────────────────────────────────────

class TestSameProduct(unittest.TestCase):

    def test_same_product_returns_true(self):
        self.assertTrue(GEN.same_product(
            "OP01 Booster Box",
            "One Piece OP-01 Booster Box",
        ))

    def test_different_set_returns_false(self):
        self.assertFalse(GEN.same_product(
            "One Piece OP01 Booster Box",
            "One Piece OP02 Booster Box",
        ))

    def test_low_confidence_returns_false(self):
        self.assertFalse(GEN.same_product(
            "Mystery box figure",
            "Anime figure box",
        ))

    def test_barcode_same_product(self):
        self.assertTrue(GEN.same_product(
            "Pokemon card 4902428123456",
            "4902428123456 Japanese TCG card",
        ))


# ─────────────────────────────────────────────────────────────────────────────
# 7. canonical_tokens() fuzzy input
# ─────────────────────────────────────────────────────────────────────────────

class TestCanonicalTokens(unittest.TestCase):

    def test_strips_brand(self):
        tokens = GEN.canonical_tokens("One Piece OP01 Booster Box")
        self.assertNotIn("one piece", tokens.lower())

    def test_strips_model_code(self):
        tokens = GEN.canonical_tokens("One Piece OP01 Booster Box")
        self.assertNotIn("op01", tokens.lower())

    def test_non_empty_result(self):
        tokens = GEN.canonical_tokens("Pokemon SV01 Pikachu Ex Booster Box")
        self.assertTrue(tokens.strip())

    def test_unrecognised_title_returns_something(self):
        tokens = GEN.canonical_tokens("Some random collector item 123")
        self.assertTrue(len(tokens) > 0)


# ─────────────────────────────────────────────────────────────────────────────
# 8. key_from_components() direct API
# ─────────────────────────────────────────────────────────────────────────────

class TestKeyFromComponents(unittest.TestCase):

    def test_barcode_wins(self):
        k = GEN.key_from_components(
            brand="one_piece",
            model_code="OP01",
            edition_code="booster_box",
            barcode="4902428123456",
        )
        self.assertEqual(k, "barcode:4902428123456")

    def test_two_components(self):
        k = GEN.key_from_components(brand="one_piece", model_code="OP01")
        self.assertIsNotNone(k)
        self.assertTrue(k.startswith("pk:"))

    def test_matches_title_generation(self):
        comp = GEN.generate("One Piece OP01 Booster Box")
        k_direct = GEN.key_from_components(
            brand=comp.brand,
            model_code=comp.model_code,
            edition_code=comp.edition_code,
        )
        self.assertEqual(comp.product_key, k_direct)

    def test_single_short_model_returns_none(self):
        k = GEN.key_from_components(model_code="OP")  # only 2 chars
        self.assertIsNone(k)


# ─────────────────────────────────────────────────────────────────────────────
# 9. as_dict() output shape
# ─────────────────────────────────────────────────────────────────────────────

class TestAsDict(unittest.TestCase):

    def test_keys_present(self):
        comp = GEN.generate("One Piece OP01 Booster Box")
        d = comp.as_dict()
        for field in ("brand", "model_code", "edition_code", "barcode",
                      "product_key", "confidence"):
            self.assertIn(field, d)

    def test_values_match_attributes(self):
        comp = GEN.generate("Pokemon SV01 Booster Box")
        d = comp.as_dict()
        self.assertEqual(d["brand"], comp.brand)
        self.assertEqual(d["product_key"], comp.product_key)
        self.assertEqual(d["confidence"], comp.confidence)


if __name__ == "__main__":
    unittest.main(verbosity=2)
