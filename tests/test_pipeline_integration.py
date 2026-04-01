"""End-to-end integration tests for the on-demand research pipeline.

These tests verify that all pipeline stages connect correctly using
mock data — no real network calls, no external APIs, no Playwright.

Run with:
    python -m pytest tests/test_pipeline_integration.py -v
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database.models import (
    JapanProduct,
    JapanSource,
    MatchConfidence,
    MatchResult,
    ProfitResult,
    ShopeeProduct,
    StockStatus,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────


def _make_shopee(title: str, price: float, url: str) -> ShopeeProduct:
    return ShopeeProduct(
        title=title,
        price=price,
        currency="PHP",
        sold_count=500,
        rating=4.8,
        review_count=100,
        seller_name="TestSeller",
        product_url=url,
        image_url="https://img.test/1.jpg",
        keyword="pokemon card",
        market="PH",
    )


def _make_japan(title: str, price_jpy: float, url: str) -> JapanProduct:
    return JapanProduct(
        title=title,
        price=price_jpy,
        currency="JPY",
        image_url="https://img.test/jp1.jpg",
        product_url=url,
        source=JapanSource.AMAZON_JP,
        source_id="B000TEST01",
        stock_status=StockStatus.IN_STOCK,
        condition="new",
    )


@pytest.fixture
def shopee_products():
    return [
        _make_shopee(
            "Pokemon Card Game Scarlet Violet Booster Box",
            1500.0,
            "https://shopee.ph/product/1",
        ),
        _make_shopee(
            "One Piece Card Game OP-01 Booster",
            1200.0,
            "https://shopee.ph/product/2",
        ),
    ]


@pytest.fixture
def japan_products():
    return [
        _make_japan(
            "ポケモンカードゲーム スカーレット バイオレット ブースター",
            5500.0,
            "https://amazon.co.jp/dp/B000TEST01",
        ),
        _make_japan(
            "ワンピースカードゲーム OP-01 ブースター",
            4200.0,
            "https://amazon.co.jp/dp/B000TEST02",
        ),
    ]


# ── Test: Profit Engine ──────────────────────────────────────────────────────


class TestProfitEngine:
    """Verify profit calculation math and filtering."""

    def test_calculate_single(self, shopee_products, japan_products):
        from src.profit.profit_engine import ProfitEngine

        match = MatchResult(
            shopee_product=shopee_products[0],
            japan_product=japan_products[0],
            similarity_score=85.0,
            match_method="fuzzy_title",
            confidence_level=MatchConfidence.HIGH_FUZZY,
        )

        engine = ProfitEngine()

        with patch("src.profit.profit_engine.get_php_to_jpy_rate", return_value=2.5):
            result = engine.calculate(match)

        assert isinstance(result, ProfitResult)
        assert result.profit_jpy != 0
        assert result.roi_percent != 0
        # Revenue: 1500 * (1-0.17) * 2.5 = 3112.5 JPY
        # Cost: 5500 + 300 = 5800 JPY
        # Profit: 3112.5 - 5800 = -2687.5 (unprofitable)
        assert result.is_profitable is False

    def test_calculate_profitable_match(self):
        """Create a match that IS profitable."""
        from src.profit.profit_engine import ProfitEngine

        shopee = _make_shopee("High Price Item", 5000.0, "https://shopee.ph/high")
        japan = _make_japan("安い仕入れ品", 2000.0, "https://amazon.co.jp/dp/cheap")

        match = MatchResult(
            shopee_product=shopee,
            japan_product=japan,
            similarity_score=90.0,
            match_method="product_key",
            confidence_level=MatchConfidence.EXACT,
        )

        engine = ProfitEngine()

        with patch("src.profit.profit_engine.get_php_to_jpy_rate", return_value=2.5):
            result = engine.calculate(match)

        # Revenue: 5000 * 0.83 * 2.5 = 10375 JPY
        # Cost: 2000 + 300 = 2300 JPY
        # Profit: 10375 - 2300 = 8075 JPY
        assert result.is_profitable is True
        assert result.profit_jpy > 2000
        assert result.roi_percent > 0.30

    def test_calculate_many_and_filter(self, shopee_products, japan_products):
        from src.profit.profit_engine import ProfitEngine

        matches = [
            MatchResult(
                shopee_product=shopee_products[i],
                japan_product=japan_products[i],
                similarity_score=80.0,
                match_method="fuzzy_title",
                confidence_level=MatchConfidence.HIGH_FUZZY,
            )
            for i in range(2)
        ]

        engine = ProfitEngine()

        with patch("src.profit.profit_engine.get_php_to_jpy_rate", return_value=2.5):
            results = engine.calculate_many(matches)

        assert len(results) == 2
        assert all(isinstance(r, ProfitResult) for r in results)

        profitable = engine.filter_profitable(results)
        assert isinstance(profitable, list)


# ── Test: Product Matcher ────────────────────────────────────────────────────


class TestProductMatcher:
    """Verify matching strategies work."""

    def test_find_matches_fuzzy(self, shopee_products, japan_products):
        from src.matching.product_matcher import ProductMatcher

        matcher = ProductMatcher()
        matches = matcher.find_matches(shopee_products, japan_products)

        assert isinstance(matches, list)
        for m in matches:
            assert isinstance(m, MatchResult)
            assert m.similarity_score >= 0
            assert m.match_method

    def test_find_matches_empty_inputs(self):
        from src.matching.product_matcher import ProductMatcher

        matcher = ProductMatcher()
        assert matcher.find_matches([], []) == []

    def test_product_key_exact_match(self):
        from src.matching.product_matcher import ProductMatcher

        shopee = _make_shopee("Test Card", 1000.0, "https://shopee.ph/pk1")
        shopee.product_key = "POKEMON-SV-BOOSTER-01"

        japan = _make_japan("テストカード", 3000.0, "https://amazon.co.jp/dp/pk1")
        japan.product_key = "POKEMON-SV-BOOSTER-01"

        matcher = ProductMatcher()
        matches = matcher.find_matches([shopee], [japan])

        assert len(matches) == 1
        assert matches[0].similarity_score == 100
        assert matches[0].confidence_level == MatchConfidence.EXACT


# ── Test: Pipeline orchestrator (mocked) ─────────────────────────────────────


class TestResearchPipeline:
    """Test pipeline wiring with all external calls mocked."""

    def test_pipeline_returns_report(self, shopee_products, japan_products):
        from src.research_pipeline.pipeline import (
            PipelineReport,
            run_research_pipeline,
        )

        mock_db = MagicMock()
        mock_db.initialize = MagicMock()
        mock_db.upsert_product = MagicMock()
        mock_db.upsert_source = MagicMock()
        mock_db.upsert_match = MagicMock()
        mock_db.set_product_key = MagicMock()
        mock_db.get_product_id = MagicMock(return_value=1)

        with (
            patch("src.research_pipeline.pipeline.db", mock_db),
            patch(
                "src.research_pipeline.pipeline._scrape_keyword",
                new_callable=lambda: lambda: AsyncMock(return_value=shopee_products),
            ),
            patch(
                "src.research_pipeline.pipeline.asyncio.run",
                return_value=shopee_products,
            ),
            patch(
                "src.supplier_search.search_engine.SupplierSearchEngine"
            ) as MockSearch,
            patch(
                "src.profit.profit_engine.get_php_to_jpy_rate",
                return_value=2.5,
            ),
        ):
            mock_engine = MockSearch.return_value
            mock_engine.search_single = MagicMock(return_value=japan_products)

            report = run_research_pipeline("pokemon card", max_pages=1, top_n=5)

        assert isinstance(report, PipelineReport)
        assert report.keyword == "pokemon card"
        assert report.products_scraped == 2

    def test_pipeline_empty_scrape(self):
        from src.research_pipeline.pipeline import (
            PipelineReport,
            run_research_pipeline,
        )

        mock_db = MagicMock()
        mock_db.initialize = MagicMock()

        with (
            patch("src.research_pipeline.pipeline.db", mock_db),
            patch(
                "src.research_pipeline.pipeline.asyncio.run",
                return_value=[],
            ),
        ):
            report = run_research_pipeline("nonexistent_keyword_xyz")

        assert isinstance(report, PipelineReport)
        assert report.products_scraped == 0
        assert report.results == []


# ── Test: Scraper utilities ──────────────────────────────────────────────────


class TestScraperUtils:
    """Test shared scraper resilience utilities."""

    def test_adaptive_delay(self):
        from src.utils.scraper_utils import AdaptiveDelay

        delay = AdaptiveDelay(base_delay=0.01, max_delay=1.0, backoff_factor=2.0)
        assert delay.consecutive_failures == 0

        delay.on_failure()
        assert delay.consecutive_failures == 1

        delay.on_success()
        assert delay.consecutive_failures == 0

    def test_is_blocked_403(self):
        from src.utils.scraper_utils import is_blocked

        resp = MagicMock()
        resp.status_code = 403
        resp.content = b"Access denied"
        resp.text = "Access denied"
        assert is_blocked(resp) is True

    def test_is_blocked_captcha(self):
        from src.utils.scraper_utils import is_blocked

        resp = MagicMock()
        resp.status_code = 200
        resp.content = b"Please verify you are a human. Captcha check required."
        resp.text = "Please verify you are a human. Captcha check required."
        assert is_blocked(resp) is True

    def test_is_blocked_normal_page(self):
        from src.utils.scraper_utils import is_blocked

        resp = MagicMock()
        resp.status_code = 200
        resp.content = b"x" * 100_000  # Large page
        resp.text = "Normal product listing page content"
        assert is_blocked(resp) is False

    def test_random_ua(self):
        from src.utils.scraper_utils import random_ua

        ua = random_ua()
        assert isinstance(ua, str)
        assert "Mozilla" in ua


# ── Test: OpenAI v1 API compatibility ────────────────────────────────────────


class TestOpenAIV1:
    """Verify AI generators use v1 API correctly."""

    def test_title_generator_init(self):
        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.OPENAI_API_KEY = "sk-test"
            mock_settings.OPENAI_MODEL = "gpt-4o-mini"
            mock_settings.RETRY_MAX_ATTEMPTS = 3

            from src.ai.title_generator import TitleGenerator

            gen = TitleGenerator()
            assert hasattr(gen, "client")
            assert hasattr(gen.client, "chat")

    def test_description_generator_init(self):
        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.OPENAI_API_KEY = "sk-test"
            mock_settings.OPENAI_MODEL = "gpt-4o-mini"
            mock_settings.RETRY_MAX_ATTEMPTS = 3

            from src.ai.description_generator import DescriptionGenerator

            gen = DescriptionGenerator()
            assert hasattr(gen, "client")

    def test_keyword_generator_init(self):
        with patch("src.config.settings.settings") as mock_settings:
            mock_settings.OPENAI_API_KEY = "sk-test"
            mock_settings.OPENAI_MODEL = "gpt-4o-mini"
            mock_settings.RETRY_MAX_ATTEMPTS = 3

            from src.ai.keyword_generator import KeywordGenerator

            gen = KeywordGenerator()
            assert hasattr(gen, "client")


# ── Test: Settings validation ────────────────────────────────────────────────


class TestSettings:
    """Verify settings load and have expected defaults."""

    def test_automation_disabled_by_default(self):
        from src.config.settings import settings

        assert settings.AUTOMATION_ENABLED is False

    def test_default_model(self):
        from src.config.settings import settings

        assert settings.OPENAI_MODEL == "gpt-4o-mini"

    def test_profit_thresholds(self):
        from src.config.settings import settings

        assert settings.MIN_PROFIT_YEN == 2000.0
        assert settings.MIN_ROI == 0.30
        assert settings.SHOPEE_FEE_RATE == 0.17
