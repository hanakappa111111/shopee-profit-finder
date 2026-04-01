"""
Shopee listing builder module.

Converts profit analysis results into Shopee-ready listings with AI-generated
content, pricing, images, and category classification.
"""

from typing import Optional

from src.ai.description_generator import DescriptionGenerator
from src.ai.keyword_generator import KeywordGenerator
from src.ai.title_generator import TitleGenerator
from src.config.settings import settings
from src.database.models import (
    GeneratedDescription,
    GeneratedKeywords,
    GeneratedTitles,
    JapanProduct,
    ListingStatus,
    ProfitResult,
    ShopeeListing,
)
from src.utils.logger import logger

# Category mapping: product title patterns -> Shopee category_id
CATEGORY_MAP = {
    "pokemon": 100643,
    "tcg": 100643,
    "trading card": 100643,
    "card game": 100643,
    "figure": 100639,
    "anime figure": 100639,
    "nendoroid": 100639,
    "figma": 100639,
    "figurine": 100639,
    "plush": 100650,
    "stuffed": 100650,
    "gundam": 100640,
    "model kit": 100640,
    "funko": 100641,
}

# Brand extraction patterns
BRAND_PATTERNS = {
    "pokemon": "The Pokemon Company",
    "bandai": "Bandai",
    "good smile": "Good Smile Company",
    "aniplex": "Aniplex",
}


class ListingBuilder:
    """
    Builds Shopee listings from profit analysis results.

    Handles AI content generation (title, description, keywords), pricing,
    image assembly, category mapping, and brand extraction.
    """

    def __init__(self) -> None:
        """Initialize AI generators if API key is available, otherwise warn."""
        if settings.OPENAI_API_KEY:
            self.title_generator = TitleGenerator()
            self.description_generator = DescriptionGenerator()
            self.keyword_generator = KeywordGenerator()
            logger.info("ListingBuilder initialized with AI generators")
        else:
            self.title_generator = None
            self.description_generator = None
            self.keyword_generator = None
            logger.warning(
                "OPENAI_API_KEY not set; ListingBuilder will use fallback content only"
            )

    def build(self, profit_result: ProfitResult) -> ShopeeListing:
        """
        Build a single Shopee listing from a profit analysis result.

        Args:
            profit_result: Analyzed profit opportunity with Japan and Shopee products.

        Returns:
            Complete ShopeeListing ready for upload.
        """
        shopee_product = profit_result.shopee_product
        japan_product = profit_result.japan_product

        # Generate or fallback title
        if self.title_generator:
            title_result: GeneratedTitles = self.title_generator.generate(
                japan_product.title
            )
            title = title_result.title
        else:
            title = self._fallback_title(japan_product.title)

        # Generate or fallback description
        if self.description_generator:
            desc_result: GeneratedDescription = (
                self.description_generator.generate(
                    japan_product.title, japan_product.description
                )
            )
            description = self._assemble_description(desc_result)
        else:
            description = self._fallback_description(
                japan_product.title, japan_product
            )

        # Generate or fallback keywords
        if self.keyword_generator:
            keywords_result: GeneratedKeywords = self.keyword_generator.generate(
                japan_product.title
            )
            keywords = keywords_result.keywords
        else:
            keywords = [japan_product.title.split()[0]]

        # Calculate listing price (10% buffer)
        listing_price = shopee_product.price * 1.10

        # Deduplicate images from both sources
        images = list(
            dict.fromkeys(
                shopee_product.image_url + japan_product.image_url
            )
        )

        # Guess category from title
        category_id = self._guess_category(title)

        # Extract brand from title
        brand = self._extract_brand(title)

        # Create listing
        listing = ShopeeListing(
            profit_result_id=profit_result.id,
            title=title,
            description=description,
            keywords=keywords,
            price=listing_price,
            stock=japan_product.stock,
            images=images,
            category_id=category_id,
            brand=brand,
            status=ListingStatus.DRAFT,
        )

        logger.info(
            f"Built listing for '{title}' | Price: ${listing_price:.2f} | Category: {category_id}"
        )

        return listing

    def build_many(self, results: list[ProfitResult]) -> list[ShopeeListing]:
        """
        Build multiple Shopee listings in batch.

        Args:
            results: List of profit analysis results.

        Returns:
            List of complete ShopeeListing objects.
        """
        listings = [self.build(result) for result in results]
        logger.info(f"Built {len(listings)} listings in batch")
        return listings

    def _guess_category(self, title: str) -> int:
        """
        Guess Shopee category ID from product title.

        Args:
            title: Product title.

        Returns:
            Shopee category_id (defaults to 100643).
        """
        title_lower = title.lower()
        for pattern, category_id in CATEGORY_MAP.items():
            if pattern in title_lower:
                logger.debug(f"Matched category {category_id} for pattern '{pattern}'")
                return category_id
        logger.debug("No category pattern matched; using default 100643")
        return 100643

    def _extract_brand(self, title: str) -> str:
        """
        Extract brand name from product title.

        Args:
            title: Product title.

        Returns:
            Brand name or empty string if not recognized.
        """
        title_lower = title.lower()
        for pattern, brand_name in BRAND_PATTERNS.items():
            if pattern in title_lower:
                logger.debug(f"Extracted brand '{brand_name}' from pattern '{pattern}'")
                return brand_name
        return ""

    def _fallback_title(self, title: str) -> str:
        """
        Generate fallback title when AI is unavailable.

        Args:
            title: Original product title.

        Returns:
            Cleaned and optimized fallback title.
        """
        # Simple cleanup: trim, remove extra spaces
        fallback = " ".join(title.split())
        logger.debug(f"Using fallback title: {fallback}")
        return fallback

    def _fallback_description(
        self, title: str, japan: JapanProduct
    ) -> str:
        """
        Generate fallback description when AI is unavailable.

        Args:
            title: Product title.
            japan: Japan source product data.

        Returns:
            Basic fallback description.
        """
        fallback = (
            f"Premium {title}\n\n"
            f"Details:\n{japan.description}\n\n"
            f"Stock: {japan.stock}\n"
            f"Sourced from Japan\n"
            f"Fast shipping available"
        )
        logger.debug("Using fallback description")
        return fallback

    def _assemble_description(self, desc: GeneratedDescription) -> str:
        """
        Assemble final description from generated components.

        Args:
            desc: Generated description object with sections.

        Returns:
            Formatted description string.
        """
        # Join description with original description if available
        assembled = f"{desc.description}\n\n{desc.description}"
        logger.debug("Assembled AI-generated description")
        return assembled
