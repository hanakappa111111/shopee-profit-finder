"""
Module to find related and similar products to expand search coverage.

This module provides functionality to discover related products based on
keyword expansion and similarity matching, and to generate Japanese search
queries for international sourcing.
"""

from typing import Optional
from loguru import logger
from rapidfuzz import fuzz

from src.config.settings import settings
from src.database.models import ShopeeProduct, WinningProduct
from src.utils.logger import logger as app_logger


class RelatedProductEngine:
    """
    Finds related and similar products to expand arbitrage search coverage.

    Implements keyword expansion, product similarity matching, and Japanese
    search query generation for international sourcing.
    """

    # Brand expansion mappings for common product categories
    BRAND_EXPANSIONS = {
        "pokemon": [
            "pokemon card",
            "pokemon tcg",
            "pokemon booster",
            "pikachu",
        ],
        "one piece": [
            "one piece card",
            "one piece figure",
            "op card",
        ],
        "bandai": [
            "bandai figure",
            "bandai gundam",
            "bandai toy",
        ],
    }

    # Japanese brand name mappings
    JAPANESE_MAPPINGS = {
        "pokemon": "ポケモン",
        "one piece": "ワンピース",
        "bandai": "バンダイ",
    }

    # Common noise words to filter from titles
    NOISE_WORDS = {
        "the",
        "a",
        "an",
        "with",
        "for",
        "of",
        "and",
        "or",
        "to",
        "in",
        "on",
        "at",
        "by",
        "from",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
    }

    def __init__(self) -> None:
        """Initialize the RelatedProductEngine with brand expansion mappings."""
        app_logger.info("RelatedProductEngine initialized")

    def expand_keywords(self, base_keyword: str) -> list[str]:
        """
        Expand a base keyword into related search terms.

        Checks if any known brand is in the keyword and returns related terms.
        Always includes the original keyword in results.

        Args:
            base_keyword: The base keyword to expand.

        Returns:
            List of deduplicated related search terms (max 5 including original).
        """
        expanded = {base_keyword}  # Always include original

        base_lower = base_keyword.lower()

        # Check for brand expansions
        for brand_key, related_terms in self.BRAND_EXPANSIONS.items():
            if brand_key in base_lower:
                expanded.update(related_terms)
                break

        # Convert to list and limit to 5 terms
        result = list(expanded)[: 5]

        app_logger.debug(
            f"Expanded keyword '{base_keyword}' to {len(result)} terms: {result}"
        )

        return result

    def find_related_products(
        self,
        winners: list[WinningProduct],
        all_products: list[dict],
    ) -> list[dict]:
        """
        Find related products similar to winning products.

        For each winner, searches all_products for similar items using fuzzy matching.
        Returns products not already in winners list.

        Args:
            winners: List of winning products to find related items for.
            all_products: List of all available products to search in.

        Returns:
            List of deduplicated related products not in winners.
        """
        winner_ids = {w.product_id for w in winners}
        related = {}

        for winner in winners:
            winner_title = winner.product.get("title", "").lower()

            for product in all_products:
                product_id = product.get("product_id")

                # Skip if already a winner
                if product_id in winner_ids:
                    continue

                # Skip if already in related
                if product_id in related:
                    continue

                product_title = product.get("title", "").lower()

                # Use token_set_ratio for fuzzy matching (order-invariant)
                similarity = fuzz.token_set_ratio(winner_title, product_title)

                if similarity > 65:
                    related[product_id] = product
                    app_logger.debug(
                        f"Found related product (similarity: {similarity}): "
                        f"{product.get('title', 'Unknown')}"
                    )

        result = list(related.values())
        app_logger.info(
            f"Found {len(result)} related products for {len(winners)} winners"
        )

        return result

    def generate_japan_search_queries(self, winner: WinningProduct) -> list[str]:
        """
        Generate Japanese search queries for a winning product.

        Extracts key terms from the product title, removes noise words,
        and generates search variations including Japanese brand names.

        Args:
            winner: WinningProduct to generate queries for.

        Returns:
            List of 3-5 search query strings (both English and Japanese).
        """
        title = winner.product.get("title", "")
        queries = [title]  # Always include original

        # Extract key terms and remove noise words
        words = title.lower().split()
        key_terms = [w for w in words if w not in self.NOISE_WORDS]

        # Generate brand + type variation if multiple key terms
        if len(key_terms) >= 2:
            brand_type = f"{key_terms[0]} {key_terms[1]}"
            queries.append(brand_type)

        # Check for Japanese brand mappings and add Japanese variants
        title_lower = title.lower()
        for english_brand, japanese_brand in self.JAPANESE_MAPPINGS.items():
            if english_brand in title_lower:
                # Add Japanese brand name variant
                japanese_query = title.replace(english_brand, japanese_brand)
                queries.append(japanese_query)

                # Add standalone Japanese brand with key type
                if len(key_terms) >= 1:
                    queries.append(f"{japanese_brand} {key_terms[0]}")
                break

        # Deduplicate and limit to 5
        queries = list(dict.fromkeys(queries))[: 5]

        app_logger.debug(
            f"Generated {len(queries)} Japan search queries for '{title}': {queries}"
        )

        return queries

    def suggest_related_keywords(self, winner: WinningProduct) -> list[str]:
        """
        Suggest related keywords for Japan sourcing.

        Returns a list of related search keywords that can be used to find
        similar products in Japanese markets.

        Args:
            winner: WinningProduct to suggest keywords for.

        Returns:
            List of related search keywords.
        """
        title = winner.product.get("title", "")
        keywords = []

        # Extract key terms
        words = title.lower().split()
        key_terms = [w for w in words if w not in self.NOISE_WORDS]

        # Add key terms as individual keywords
        keywords.extend(key_terms)

        # Add brand expansions
        title_lower = title.lower()
        for brand_key, related_terms in self.BRAND_EXPANSIONS.items():
            if brand_key in title_lower:
                keywords.extend(related_terms)
                break

        # Add Japanese mappings if applicable
        for english_brand, japanese_brand in self.JAPANESE_MAPPINGS.items():
            if english_brand in title_lower:
                keywords.append(japanese_brand)
                break

        # Deduplicate, remove noise, and return
        keywords = [kw for kw in dict.fromkeys(keywords) if kw not in self.NOISE_WORDS]

        app_logger.debug(
            f"Suggested {len(keywords)} related keywords for '{title}': {keywords}"
        )

        return keywords
