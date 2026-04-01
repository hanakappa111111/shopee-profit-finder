"""AI-powered SEO keyword generation for Shopee listings.

Generates optimized keywords, hashtags, and search tags for Japanese anime
collectibles and TCG products using OpenAI API.
"""

import json
import time
from typing import Optional
import openai

from src.config.settings import settings
from src.database.models import GeneratedKeywords
from src.utils.logger import logger


class KeywordGenerator:
    """Generates optimized SEO keywords and hashtags using AI.

    Attributes:
        api_key: OpenAI API key from settings.
        model: Model name (e.g., "gpt-4" or "gpt-3.5-turbo").
        retry_max: Maximum retry attempts for API errors.
    """

    SYSTEM_PROMPT = (
        "You are an expert SEO specialist for Shopee, specializing in anime "
        "and trading card game (TCG) products. Generate high-quality keywords, "
        "hashtags, and search tags that maximize product discoverability.\n"
        "Requirements:\n"
        "- keywords: 10-15 relevant SEO keywords\n"
        "- hashtags: 5-8 hashtags (must start with #)\n"
        "- search_tags: 5-10 short, focused search terms\n"
        "- Include brand names, product types, conditions, anime/TCG keywords\n"
        "- Return ONLY valid JSON with these exact keys.\n"
        "JSON format:\n"
        "{\n"
        '  "keywords": ["...", "..."],\n'
        '  "hashtags": ["#...", "#..."],\n'
        '  "search_tags": ["...", "..."]\n'
        "}"
    )

    def __init__(self) -> None:
        """Initialize KeywordGenerator with OpenAI settings."""
        openai.api_key = settings.OPENAI_API_KEY
        self.model = settings.OPENAI_MODEL
        self.retry_max = settings.RETRY_MAX_ATTEMPTS

    def generate(
        self,
        product_title: str,
        category: str = ""
    ) -> GeneratedKeywords:
        """Generate SEO keywords and hashtags for a product.

        Calls OpenAI API with retry logic for rate limits and API errors.
        Falls back to keyword extraction on persistent failures.

        Args:
            product_title: Product title to generate keywords for.
            category: Optional product category (e.g., "TCG", "Anime Figure").

        Returns:
            GeneratedKeywords with keywords, hashtags, and search tags.
        """
        category_context = f"Category: {category}\n" if category else ""
        prompt = (
            f"Generate SEO keywords and hashtags for this Shopee product:\n"
            f"{category_context}"
            f"Title: {product_title}\n"
            f"Return ONLY valid JSON with 'keywords', 'hashtags', and 'search_tags'."
        )

        for attempt in range(self.retry_max):
            try:
                response = openai.ChatCompletion.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": self.SYSTEM_PROMPT},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.6,
                    max_tokens=400
                )

                raw_response = response.choices[0].message.content
                result = self._parse_keywords(raw_response)

                if result:
                    logger.info(f"Generated keywords for: {product_title[:50]}")
                    return result

            except openai.error.RateLimitError:
                wait_time = 60
                logger.warning(
                    f"Rate limited on attempt {attempt + 1}/{self.retry_max}. "
                    f"Waiting {wait_time}s..."
                )
                time.sleep(wait_time)

            except openai.error.APIError as e:
                logger.warning(
                    f"API error on attempt {attempt + 1}/{self.retry_max}: {e}"
                )
                if attempt < self.retry_max - 1:
                    time.sleep(5)

            except Exception as e:
                logger.error(f"Unexpected error generating keywords: {e}")
                break

        # Fallback on all failures
        logger.info(f"Falling back to keyword extraction for: {product_title[:50]}")
        return self._fallback_keywords(product_title)

    def generate_batch(self, titles: list[str]) -> list[GeneratedKeywords]:
        """Generate keywords for multiple products with rate limiting.

        Adds 0.5s delay between API calls to respect rate limits.

        Args:
            titles: List of product titles to generate keywords for.

        Returns:
            List of GeneratedKeywords objects.
        """
        results = []
        for i, title in enumerate(titles):
            if i > 0:
                time.sleep(0.5)
            results.append(self.generate(title))
        return results

    @staticmethod
    def _parse_keywords(raw: str) -> Optional[GeneratedKeywords]:
        """Parse keywords JSON response from API.

        Args:
            raw: Raw response string from API.

        Returns:
            GeneratedKeywords if parsed successfully, else None.
        """
        # Strip markdown code fences
        cleaned = raw.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

        try:
            parsed = json.loads(cleaned)
            if isinstance(parsed, dict):
                keywords = parsed.get("keywords", [])
                hashtags = parsed.get("hashtags", [])
                search_tags = parsed.get("search_tags", [])

                if keywords and hashtags and search_tags:
                    # Ensure hashtags start with #
                    hashtags = [
                        tag if tag.startswith("#") else f"#{tag}"
                        for tag in hashtags
                    ]

                    return GeneratedKeywords(
                        keywords=keywords,
                        hashtags=hashtags,
                        search_tags=search_tags
                    )
        except json.JSONDecodeError:
            logger.debug("Failed to parse keywords JSON")

        return None

    @staticmethod
    def _fallback_keywords(title: str) -> GeneratedKeywords:
        """Generate fallback keywords by extracting from title.

        Extracts key words from the title and adds generic anime/TCG tags.

        Args:
            title: Original product title.

        Returns:
            GeneratedKeywords with extracted and generic keywords.
        """
        # Extract key words from title
        words = title.lower().split()
        extracted = [w.strip('()[]{}') for w in words if len(w) > 3][:8]

        # Generic anime/TCG keywords
        generic_keywords = [
            "anime collectible", "Japanese import", "authentic",
            "collector item", "trading card"
        ]
        keywords = extracted + generic_keywords

        # Generic hashtags
        hashtags = [
            "#anime", "#collectibles", "#Japanese", "#authentic",
            "#seller", "#import", "#gaming"
        ]

        # Generic search tags
        search_tags = [
            "anime", "collectible", "Japan", "authentic",
            "import", "card game", "figure"
        ]

        return GeneratedKeywords(
            keywords=keywords[:15],
            hashtags=hashtags[:8],
            search_tags=search_tags[:10]
        )
