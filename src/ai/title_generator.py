"""AI-powered title generation for Shopee listings.

Generates optimized product titles for Japanese anime collectibles and TCG
using OpenAI API with fallback and retry logic.
"""

import json
import time
from typing import Optional

from openai import OpenAI, RateLimitError, APIError

from src.config.settings import settings
from src.database.models import GeneratedTitles
from src.utils.logger import logger


class TitleGenerator:
    """Generates optimized Shopee product titles using AI.

    Attributes:
        client: OpenAI client instance.
        model: Model name (e.g., "gpt-4o-mini").
        retry_max: Maximum retry attempts for API errors.
    """

    SYSTEM_PROMPT = (
        "You are an expert Shopee listing copywriter specializing in Japanese anime "
        "collectibles and trading card games (TCG). "
        "Your titles must follow these strict rules:\n"
        "- Maximum 120 characters\n"
        "- English language only\n"
        "- Include brand name and key product keywords\n"
        "- Target anime collectors\n"
        "- Highlight Japan authenticity\n"
        "- No ALL-CAPS text\n"
        "- No words like SALE, CHEAP, HOT, MUST-BUY\n"
        "- Return ONLY a valid JSON array of exactly 5 title strings, nothing else.\n"
        "- Each title should be unique in approach but cohesive in quality."
    )

    def __init__(self) -> None:
        """Initialize TitleGenerator with OpenAI settings."""
        self.client = OpenAI(api_key=settings.OPENAI_API_KEY)
        self.model = settings.OPENAI_MODEL
        self.retry_max = settings.RETRY_MAX_ATTEMPTS

    def generate(self, product_title: str) -> GeneratedTitles:
        """Generate optimized Shopee titles for a product.

        Calls OpenAI API with retry logic for rate limits and API errors.
        Falls back to simple generated title on persistent failures.

        Args:
            product_title: Original product title to generate optimized versions for.

        Returns:
            GeneratedTitles object containing array of generated titles.
        """
        prompt = (
            f"Generate 5 optimized Shopee product titles for this Japanese collectible:\n"
            f"Original: {product_title}\n"
            f"Return ONLY valid JSON array of strings."
        )

        for attempt in range(self.retry_max):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": self.SYSTEM_PROMPT},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.7,
                    max_tokens=300
                )

                raw_response = response.choices[0].message.content
                titles = self._parse_titles(raw_response)

                if titles:
                    logger.info(f"Generated {len(titles)} titles for: {product_title[:50]}")
                    return GeneratedTitles(titles=titles, source_title=product_title)

            except RateLimitError:
                wait_time = 60
                logger.warning(
                    f"Rate limited on attempt {attempt + 1}/{self.retry_max}. "
                    f"Waiting {wait_time}s..."
                )
                time.sleep(wait_time)

            except APIError as e:
                logger.warning(
                    f"API error on attempt {attempt + 1}/{self.retry_max}: {e}"
                )
                if attempt < self.retry_max - 1:
                    time.sleep(5)

            except Exception as e:
                logger.error(f"Unexpected error generating titles: {e}")
                break

        # Fallback on all failures
        logger.info(f"Falling back to simple title generation for: {product_title[:50]}")
        fallback_title = self._fallback_title(product_title)
        return GeneratedTitles(
            titles=[fallback_title],
            source_title=product_title
        )

    def generate_batch(self, titles: list[str]) -> list[GeneratedTitles]:
        """Generate titles for multiple products with rate limiting.

        Adds 0.5s delay between API calls to respect rate limits.

        Args:
            titles: List of product titles to generate for.

        Returns:
            List of GeneratedTitles objects.
        """
        results = []
        for i, title in enumerate(titles):
            if i > 0:
                time.sleep(0.5)
            results.append(self.generate(title))
        return results

    @staticmethod
    def _parse_titles(raw: str) -> list[str]:
        """Parse JSON array from API response with fallback.

        Strips markdown code fences, parses JSON, and falls back to
        regex extraction if JSON parsing fails.

        Args:
            raw: Raw response string from API.

        Returns:
            List of title strings if parsed successfully, else empty list.
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

        # Try JSON parsing
        try:
            parsed = json.loads(cleaned)
            if isinstance(parsed, list):
                return [str(t).strip()[:120] for t in parsed]
        except json.JSONDecodeError:
            logger.debug("Failed to parse JSON, attempting regex fallback")

        # Fallback: extract quoted strings
        import re
        matches = re.findall(r'"([^"]+)"', raw)
        if matches:
            return [t.strip()[:120] for t in matches[:5]]

        return []

    @staticmethod
    def _fallback_title(title: str) -> str:
        """Generate simple fallback title.

        Args:
            title: Original product title.

        Returns:
            Fallback title combining original title with "Japanese Import".
        """
        fallback = f"{title} - Japanese Import"
        return fallback[:120]
