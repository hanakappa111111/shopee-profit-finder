"""AI-powered description generation for Shopee listings.

Generates detailed, SEO-optimized product descriptions for Japanese anime
collectibles and TCG using OpenAI API.
"""

import json
import time
from typing import Optional
import openai

from src.config.settings import settings
from src.database.models import GeneratedDescription, JapanProduct
from src.utils.logger import logger


class DescriptionGenerator:
    """Generates optimized Shopee product descriptions using AI.

    Attributes:
        api_key: OpenAI API key from settings.
        model: Model name (e.g., "gpt-4" or "gpt-3.5-turbo").
        retry_max: Maximum retry attempts for API errors.
    """

    SYSTEM_PROMPT = (
        "You are an expert Shopee product description writer for Japanese anime "
        "collectibles and trading cards. Create compelling, authentic descriptions that:\n"
        "- Highlight Japan authenticity and quality\n"
        "- Use persuasive copywriting for collectors\n"
        "- Include product details, condition, authenticity guarantees\n"
        "- Optimize for search with natural keywords\n"
        "- Format as 2-3 paragraphs (200-400 words total)\n"
        "- Include 5 key bullet points\n"
        "- Return ONLY valid JSON with 'description' and 'bullet_points' keys.\n"
        "JSON format: {\"description\": \"...\", \"bullet_points\": [\"...\", \"...\", ...]}"
    )

    def __init__(self) -> None:
        """Initialize DescriptionGenerator with OpenAI settings."""
        openai.api_key = settings.OPENAI_API_KEY
        self.model = settings.OPENAI_MODEL
        self.retry_max = settings.RETRY_MAX_ATTEMPTS

    def generate(
        self,
        product_title: str,
        japan_product: Optional[JapanProduct] = None,
        context: str = ""
    ) -> GeneratedDescription:
        """Generate detailed description for a product.

        Calls OpenAI API with retry logic. Uses Japan product details
        and optional context to improve description quality.

        Args:
            product_title: Product title for description.
            japan_product: Optional JapanProduct with source details.
            context: Optional additional context (e.g., brand info, condition).

        Returns:
            GeneratedDescription with description text and bullet points.
        """
        prompt = self._build_prompt(product_title, japan_product, context)

        for attempt in range(self.retry_max):
            try:
                response = openai.ChatCompletion.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": self.SYSTEM_PROMPT},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.7,
                    max_tokens=600
                )

                raw_response = response.choices[0].message.content
                result = self._parse_response(raw_response, product_title)

                if result:
                    logger.info(f"Generated description for: {product_title[:50]}")
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
                logger.error(f"Unexpected error generating description: {e}")
                break

        # Fallback on all failures
        logger.info(f"Falling back to template description for: {product_title[:50]}")
        return self._fallback_description(product_title)

    def generate_batch(self, products: list[tuple[str, Optional[JapanProduct]]]) -> list[GeneratedDescription]:
        """Generate descriptions for multiple products with rate limiting.

        Adds 0.5s delay between API calls to respect rate limits.

        Args:
            products: List of (title, japan_product) tuples.

        Returns:
            List of GeneratedDescription objects.
        """
        results = []
        for i, (title, japan_product) in enumerate(products):
            if i > 0:
                time.sleep(0.5)
            results.append(self.generate(title, japan_product))
        return results

    @staticmethod
    def _build_prompt(
        title: str,
        japan_product: Optional[JapanProduct],
        context: str
    ) -> str:
        """Build prompt for description generation.

        Args:
            title: Product title.
            japan_product: Optional Japan product details.
            context: Optional additional context.

        Returns:
            Formatted prompt string.
        """
        prompt = f"Generate a compelling Shopee product description for:\nTitle: {title}\n"

        if japan_product:
            prompt += f"Source (Japan): {japan_product.title}\n"
            if japan_product.description:
                prompt += f"Details: {japan_product.description[:200]}\n"

        if context:
            prompt += f"Additional Context: {context}\n"

        prompt += (
            "Create an engaging description highlighting authenticity, "
            "condition, and appeal to collectors. "
            "Return JSON with 'description' and 'bullet_points' keys."
        )

        return prompt

    @staticmethod
    def _parse_response(raw: str, title: str) -> Optional[GeneratedDescription]:
        """Parse description response from API.

        Args:
            raw: Raw response string from API.
            title: Original product title for reference.

        Returns:
            GeneratedDescription if parsed successfully, else None.
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
                description = parsed.get("description", "")
                bullet_points = parsed.get("bullet_points", [])

                if description and bullet_points:
                    return GeneratedDescription(
                        description=description,
                        bullet_points=bullet_points,
                        source_title=title
                    )
        except json.JSONDecodeError:
            logger.debug("Failed to parse description JSON")

        return None

    @staticmethod
    def _fallback_description(title: str) -> GeneratedDescription:
        """Generate template fallback description.

        Args:
            title: Original product title.

        Returns:
            GeneratedDescription with generic template.
        """
        description = (
            f"Authentic Japanese Import - {title}\n\n"
            f"This is a genuine Japanese collectible sourced directly from Japan. "
            f"We specialize in authentic anime and trading card game products, "
            f"carefully selected for quality and authenticity. Perfect for collectors "
            f"seeking original Japanese editions.\n\n"
            f"All items are inspected for authenticity and condition before shipping. "
            f"We guarantee genuine Japanese products with secure packaging."
        )

        bullet_points = [
            "Authentic Japanese product",
            "Direct source from Japan",
            "Genuine collectible item",
            "Secure packaging",
            "Quality inspection guaranteed"
        ]

        return GeneratedDescription(
            description=description,
            bullet_points=bullet_points,
            source_title=title
        )
