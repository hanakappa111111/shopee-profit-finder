"""Universal Product Key package.

Generates a normalised, cross-platform product identity key so the same
physical product is recognised regardless of the marketplace title language
(English / Japanese) or formatting variation.
"""

from src.product_key.generator import (
    ProductKeyComponents,
    ProductKeyGenerator,
    product_key_generator,
)

__all__ = [
    "ProductKeyComponents",
    "ProductKeyGenerator",
    "product_key_generator",
]
