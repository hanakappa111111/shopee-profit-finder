"""Japan Supplier Search AI — Abstract base for marketplace adapters.

Each marketplace adapter wraps an existing scraper from ``japan_source/``
and adds supplier-search-specific behaviour:

* product_key enrichment via ``product_key_generator``
* result-count limiting
* structured error handling and logging

Subclasses need only implement :meth:`_get_inner_scraper` and optionally
override :meth:`_transform_query` for marketplace-specific query tuning.
"""

from __future__ import annotations

import abc
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.config.settings import settings
from src.database.models import JapanProduct
from src.utils.logger import logger


class BaseMarketplaceAdapter(abc.ABC):
    """Abstract adapter that wraps an existing ``japan_source`` scraper.

    Parameters
    ----------
    max_results:
        Max results to keep per query.
        Defaults to ``settings.SUPPLIER_MAX_RESULTS_PER_QUERY``.
    enrich_product_key:
        Whether to run ``product_key_generator`` on each result.
        Defaults to ``True``.
    """

    marketplace_name: str = "unknown"

    def __init__(
        self,
        max_results: Optional[int] = None,
        enrich_product_key: bool = True,
    ) -> None:
        self._max_results = max_results or settings.SUPPLIER_MAX_RESULTS_PER_QUERY
        self._enrich_key  = enrich_product_key
        self._scraper     = self._get_inner_scraper()

    @abc.abstractmethod
    def _get_inner_scraper(self) -> Any:
        """Return an instance of the underlying ``japan_source`` scraper."""
        ...

    def search(self, query: str) -> List[JapanProduct]:
        """Execute a search query and return enriched JapanProduct results.

        Parameters
        ----------
        query:
            Raw search string (will be passed through :meth:`_transform_query`
            before submission to the marketplace).

        Returns
        -------
        List[JapanProduct]
            Enriched results, capped at ``max_results``.
        """
        transformed = self._transform_query(query)

        try:
            raw_results = self._scraper.search(
                transformed,
                limit=self._max_results,
            )
        except Exception as exc:
            logger.warning(
                f"[{self.marketplace_name}] search failed for "
                f"query={transformed!r}: {exc}"
            )
            return []

        # Enrich with product_key
        if self._enrich_key:
            raw_results = [self._enrich(p) for p in raw_results]

        logger.debug(
            f"[{self.marketplace_name}] query={transformed!r} → "
            f"{len(raw_results)} results"
        )
        return raw_results[: self._max_results]

    def _transform_query(self, query: str) -> str:
        """Optional hook: transform the query for this marketplace.

        Default implementation passes through unchanged.  Subclasses can
        override for marketplace-specific keyword optimisation.
        """
        return query

    @staticmethod
    def _enrich(product: JapanProduct) -> JapanProduct:
        """Assign a ``product_key`` if one can be generated from the title."""
        if product.product_key:
            return product  # Already has a key

        try:
            from src.product_key.generator import product_key_generator
            result = product_key_generator.generate(product.title)
            if result and result.product_key:
                product.product_key = result.product_key
                product.product_key_confidence = result.confidence
        except Exception:
            pass  # Best-effort enrichment

        return product
