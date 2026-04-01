"""Japan Supplier Search AI — Rakuten marketplace adapter.

Wraps ``src.japan_source.rakuten_scraper.RakutenScraper`` and adds
product_key enrichment.  Rakuten often has competitive pricing for
TCG sealed products and official figure retailers.
"""

from __future__ import annotations

from typing import Any

from src.supplier_search.base_scraper import BaseMarketplaceAdapter


class RakutenAdapter(BaseMarketplaceAdapter):
    """Rakuten Ichiba marketplace adapter."""

    marketplace_name = "Rakuten"

    def _get_inner_scraper(self) -> Any:
        from src.japan_source.rakuten_scraper import RakutenScraper
        return RakutenScraper()

    def _transform_query(self, query: str) -> str:
        """Rakuten performs well with Japanese keywords.

        No major transformation needed; just cap length.
        """
        q = query.strip()
        if len(q) > 120:
            q = q[:120].rsplit(" ", 1)[0]
        return q
