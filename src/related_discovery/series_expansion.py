"""Related Product Discovery AI — Series Expansion strategy.

Given a seed Shopee product whose title contains a TCG set code (e.g. "OP01",
"SV-04", "BT-12"), ``SeriesExpander`` generates candidates for adjacent set
codes in the same series.

Algorithm
---------
1. Extract a *series code* from the seed title and keyword fields using the
   same regex that ``product_key_generator`` uses (``_MODEL_PATTERNS``).
   The code is split into three parts::

       prefix  –  one to three uppercase letters, e.g. "OP", "SV", "BT"
       num     –  zero-padded decimal number, e.g. "01", "04", "12"
       suffix  –  optional trailing letters, e.g. "" or "a"

2. Generate ``DISCOVERY_SERIES_LOOKAHEAD`` codes *ahead* of the seed (e.g.
   OP02 → OP05 when lookahead=3) and up to ``DISCOVERY_SERIES_LOOKAHEAD``
   codes *behind* it (e.g. OP01 → only OP02 … since there is no OP00).

3. For each generated code, check whether any existing Shopee product in the
   DB has that code in its title or keyword.

   * **DB-confirmed** (found in ``products``):  confidence = 90
   * **Generated** (not found):                 confidence = 75

4. Return ``List[RelatedProductCandidate]`` with
   ``discovery_method = DiscoveryMethod.SERIES``, filtered by
   ``min_confidence``, sorted descending, capped at ``max_keywords``.

The module reads from ``products`` but **never writes** — all writes are done
by ``DiscoveryEngine``.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from src.config.settings import settings
from src.database.models import DiscoveryMethod, RelatedProductCandidate
from src.utils.logger import logger

if TYPE_CHECKING:
    from src.database.database import Database


# ── Series-code regex ─────────────────────────────────────────────────────────
# Matches patterns like: OP01, OP-01, SV04, BT-12, ST01, PRE01, EB03a
# Groups: (prefix)(optional-dash)(zero-padded-number)(optional-suffix)
_SERIES_RE = re.compile(
    r'\b([A-Z]{1,3})-?(\d{2,3})([a-zA-Z]?)\b',
    re.IGNORECASE,
)

# Confidence levels
_CONF_DB_CONFIRMED: float = 90.0
_CONF_GENERATED:    float = 75.0


def _parse_series_code(text: str) -> Optional[Tuple[str, int, str, str]]:
    """Try to extract the first series code from *text*.

    Returns
    -------
    tuple (prefix_upper, num_int, suffix_lower, raw_matched) or ``None``.
    """
    if not text:
        return None
    m = _SERIES_RE.search(text)
    if m is None:
        return None
    prefix = m.group(1).upper()
    num    = int(m.group(2))
    suffix = m.group(3).lower()
    raw    = m.group(0)
    return prefix, num, suffix, raw


def _format_code(prefix: str, num: int, suffix: str, pad_width: int) -> str:
    """Reconstruct a series code string with the original zero-padding width."""
    return f"{prefix}{str(num).zfill(pad_width)}{suffix}"


class SeriesExpander:
    """Generate sequential series-sibling keywords for a seed Shopee product.

    Parameters
    ----------
    db:
        Open :class:`~src.database.database.Database` instance (read-only use).
    lookahead:
        How many set codes to generate ahead *and* behind the seed code.
        Defaults to ``settings.DISCOVERY_SERIES_LOOKAHEAD``.
    max_keywords:
        Hard cap on candidates returned per seed.
        Defaults to ``settings.DISCOVERY_MAX_KEYWORDS_PER_SEED``.
    min_confidence:
        Minimum confidence to include a result.
        Defaults to ``settings.DISCOVERY_MIN_CONFIDENCE``.
    """

    def __init__(
        self,
        db: "Database",
        lookahead: Optional[int] = None,
        max_keywords: Optional[int] = None,
        min_confidence: Optional[float] = None,
    ) -> None:
        self._db       = db
        self._lookahead  = lookahead if lookahead is not None else settings.DISCOVERY_SERIES_LOOKAHEAD
        self._max        = max_keywords or settings.DISCOVERY_MAX_KEYWORDS_PER_SEED
        self._min_conf   = (
            min_confidence if min_confidence is not None
            else settings.DISCOVERY_MIN_CONFIDENCE
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def expand(self, seed_row: Dict[str, Any]) -> List[RelatedProductCandidate]:
        """Run series expansion for one seed product row.

        Parameters
        ----------
        seed_row:
            Dict from ``Database.get_products()`` containing at minimum:
            ``id``, ``title``, ``keyword``.

        Returns
        -------
        List[RelatedProductCandidate]
            Deduplicated candidates sorted by confidence descending.
            Empty list if no series code is detected in the seed.
        """
        seed_id = seed_row.get("id")

        # ── Step 1: detect series code ────────────────────────────────────────
        parsed = self._detect_series_code(seed_row)
        if parsed is None:
            logger.debug(
                f"[SeriesExpander] seed_id={seed_id}: "
                "no series code detected — skipping series expansion"
            )
            return []

        prefix, num, suffix, raw, pad_width, context_title = parsed

        logger.debug(
            f"[SeriesExpander] seed_id={seed_id}: "
            f"detected series code={raw!r}  prefix={prefix!r}  num={num}  "
            f"suffix={suffix!r}  pad_width={pad_width}"
        )

        # ── Step 2: build candidate code list ────────────────────────────────
        target_nums: List[int] = []
        # Codes behind the seed (down to 1 minimum)
        for delta in range(1, self._lookahead + 1):
            n = num - delta
            if n >= 1:
                target_nums.append(n)
        # Codes ahead of the seed
        for delta in range(1, self._lookahead + 1):
            target_nums.append(num + delta)

        # ── Step 3: build title lookup from DB ────────────────────────────────
        # We only need to scan products for this prefix once.
        db_code_set: frozenset[str] = self._build_db_code_set(prefix)

        # ── Step 4: create candidates ─────────────────────────────────────────
        now = datetime.utcnow()
        candidates: Dict[str, RelatedProductCandidate] = {}

        for n in target_nums:
            code = _format_code(prefix, n, suffix, pad_width)
            # Construct a search keyword: replace the original code in the seed
            # title, giving the Japan scraper a realistic search string.
            kw = self._build_keyword(code, raw, context_title)

            confirmed = code.upper() in db_code_set
            conf = _CONF_DB_CONFIRMED if confirmed else _CONF_GENERATED

            if conf < self._min_conf:
                continue

            if kw not in candidates:
                candidates[kw] = RelatedProductCandidate(
                    seed_product_id=seed_id,
                    related_keyword=kw,
                    discovery_method=DiscoveryMethod.SERIES,
                    confidence_score=conf,
                    created_at=now,
                )

        # ── Step 5: sort, cap, return ─────────────────────────────────────────
        results = sorted(
            candidates.values(),
            key=lambda c: c.confidence_score,
            reverse=True,
        )[: self._max]

        logger.debug(
            f"[SeriesExpander] seed_id={seed_id}: "
            f"{len(results)} keywords generated "
            f"({sum(1 for c in results if c.confidence_score >= _CONF_DB_CONFIRMED)} DB-confirmed)"
        )
        return results

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _detect_series_code(
        self,
        row: Dict[str, Any],
    ) -> Optional[Tuple[str, int, str, str, int, str]]:
        """Try to parse a series code from ``title`` then ``keyword``.

        Returns (prefix, num, suffix, raw, pad_width, source_text) or None.
        """
        for field_name in ("title", "keyword"):
            text = str(row.get(field_name) or "")
            result = _parse_series_code(text)
            if result is not None:
                prefix, num, suffix, raw = result
                # Determine original zero-padding width (e.g. "OP01" → 2, "BT-012" → 3)
                pad_width = len(re.search(r'\d+', raw).group(0))
                return prefix, num, suffix, raw, pad_width, text
        return None

    def _build_db_code_set(self, prefix: str) -> frozenset:
        """Return a frozenset of upper-cased series codes found in the DB
        whose prefix matches *prefix*.

        We scan ``products.title`` and ``products.keyword`` for any token
        matching the series regex, then keep only those with the right prefix.
        """
        codes: set[str] = set()
        try:
            all_products = self._db.get_products(limit=2000)
            for p in all_products:
                for field_name in ("title", "keyword"):
                    text = str(p.get(field_name) or "")
                    for m in _SERIES_RE.finditer(text):
                        if m.group(1).upper() == prefix.upper():
                            # Store without dash: e.g. "OP-01" → "OP01"
                            code_clean = (
                                m.group(1).upper()
                                + m.group(2)
                                + m.group(3).lower()
                            )
                            codes.add(code_clean)
        except Exception as exc:
            logger.warning(f"[SeriesExpander] DB code scan failed: {exc}")
        return frozenset(codes)

    @staticmethod
    def _build_keyword(
        new_code: str,
        original_code: str,
        source_title: str,
    ) -> str:
        """Build a search keyword by substituting *new_code* for
        *original_code* in *source_title*.

        If the substitution would produce an identical or very short string,
        fall back to the bare *new_code*.
        """
        if not source_title:
            return new_code

        # Case-insensitive replacement of the first occurrence
        pattern = re.compile(re.escape(original_code), re.IGNORECASE)
        kw = pattern.sub(new_code, source_title, count=1).strip()

        # Sanity guards
        if len(kw) < 3 or kw == source_title:
            return new_code

        # Trim to 120 chars so it fits the DB column
        return kw[:120]
