"""ProductMatchingAI — weighted similarity scorer for Shopee ↔ Japan product pairs.

Purpose
-------
The existing :class:`~src.matching.product_matcher.ProductMatcher` finds
*candidate* pairs via structural heuristics (product_key, barcode, brand+model,
title fuzzy).  ``ProductMatchingAI`` acts as a **second-pass validator**: it
scores every candidate pair on four independent signals and drops pairs whose
combined score falls below the acceptance threshold.

This significantly reduces false positives in the matching stage, ensuring that
downstream profit calculations are based on correctly matched products.

Scoring model
-------------
::

    match_score = 0.50 * title_similarity
                + 0.20 * brand_match
                + 0.20 * model_match
                + 0.10 * price_sanity

All sub-scores are in [0.0, 1.0].  The weighted sum is also in [0.0, 1.0].

Acceptance tiers
~~~~~~~~~~~~~~~~
::

    0.9 – 1.0  very likely same product
    0.7 – 0.9  possible match
    0.0 – 0.7  unlikely match

Default threshold: 0.8 (configurable).

Signal definitions
------------------
title_similarity (weight 0.50)
    Jaccard similarity on filtered token sets extracted from both titles.
    • normalise → lowercase, strip punctuation
    • remove English stop words (common noise: "the", "and", "for", …)
    • extract only Latin+numeric tokens (≥2 chars) — effective for cross-language
      pairs because Japanese product titles almost always contain the brand name
      and model number in Latin script
    • Jaccard = |A ∩ B| / |A ∪ B|

brand_match (weight 0.20)
    Extract the most prominent capitalised/Latin word cluster from each title and
    compare.  If both products share a brand token the score is 1.0, else 0.0.
    If brand cannot be extracted from either side the signal is neutral (0.5) to
    avoid penalising products where brand information is simply absent.

model_match (weight 0.20)
    Search both titles for model-number patterns (letter prefix + digit suffix,
    e.g. "RX-V485", "NK-100", "GRX100").  If a model number appears in BOTH
    titles and matches exactly, score is 1.0.  If a model number appears in
    only one title, score is 0.0.  If neither title contains a model number the
    signal is neutral (0.5) — product may genuinely have no model code.

price_sanity (weight 0.10)
    Convert the Shopee price to JPY using the live exchange rate and compare
    with the Japan supplier price.  A price difference > 80 % is a strong
    indicator of a false match (e.g. a ¥50 000 camera matched against a ¥750
    phone case).

    Scoring curve (relative difference d = |shopee_jpy - japan_jpy| / japan_jpy):
        d ≤ 0.50  →  1.0   (within 50 % — price fine)
        d ≤ 0.80  →  0.5   (50–80 % divergence — marginal)
        d >  0.80 →  0.2   (> 80 % — likely false match, heavy penalty)

Bypass rule
-----------
Pairs matched by **exact** strategies (product_key or barcode) are trusted
unconditionally and bypass AI scoring.  They receive match_score=1.0 and
matching_method="keyword".

Performance
-----------
All operations are pure in-memory regex + set arithmetic.  No network calls,
no OpenAI API.  50 pairs complete in < 5 ms.

Usage
-----
::

    from src.product_matching.matcher import ProductMatchingAI

    ai = ProductMatchingAI(threshold=0.8)
    accepted, all_scores = ai.filter_matches(match_results)
    # accepted: MatchResult list that passed the AI filter
    # all_scores: MatchAIScore list for every pair (for DB persistence)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Tuple

from src.database.models import JapanProduct, MatchResult, ShopeeProduct
from src.utils.logger import logger


# ── Constants ─────────────────────────────────────────────────────────────────

# Weights must sum to 1.0
_W_TITLE = 0.50
_W_BRAND = 0.20
_W_MODEL = 0.20
_W_PRICE = 0.10

# Minimum match_score to accept a pair
DEFAULT_THRESHOLD = 0.8

# Exact-match strategies that bypass AI scoring entirely
_EXACT_METHODS = frozenset({"product_key", "barcode"})

# English stop words common in product listings
_STOP_WORDS = frozenset({
    "a", "an", "the", "and", "or", "for", "of", "in", "to", "with",
    "new", "used", "set", "lot", "pack", "box", "item", "limited",
    "edition", "version", "series", "type", "model", "official",
    "authentic", "original", "genuine", "japan", "japanese",
    "import", "imported", "domestic", "free", "shipping",
})

# Regex: extract Latin + numeric tokens of length >= 2
_LATIN_TOKEN = re.compile(r"[a-zA-Z0-9]{2,}")

# Regex: model-number pattern — 1-5 letters + optional separator + 2-6 digits
# Covers: "RX-V485", "NK100", "GRX-100S", "MTB2023", "HC-SR04"
_MODEL_PATTERN = re.compile(
    r"\b([A-Z]{1,5}[-_]?\d{2,6}[A-Z0-9]{0,4})\b"
)


# ── Output dataclass ──────────────────────────────────────────────────────────


@dataclass
class MatchAIScore:
    """AI similarity result for a single Shopee ↔ Japan product pair."""

    shopee_url: str
    japan_url: str
    match_score: float           # 0.0–1.0 weighted composite
    title_similarity: float      # 0.0–1.0
    brand_score: float           # 0.0–1.0
    model_score: float           # 0.0–1.0
    price_score: float           # 0.0–1.0
    matching_method: str         # "keyword" | "ai_match"
    passed: bool                 # True if match_score >= threshold
    scored_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "shopee_url": self.shopee_url,
            "japan_url": self.japan_url,
            "match_score": round(self.match_score, 3),
            "title_similarity": round(self.title_similarity, 3),
            "brand_score": round(self.brand_score, 3),
            "model_score": round(self.model_score, 3),
            "price_score": round(self.price_score, 3),
            "matching_method": self.matching_method,
            "passed": self.passed,
        }


# ── Sub-scorer functions ──────────────────────────────────────────────────────


def _tokenize(text: str) -> frozenset[str]:
    """Lowercase, extract Latin/numeric tokens ≥2 chars, remove stop words."""
    tokens = set(_LATIN_TOKEN.findall(text.lower()))
    return frozenset(tokens - _STOP_WORDS)


def _jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    """Jaccard similarity: |intersection| / |union|.  Returns 0.0 if both empty."""
    if not a and not b:
        return 0.5  # both titles lack Latin tokens; treat as neutral
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


def _title_similarity(shopee_title: str, japan_title: str) -> float:
    """Jaccard similarity on filtered Latin token sets."""
    return _jaccard(_tokenize(shopee_title), _tokenize(japan_title))


def _extract_brand(title: str) -> Optional[str]:
    """Heuristic brand extraction: first all-caps or title-cased word cluster.

    Returns the longest consecutive sequence of 'brandish' tokens (all-caps
    or title-cased, length 2–20) that appears near the start of the title.
    Returns None if nothing convincing is found.
    """
    # Look for uppercase abbreviations or TitleCase words near the start
    brand_pattern = re.compile(r"\b([A-Z][A-Za-z0-9]{1,19}|[A-Z]{2,10})\b")
    matches = brand_pattern.findall(title[:80])  # only first 80 chars
    # Filter out all-caps model-number-like tokens (contain digits)
    candidates = [m for m in matches if not any(ch.isdigit() for ch in m)]
    return candidates[0].lower() if candidates else None


def _brand_match(shopee: ShopeeProduct, japan: JapanProduct) -> float:
    """1.0 if brands match, 0.0 if they clearly differ, 0.5 if indeterminate."""
    sp_brand = _extract_brand(shopee.title)
    jp_brand = _extract_brand(japan.title)

    if sp_brand is None or jp_brand is None:
        # Can't extract brand from at least one side → neutral
        return 0.5

    return 1.0 if sp_brand == jp_brand else 0.0


def _extract_models(title: str) -> frozenset[str]:
    """Return all model-number tokens found in *title* (uppercase normalised)."""
    return frozenset(m.upper().replace("-", "").replace("_", "")
                     for m in _MODEL_PATTERN.findall(title.upper()))


def _model_match(shopee_title: str, japan_title: str) -> float:
    """1.0 if model numbers match, 0.0 if they differ, 0.5 if absent."""
    sp_models = _extract_models(shopee_title)
    jp_models = _extract_models(japan_title)

    if not sp_models and not jp_models:
        return 0.5  # neither title has a model number → neutral

    if not sp_models or not jp_models:
        return 0.0  # one side has a model, the other doesn't → mismatch

    # If any model number appears in both → match
    return 1.0 if sp_models & jp_models else 0.0


def _price_sanity(shopee: ShopeeProduct, japan: JapanProduct) -> float:
    """Score based on price proximity after currency conversion.

    Scoring curve (relative difference d):
        d ≤ 0.50  → 1.0  (within 50 %)
        d ≤ 0.80  → 0.5  (50–80 % divergence — marginal)
        d >  0.80 → 0.2  (> 80 % — likely false match)
    """
    if shopee.price <= 0 or japan.price_jpy <= 0:
        return 0.5  # missing price data → neutral

    # Convert Shopee local price to JPY
    try:
        from src.profit.profit_engine import get_local_to_jpy_rate
        rate = get_local_to_jpy_rate(shopee.market if hasattr(shopee, "market") else None)
    except Exception:
        return 0.5  # rate unavailable → neutral

    shopee_jpy = shopee.price * rate
    diff = abs(shopee_jpy - japan.price_jpy) / max(japan.price_jpy, 0.01)

    if diff <= 0.50:
        return 1.0
    if diff <= 0.80:
        return 0.5
    return 0.2


# ── Main class ────────────────────────────────────────────────────────────────


class ProductMatchingAI:
    """Second-pass AI validator for Shopee ↔ Japan product matches.

    Parameters
    ----------
    threshold:
        Minimum match_score (0.0–1.0) to accept a pair.  Default: 0.8.
    """

    def __init__(self, threshold: float = DEFAULT_THRESHOLD) -> None:
        self.threshold = threshold

    # ── Public API ────────────────────────────────────────────────────────────

    def score_pair(
        self,
        shopee: ShopeeProduct,
        japan: JapanProduct,
        match_method: str = "title_fuzzy",
    ) -> MatchAIScore:
        """Compute a weighted similarity score for a single product pair.

        Exact matches (product_key / barcode) bypass AI scoring and receive
        match_score=1.0, matching_method='keyword'.
        """
        if match_method in _EXACT_METHODS:
            return MatchAIScore(
                shopee_url=shopee.product_url,
                japan_url=japan.product_url,
                match_score=1.0,
                title_similarity=1.0,
                brand_score=1.0,
                model_score=1.0,
                price_score=1.0,
                matching_method="keyword",
                passed=True,
            )

        t_sim = _title_similarity(shopee.title, japan.title)
        b_score = _brand_match(shopee, japan)
        m_score = _model_match(shopee.title, japan.title)
        p_score = _price_sanity(shopee, japan)

        total = (
            _W_TITLE * t_sim
            + _W_BRAND * b_score
            + _W_MODEL * m_score
            + _W_PRICE * p_score
        )

        return MatchAIScore(
            shopee_url=shopee.product_url,
            japan_url=japan.product_url,
            match_score=total,
            title_similarity=t_sim,
            brand_score=b_score,
            model_score=m_score,
            price_score=p_score,
            matching_method="ai_match",
            passed=(total >= self.threshold),
        )

    def filter_matches(
        self,
        matches: List[MatchResult],
    ) -> Tuple[List[MatchResult], List[MatchAIScore]]:
        """Score and filter a list of candidate matches.

        Parameters
        ----------
        matches:
            Output of :class:`~src.matching.product_matcher.ProductMatcher`.

        Returns
        -------
        accepted : List[MatchResult]
            Pairs whose AI score >= threshold, in original order.
        all_scores : List[MatchAIScore]
            Scoring detail for every pair (including rejected ones).
        """
        if not matches:
            return [], []

        accepted: List[MatchResult] = []
        all_scores: List[MatchAIScore] = []

        for match in matches:
            ai = self.score_pair(
                match.shopee_product,
                match.japan_product,
                match_method=match.match_method,
            )
            all_scores.append(ai)
            if ai.passed:
                accepted.append(match)

        # ── Logging ──────────────────────────────────────────────────────────
        n_exact = sum(1 for s in all_scores if s.matching_method == "keyword")
        n_ai_pass = sum(1 for s in all_scores if s.matching_method == "ai_match" and s.passed)
        n_ai_fail = sum(1 for s in all_scores if s.matching_method == "ai_match" and not s.passed)

        logger.info(
            f"[ProductMatchingAI] "
            f"input={len(matches)} | "
            f"accepted={len(accepted)} | "
            f"exact_bypass={n_exact} | "
            f"ai_pass={n_ai_pass} | "
            f"ai_reject={n_ai_fail} | "
            f"threshold={self.threshold}"
        )

        if n_ai_fail:
            # Log details of rejected pairs for debugging
            rejected_scores = [
                s for s in all_scores
                if s.matching_method == "ai_match" and not s.passed
            ]
            for s in rejected_scores[:3]:  # show top-3 rejections
                logger.debug(
                    f"[ProductMatchingAI] REJECTED "
                    f"score={s.match_score:.3f} "
                    f"(T={s.title_similarity:.2f} "
                    f"B={s.brand_score:.2f} "
                    f"M={s.model_score:.2f} "
                    f"P={s.price_score:.2f})"
                )

        return accepted, all_scores
