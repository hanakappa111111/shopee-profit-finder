"""Research AI — product discovery engine.

Public re-exports
-----------------
ResearchEngine
    Main orchestrator.  Call ``scan()`` to populate ``research_candidates``.
ResearchScorer
    Stateless scoring component.  Useful for unit-testing individual scores.
SnapshotTrendAnalyzer
    Reads ``product_snapshots`` to derive velocity and stability signals.
research_engine
    Module-level singleton ``ResearchEngine`` bound to the shared ``db``.
"""

from src.research_ai.research_engine import ResearchEngine, research_engine
from src.research_ai.scoring import ResearchScorer, ScoreBreakdown, WEIGHTS
from src.research_ai.trend_detection import SnapshotTrendAnalyzer

__all__ = [
    "ResearchEngine",
    "research_engine",
    "ResearchScorer",
    "ScoreBreakdown",
    "WEIGHTS",
    "SnapshotTrendAnalyzer",
]
