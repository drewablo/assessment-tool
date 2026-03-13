import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import _pathway_for_score, _pathway_confidence


def test_pathway_for_score_thresholds():
    assert _pathway_for_score(80) == "continue"
    assert _pathway_for_score(60) == "transform"
    assert _pathway_for_score(45) == "partner"
    assert _pathway_for_score(30) == "close"


def test_pathway_confidence_combines_data_and_stage2_readiness():
    assert _pathway_confidence("high", "ready") == "high"
    assert _pathway_confidence("medium", "partial") == "medium"
    assert _pathway_confidence("low", "not_ready") == "low"
