import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from main import scoring_weights


@pytest.mark.asyncio
async def test_scoring_weights_exposes_module_aware_hierarchical_metadata():
    payload = await scoring_weights()

    assert "hierarchical" in payload
    assert "hierarchical_by_ministry" in payload

    modules = payload["hierarchical_by_ministry"]
    assert set(modules.keys()) == {"schools", "housing", "elder_care"}

    for ministry in ("schools", "housing", "elder_care"):
        assert set(modules[ministry].keys()) == {
            "market_opportunity",
            "competitive_position",
            "community_fit",
            "sustainability_risk",
        }

    assert "pipeline demand" in modules["schools"]["market_opportunity"]["description"].lower()
    assert "pipeline demand" not in modules["housing"]["market_opportunity"]["description"].lower()
    assert "pipeline demand" not in modules["elder_care"]["market_opportunity"]["description"].lower()
