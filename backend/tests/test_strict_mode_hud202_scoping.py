import types

import pytest
from fastapi import HTTPException

from main import analyze
from models.schemas import AnalysisRequest


@pytest.mark.asyncio
async def test_db_strict_does_not_block_non_senior_housing_when_hud202_missing(monkeypatch):
    called = {"ran": False}

    async def fake_collect_health():
        return {"counts": {"census_tracts": 100, "hud_section_202": 0}}

    async def fake_geocode(_address: str):
        return {"lat": 41.0, "lon": -87.0, "state_fips": "17", "county_fips": "031"}

    async def fake_run_analysis(_location, request, run_mode="db_with_fallback"):
        called["ran"] = True
        return types.SimpleNamespace(ministry_type=request.ministry_type), {"fallback_notes": [], "effective_source_counts": {}}

    async def fake_enrich(result, _request):
        return result

    monkeypatch.setattr("main.USE_DB", True)
    async def fake_get_redis():
        return None

    monkeypatch.setattr("main._get_redis", fake_get_redis)
    monkeypatch.setattr("main._collect_db_data_health", fake_collect_health)
    monkeypatch.setattr("main.geocode_address", fake_geocode)
    monkeypatch.setattr("main._run_analysis", fake_run_analysis)
    monkeypatch.setattr("main._apply_reliability_metadata", lambda result, **kwargs: result)
    monkeypatch.setattr("main._enrich_analysis_result", fake_enrich)

    result = await analyze(
        AnalysisRequest(
            school_name="Housing Site",
            address="123 Main St",
            ministry_type="housing",
            housing_target_population="all_ages",
            run_mode="db_strict",
        )
    )

    assert called["ran"] is True
    assert result.ministry_type == "housing"


@pytest.mark.asyncio
async def test_db_strict_blocks_senior_housing_when_hud202_missing(monkeypatch):
    async def fake_collect_health():
        return {"counts": {"census_tracts": 100, "hud_section_202": 0}}

    monkeypatch.setattr("main.USE_DB", True)
    monkeypatch.setattr("main._collect_db_data_health", fake_collect_health)

    with pytest.raises(HTTPException) as exc:
        await analyze(
            AnalysisRequest(
                school_name="Senior Site",
                address="123 Main St",
                ministry_type="housing",
                housing_target_population="senior_only",
                run_mode="db_strict",
            )
        )

    assert exc.value.status_code == 503
    assert exc.value.detail["error_code"] == "STRICT_MODE_BLOCKED"
    assert any("hud_section_202" in b for b in exc.value.detail.get("blockers", []))
