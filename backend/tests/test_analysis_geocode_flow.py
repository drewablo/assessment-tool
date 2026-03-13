import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi import HTTPException

from api.geocoder import GeocoderServiceError
from main import analyze
from models.schemas import AnalysisRequest


@pytest.mark.asyncio
async def test_analyze_maps_geocoder_service_failure_to_503(monkeypatch):
    async def fake_geocode(_address: str):
        raise GeocoderServiceError("Geocoder HTTP 403")

    monkeypatch.setattr("main.geocode_address", fake_geocode)

    with pytest.raises(HTTPException) as exc_info:
        await analyze(AnalysisRequest(school_name="X", address="123 Main St", ministry_type="schools"))

    exc = exc_info.value
    assert exc.status_code == 503
    assert exc.detail["error_code"] == "GEOCODER_UNAVAILABLE"


@pytest.mark.asyncio
@pytest.mark.parametrize("ministry_type", ["schools", "housing", "elder_care"])
async def test_analyze_shared_flow_reaches_dispatch_for_all_ministries(monkeypatch, ministry_type):
    calls = []

    async def fake_geocode(_address: str):
        return {"lat": 41.0, "lon": -87.0, "state_fips": "17", "county_fips": "031"}

    async def fake_run_analysis(_location, request):
        calls.append(request.ministry_type)
        return {"ministry_type": request.ministry_type, "overall": 75}

    async def fake_enrich(result, _request):
        return {**result, "enriched": True}

    monkeypatch.setattr("main.geocode_address", fake_geocode)
    monkeypatch.setattr("main._run_analysis", fake_run_analysis)
    monkeypatch.setattr("main._enrich_analysis_result", fake_enrich)

    result = await analyze(
        AnalysisRequest(school_name="X", address="123 Main St", ministry_type=ministry_type)
    )

    assert calls == [ministry_type]
    assert result["ministry_type"] == ministry_type
    assert result["enriched"] is True
