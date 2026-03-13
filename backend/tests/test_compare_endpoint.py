import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from main import analyze_compare
from models.schemas import CompareAnalysisRequest, FacilityProfile


class _Score:
    def __init__(self, overall, conservative, optimistic):
        self.overall = overall
        self.scenario_conservative = conservative
        self.scenario_optimistic = optimistic
        self.stage2 = None


class _Result:
    def __init__(self, ministry_type, score):
        self.ministry_type = ministry_type
        self.feasibility_score = _Score(score, score - 10, score + 10)
        self.recommendation = f"{ministry_type}-rec"
        self.recommendation_detail = f"{ministry_type}-detail"
        self.demographics = type("_Demographics", (), {"data_confidence": "medium"})()


@pytest.mark.asyncio
async def test_compare_endpoint_dedupes_and_sorts(monkeypatch):
    async def fake_geocode(_address):
        return {"lat": 41.0, "lon": -87.0, "state_fips": "17", "county_fips": "031"}

    async def fake_run_analysis(_location, request):
        by_type = {"schools": 70, "housing": 62, "elder_care": 80}
        return _Result(request.ministry_type, by_type[request.ministry_type])

    monkeypatch.setattr("main.geocode_address", fake_geocode)
    monkeypatch.setattr("main._run_analysis", fake_run_analysis)

    request = CompareAnalysisRequest(
        school_name="Test Campus",
        address="123 Main St",
        ministry_types=["housing", "schools", "housing", "elder_care"],
    )

    response = await analyze_compare(request)

    assert response.compared_ministry_types == ["housing", "schools", "elder_care"]
    assert [r.ministry_type for r in response.results] == ["elder_care", "schools", "housing"]
    assert response.results[0].overall_score == 80

    assert response.results[0].recommended_pathway == "continue"
    assert response.results[0].fit_band == "high"
    assert response.results[0].operator_dependency in {"optional", "required", "none"}


@pytest.mark.asyncio
async def test_compare_endpoint_propagates_facility_profile(monkeypatch):
    async def fake_geocode(_address):
        return {"lat": 41.0, "lon": -87.0, "state_fips": "17", "county_fips": "031"}

    seen_profiles = []

    async def fake_run_analysis(_location, request):
        seen_profiles.append(request.facility_profile)
        return _Result(request.ministry_type, 62)

    monkeypatch.setattr("main.geocode_address", fake_geocode)
    monkeypatch.setattr("main._run_analysis", fake_run_analysis)

    profile = FacilityProfile(
        building_square_footage=9500,
        accessibility_constraints=["No elevator"],
        zoning_use_constraints=["Special use permit required"],
        sponsor_operator_capacity="low",
    )
    request = CompareAnalysisRequest(
        school_name="Compare Campus",
        address="123 Main St",
        ministry_types=["schools", "housing"],
        facility_profile=profile,
    )

    await analyze_compare(request)

    assert len(seen_profiles) == 2
    assert all(p is not None for p in seen_profiles)
    assert all(p.sponsor_operator_capacity == "low" for p in seen_profiles)
