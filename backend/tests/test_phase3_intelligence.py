import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from main import _build_benchmark_narrative, _build_board_report_pack, _build_data_freshness_metadata, export_board_pack
from models.schemas import (
    AnalysisRequest,
    AnalysisResponse,
    BenchmarkPercentiles,
    DataFreshnessMetadata,
    DemographicData,
    FeasibilityScore,
    MetricScore,
)


def _metric(score: int, label: str = "Metric") -> MetricScore:
    return MetricScore(score=score, label=label, description="d", weight=25, rating="moderate")


def _analysis_response() -> AnalysisResponse:
    return AnalysisResponse(
        school_name="St. Test",
        ministry_type="schools",
        analysis_address="123 Main St",
        county_name="County",
        state_name="Ohio",
        lat=41.0,
        lon=-87.0,
        radius_miles=10,
        catchment_type="radius",
        demographics=DemographicData(data_confidence="medium"),
        competitor_schools=[],
        catholic_school_count=0,
        total_private_school_count=0,
        feasibility_score=FeasibilityScore(
            overall=72,
            scenario_conservative=62,
            scenario_optimistic=82,
            market_size=_metric(70, "Market"),
            income=_metric(71, "Income"),
            competition=_metric(72, "Competition"),
            family_density=_metric(73, "Density"),
            benchmarks=BenchmarkPercentiles(
                percentile_state=68.5,
                percentile_national=74.2,
                sample_size_state=120,
                sample_size_national=2400,
                comparable_markets=[{"geoid": "39035123400", "overall_score": 70}],
            ),
        ),
        recommendation="Proceed",
        recommendation_detail="Detail",
        data_notes=[],
    )


@pytest.mark.asyncio
async def test_live_mode_data_freshness_metadata():
    metadata = await _build_data_freshness_metadata()
    assert isinstance(metadata, DataFreshnessMetadata)
    assert metadata.mode in {"live", "db_precomputed"}
    assert metadata.sources


def test_benchmark_narrative_generation():
    result = _analysis_response()
    narrative = _build_benchmark_narrative(result)
    assert narrative is not None
    assert "percentile" in narrative.narrative_summary.lower()


def test_board_report_pack_generation():
    result = _analysis_response()
    result.benchmark_narrative = _build_benchmark_narrative(result)
    pack = _build_board_report_pack(result)
    assert pack.executive_summary
    assert len(pack.action_roadmap.months_12) >= 1
    assert len(pack.methodology_assumptions) >= 1


@pytest.mark.asyncio
async def test_export_board_pack_endpoint(monkeypatch):
    async def fake_geocode(_address):
        return {"lat": 41.0, "lon": -87.0, "state_fips": "17", "county_fips": "031"}

    async def fake_run_analysis(_location, _request):
        return _analysis_response()

    monkeypatch.setattr("main.geocode_address", fake_geocode)
    monkeypatch.setattr("main._run_analysis", fake_run_analysis)

    payload = await export_board_pack(
        AnalysisRequest(
            school_name="St. Test",
            address="123 Main St",
            ministry_type="schools",
        )
    )

    assert payload["board_report_pack"] is not None
    assert payload["benchmark_narrative"] is not None
