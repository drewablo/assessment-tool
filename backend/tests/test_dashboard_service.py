from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import main
from models.schemas import (
    AnalysisRequest,
    AnalysisResponse,
    CompetitorSchool,
    ConfidenceSummary,
    DataFreshnessMetadata,
    DataFreshnessSource,
    DemographicData,
    ExportReadiness,
    FeasibilityScore,
    FallbackSummary,
    MetricScore,
)
from services import dashboard_service
from services.projections import HistoricalValue, build_projection_envelope


@pytest.fixture(autouse=True)
def clear_zcta_cache():
    dashboard_service._load_zcta_cache.cache_clear()
    yield
    dashboard_service._load_zcta_cache.cache_clear()


def _metric(label: str, score: int, weight: int = 25) -> MetricScore:
    return MetricScore(score=score, label=label, description=label, weight=weight, rating="moderate")


def _response(ministry_type: str = "schools") -> AnalysisResponse:
    return AnalysisResponse(
        school_name="St. Example",
        ministry_type=ministry_type,
        analysis_address="123 Main St, Fort Myers, FL 33901",
        county_name="Lee County",
        state_name="Florida",
        lat=26.6406,
        lon=-81.8723,
        radius_miles=6.0,
        catchment_minutes=20,
        catchment_type="radius",
        gender="coed",
        grade_level="k12",
        demographics=DemographicData(
            total_population=25000,
            population_under_18=6200,
            school_age_population=4100,
            estimated_catholic_school_age=850,
            median_household_income=72000,
            total_households=9800,
            families_with_children=3400,
            owner_occupied_pct=58.0,
            estimated_catholic_pct=12.0,
            data_geography="tract",
            data_confidence="medium",
            seniors_65_plus=5200,
            seniors_75_plus=2100,
            seniors_living_alone=1200,
            seniors_projected_5yr=5600,
            seniors_projected_10yr=6100,
            cost_burdened_renter_households=1850,
            renter_households=3200,
            hud_eligible_households=1425,
            total_addressable_market=1650,
            income_qualified_base=980,
            market_depth_ratio=4.8,
        ),
        competitor_schools=[
            CompetitorSchool(
                name="Competitor A",
                lat=26.65,
                lon=-81.86,
                distance_miles=3.2,
                affiliation="Independent",
                is_catholic=False,
                city="Fort Myers",
                state="FL",
                zip_code="33901",
                enrollment=310,
                gender="Co-ed",
                grade_level="K-12",
                mds_overall_rating=4,
                total_units=80,
            ),
            CompetitorSchool(
                name="Competitor B",
                lat=26.67,
                lon=-81.84,
                distance_miles=4.8,
                affiliation="Catholic",
                is_catholic=True,
                city="Fort Myers",
                state="FL",
                zip_code="33916",
                enrollment=220,
                gender="Co-ed",
                grade_level="Elementary",
                mds_overall_rating=3,
                total_units=65,
            ),
        ],
        catholic_school_count=1,
        total_private_school_count=2,
        feasibility_score=FeasibilityScore(
            overall=68,
            scenario_conservative=60,
            scenario_optimistic=75,
            weighting_profile="standard_baseline",
            market_size=_metric("Market Size", 70),
            income=_metric("Income", 66),
            competition=_metric("Competition", 62),
            family_density=_metric("Family Density", 74),
            stage2=None,
        ),
        recommendation="Moderate Sustainability Conditions",
        recommendation_detail="Directional test payload",
        data_notes=[],
        run_mode="db_with_fallback",
        catchment_mode="radius",
        outcome="success",
        fallback_summary=FallbackSummary(used=False, notes=[]),
        confidence_summary=ConfidenceSummary(level="medium", contributors=[]),
        export_readiness=ExportReadiness(ready=True, status="ready", reasons=[]),
        data_freshness=DataFreshnessMetadata(
            mode="db_precomputed",
            generated_at_utc="2026-03-18T00:00:00+00:00",
            sources=[
                DataFreshnessSource(
                    source_key="census_acs",
                    source_label="US Census ACS",
                    status="fresh",
                    freshness_hours=24.0,
                )
            ],
        ),
    )


def _write_zcta_cache(path: Path):
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"zipCode": "33901", "name": "33901", "source": "test"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-81.95, 26.60], [-81.82, 26.60], [-81.82, 26.69], [-81.95, 26.69], [-81.95, 26.60]]],
                },
            },
            {
                "type": "Feature",
                "properties": {"zipCode": "33916", "name": "33916", "source": "test"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-81.88, 26.63], [-81.78, 26.63], [-81.78, 26.73], [-81.88, 26.73], [-81.88, 26.63]]],
                },
            },
            {
                "type": "Feature",
                "properties": {"zipCode": "99999", "name": "99999", "source": "test"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-82.4, 26.2], [-82.3, 26.2], [-82.3, 26.3], [-82.4, 26.3], [-82.4, 26.2]]],
                },
            },
        ],
    }
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        json.dump(payload, fh)


@pytest.mark.asyncio
async def test_projection_envelope_adds_bounds_to_projected_points():
    envelope = build_projection_envelope(
        [HistoricalValue(2020, 100), HistoricalValue(2021, 105), HistoricalValue(2022, 112)],
        [2023, 2024],
    )

    projected = [point for point in envelope.points if point.projected]
    assert projected
    assert all(point.lower_bound is not None for point in projected)
    assert all(point.upper_bound is not None for point in projected)
    assert envelope.confidence.band in {"high", "medium", "low"}


@pytest.mark.asyncio
async def test_dashboard_response_selects_intersecting_zips_not_seeded_fallbacks(tmp_path, monkeypatch):
    cache_path = tmp_path / "zcta.json.gz"
    _write_zcta_cache(cache_path)
    monkeypatch.setattr(dashboard_service, "ZCTA_CACHE_PATH", cache_path)
    dashboard_service._load_zcta_cache.cache_clear()

    async def _fake_db(*_args, **_kwargs):
        return {}, {}, None

    monkeypatch.setattr(dashboard_service, "_load_db_aggregates", _fake_db)

    request = AnalysisRequest(
        school_name="St. Example",
        address="123 Main St, Fort Myers, FL 33901",
        ministry_type="schools",
        mission_mode=False,
        drive_minutes=20,
        geography_mode="catchment",
        gender="coed",
        grade_level="k12",
        weighting_profile="standard_baseline",
        market_context="suburban",
        care_level="all",
    )
    payload = await dashboard_service.build_dashboard_response(
        request=request,
        result=_response("schools"),
        location={"matched_address": "123 Main St, Fort Myers, FL 33901"},
    )

    assert payload.metadata.geometry_source == "census_zcta_cache"
    assert payload.catchment.zip_codes == ["33901", "33916"]
    assert "33971" not in payload.catchment.zip_codes
    assert payload.catchment.geojson["features"][0]["properties"]["schoolAgePopulation"] > 0


@pytest.mark.asyncio
async def test_dashboard_response_reports_cache_unavailable_without_synthetic_geometry(tmp_path, monkeypatch):
    cache_path = tmp_path / "missing.json.gz"
    monkeypatch.setattr(dashboard_service, "ZCTA_CACHE_PATH", cache_path)
    dashboard_service._load_zcta_cache.cache_clear()

    async def _fake_db(*_args, **_kwargs):
        return {}, {}, None

    monkeypatch.setattr(dashboard_service, "_load_db_aggregates", _fake_db)

    request = AnalysisRequest(
        school_name="St. Example",
        address="123 Main St, Fort Myers, FL 33901",
        ministry_type="housing",
        mission_mode=False,
        drive_minutes=20,
        geography_mode="catchment",
        gender="coed",
        grade_level="k12",
        weighting_profile="standard_baseline",
        market_context="suburban",
        care_level="all",
        housing_target_population="all_ages",
    )
    payload = await dashboard_service.build_dashboard_response(
        request=request,
        result=_response("housing"),
        location={"matched_address": "123 Main St, Fort Myers, FL 33901"},
    )

    assert payload.metadata.geometry_source == "cache_unavailable"
    assert payload.catchment.zip_codes == []
    assert payload.catchment.geojson["features"] == []


@pytest.mark.asyncio
async def test_dashboard_endpoint_returns_additive_payload(tmp_path, monkeypatch):
    cache_path = tmp_path / "zcta.json.gz"
    _write_zcta_cache(cache_path)
    monkeypatch.setattr(dashboard_service, "ZCTA_CACHE_PATH", cache_path)
    dashboard_service._load_zcta_cache.cache_clear()

    async def _fake_db(*_args, **_kwargs):
        return {}, {}, None

    async def _fake_geocode(_address: str):
        return {
            "lat": 26.6406,
            "lon": -81.8723,
            "matched_address": "123 Main St, Fort Myers, FL 33901",
            "county_fips": "12071",
            "state_fips": "12",
            "county_name": "Lee County",
            "state_name": "Florida",
        }

    async def _fake_get_redis():
        return None

    async def _fake_run_analysis(_location, request, run_mode=None):
        return _response(request.ministry_type), {"fallback_notes": []}

    async def _fake_enrich(result, request):
        return result

    monkeypatch.setattr(dashboard_service, "_load_db_aggregates", _fake_db)
    monkeypatch.setattr(main, "geocode_address", _fake_geocode)
    monkeypatch.setattr(main, "_get_redis", _fake_get_redis)
    monkeypatch.setattr(main, "_run_analysis", _fake_run_analysis)
    monkeypatch.setattr(main, "_enrich_analysis_result", _fake_enrich)

    client = TestClient(main.app)
    response = client.post(
        "/api/dashboard",
        json={
            "school_name": "St. Example",
            "address": "123 Main St, Fort Myers, FL 33901",
            "ministry_type": "schools",
            "mission_mode": False,
            "drive_minutes": 20,
            "geography_mode": "catchment",
            "gender": "coed",
            "grade_level": "k12",
            "weighting_profile": "standard_baseline",
            "market_context": "suburban",
            "care_level": "all",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["catchment"]["zip_codes"] == ["33901", "33916"]
    assert body["metadata"]["geometry_source"] == "census_zcta_cache"
    assert body["data"]["metric_maps"]["schoolAgePopulation"]["33901"] > 0


def test_dashboard_endpoint_requires_zcta_cache(tmp_path, monkeypatch):
    cache_path = tmp_path / "missing.json.gz"
    monkeypatch.setattr(dashboard_service, "ZCTA_CACHE_PATH", cache_path)
    dashboard_service._load_zcta_cache.cache_clear()

    async def _fake_geocode(_address: str):
        return {
            "lat": 26.6406,
            "lon": -81.8723,
            "matched_address": "123 Main St, Fort Myers, FL 33901",
            "county_fips": "12071",
            "state_fips": "12",
            "county_name": "Lee County",
            "state_name": "Florida",
        }

    async def _fake_get_redis():
        return None

    monkeypatch.setattr(main, "geocode_address", _fake_geocode)
    monkeypatch.setattr(main, "_get_redis", _fake_get_redis)

    client = TestClient(main.app)
    response = client.post(
        "/api/dashboard",
        json={
            "school_name": "St. Example",
            "address": "123 Main St, Fort Myers, FL 33901",
            "ministry_type": "schools",
            "mission_mode": False,
            "drive_minutes": 20,
            "geography_mode": "catchment",
            "gender": "coed",
            "grade_level": "k12",
            "weighting_profile": "standard_baseline",
            "market_context": "suburban",
            "care_level": "all",
        },
    )

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["error_code"] == "ZCTA_CACHE_MISSING"
    assert "ingest-zcta" in body["detail"]["message"]
