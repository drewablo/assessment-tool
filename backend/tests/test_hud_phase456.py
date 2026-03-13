import types

import pytest

from api.reports import generate_csv_report
from models.schemas import AnalysisResponse, DemographicData, FeasibilityScore, MetricScore
from modules import housing


def _metric(score: int, label: str) -> MetricScore:
    return MetricScore(score=score, label=label, description="d", weight=25, rating="moderate")


def _housing_response() -> AnalysisResponse:
    return AnalysisResponse(
        school_name="HUD Test Project",
        ministry_type="housing",
        analysis_address="123 Main St",
        county_name="Cook",
        state_name="IL",
        lat=41.0,
        lon=-87.0,
        radius_miles=15,
        catchment_type="radius",
        demographics=DemographicData(
            total_population=10000,
            total_households=4000,
            median_household_income=55000,
            ministry_target_population=1000,
            hud_tenant_households=260,
            qct_designated_projects=2,
            dda_designated_projects=1,
        ),
        competitor_schools=[],
        catholic_school_count=0,
        total_private_school_count=0,
        feasibility_score=FeasibilityScore(
            overall=70,
            scenario_conservative=58,
            scenario_optimistic=82,
            market_size=_metric(72, "Cost-Burdened Households"),
            income=_metric(60, "Income Need"),
            competition=_metric(68, "LIHTC Saturation"),
            family_density=_metric(74, "Renter Burden Intensity"),
        ),
        recommendation="Moderate Affordable Housing Opportunity",
        recommendation_detail="Detail",
        data_notes=["HUD normalized enrichment active"],
    )


def test_score_housing_hud_context_boosts_scores():
    demographics = {
        "cost_burdened_renter_households": 1000,
        "median_household_income": 45000,
        "renter_households": 4000,
    }
    projects = [
        {"distance_miles": 1.0, "li_units": 100, "is_qct": True, "is_dda": False, "tenant_households": 120},
        {"distance_miles": 2.0, "li_units": 80, "is_qct": False, "is_dda": True, "tenant_households": 90},
    ]

    base = housing._score_housing(demographics, projects)
    enriched = housing._score_housing(
        demographics,
        projects,
        hud_context={"tenant_households": 210, "qct_projects": 1, "dda_projects": 1},
    )

    assert enriched["overall"] >= base["overall"]
    assert enriched["hud_market_boost"] > 0
    assert enriched["hud_competition_boost"] > 0


def test_generate_housing_csv_includes_hud_fields():
    csv_payload = generate_csv_report(_housing_response())
    assert "HUD Tenant Households (Joined)" in csv_payload
    assert "QCT-Designated Nearby Projects" in csv_payload
    assert "DDA-Designated Nearby Projects" in csv_payload


@pytest.mark.asyncio
async def test_analyze_housing_uses_enrichment_and_traceable_notes(monkeypatch):
    monkeypatch.setattr(housing, "USE_DB", True)

    async def fake_db_projects(*, lat: float, lon: float, radius_miles: float):
        return [
            {
                "name": "HUD One",
                "lat": lat,
                "lon": lon,
                "distance_miles": 1.2,
                "city": "Chicago",
                "li_units": 85,
                "is_qct": True,
                "is_dda": False,
                "tenant_households": 110,
            }
        ]

    async def fake_benchmarks(**kwargs):
        return None

    monkeypatch.setattr(housing, "_get_nearby_housing_db", fake_db_projects)
    monkeypatch.setattr(housing, "compute_module_benchmarks", fake_benchmarks)
    monkeypatch.setattr(housing, "build_generic_hierarchical", lambda **kwargs: None)

    request = types.SimpleNamespace(
        school_name="HUD Test",
        gender="coed",
        grade_level="k12",
        weighting_profile="standard_baseline",
        stage2_inputs=None,
    )
    location = {"lat": 41.88, "lon": -87.62, "matched_address": "A", "state_name": "Illinois", "state_fips": "17"}
    demographics = {
        "cost_burdened_renter_households": 800,
        "renter_households": 2200,
        "median_household_income": 52000,
        "total_households": 3000,
        "total_population": 9000,
    }

    result = await housing.analyze_housing(
        location=location,
        demographics=demographics,
        request=request,
        radius_miles=10,
        drive_minutes=20,
        isochrone_polygon=None,
        catchment_type="radius",
    )

    assert any("HUD normalized enrichment active" in note for note in result.data_notes)
    assert result.demographics.qct_designated_projects == 1
    assert result.demographics.hud_tenant_households == 110
