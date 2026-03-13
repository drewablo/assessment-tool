import types

from main import _request_from_compare
from models.schemas import AnalysisRequest, CompareAnalysisRequest
from modules import housing


def test_schema_accepts_housing_target_population_enum_values():
    senior_request = AnalysisRequest(
        school_name="Test",
        address="123 Main",
        ministry_type="housing",
        housing_target_population="senior_only",
    )
    all_ages_request = AnalysisRequest(
        school_name="Test",
        address="123 Main",
        ministry_type="housing",
        housing_target_population="all_ages",
    )

    assert senior_request.housing_target_population == "senior_only"
    assert all_ages_request.housing_target_population == "all_ages"


def test_compare_request_construction_preserves_housing_target_population():
    compare = CompareAnalysisRequest(
        school_name="Compare",
        address="123 Main",
        ministry_types=["housing", "schools"],
        housing_target_population="senior_only",
    )

    housing_request = _request_from_compare(compare, "housing")
    school_request = _request_from_compare(compare, "schools")

    assert housing_request.housing_target_population == "senior_only"
    assert school_request.housing_target_population == "senior_only"


def test_housing_scoring_changes_between_senior_only_and_all_ages():
    demographics = {
        "cost_burdened_renter_households": 900,
        "median_household_income": 47000,
        "renter_households": 2600,
        "seniors_65_plus": 15000,
    }
    projects = [{"distance_miles": 1.4, "li_units": 120}]

    all_ages = housing._score_housing(demographics, projects, target_population="all_ages")
    senior_only = housing._score_housing(demographics, projects, target_population="senior_only")

    assert all_ages["overall"] != senior_only["overall"]
    assert all_ages["family_density"] != senior_only["family_density"]


async def _fake_benchmarks(**kwargs):
    return None


async def _fake_projects(*, lat: float, lon: float, radius_miles: float):
    return [
        {
            "name": "Project One",
            "lat": lat,
            "lon": lon,
            "distance_miles": 1.0,
            "city": "City",
            "li_units": 80,
            "is_qct": False,
            "is_dda": False,
            "tenant_households": 40,
        }
    ]


import pytest


@pytest.mark.asyncio
async def test_housing_notes_reflect_selected_type(monkeypatch):
    monkeypatch.setattr(housing, "USE_DB", True)
    monkeypatch.setattr(housing, "_get_nearby_housing_db", _fake_projects)
    monkeypatch.setattr(housing, "compute_module_benchmarks", _fake_benchmarks)
    monkeypatch.setattr(housing, "build_generic_hierarchical", lambda **kwargs: None)

    location = {"lat": 41.0, "lon": -87.0, "matched_address": "A", "state_name": "IL", "state_fips": "17"}
    demographics = {
        "cost_burdened_renter_households": 850,
        "renter_households": 2400,
        "median_household_income": 50000,
        "total_households": 3000,
        "total_population": 9000,
        "seniors_65_plus": 12000,
    }

    senior_request = types.SimpleNamespace(
        school_name="Senior",
        gender="coed",
        grade_level="k12",
        weighting_profile="standard_baseline",
        stage2_inputs=None,
        housing_target_population="senior_only",
    )
    all_ages_request = types.SimpleNamespace(
        school_name="All Ages",
        gender="coed",
        grade_level="k12",
        weighting_profile="standard_baseline",
        stage2_inputs=None,
        housing_target_population="all_ages",
    )

    senior_result = await housing.analyze_housing(
        location=location,
        demographics=demographics,
        request=senior_request,
        radius_miles=10,
        drive_minutes=20,
        isochrone_polygon=None,
        catchment_type="radius",
    )
    all_ages_result = await housing.analyze_housing(
        location=location,
        demographics=demographics,
        request=all_ages_request,
        radius_miles=10,
        drive_minutes=20,
        isochrone_polygon=None,
        catchment_type="radius",
    )

    assert any("senior-only affordable housing" in note for note in senior_result.data_notes)
    assert any("serving all age groups" in note for note in all_ages_result.data_notes)


def test_non_housing_modules_unaffected_by_field():
    schools = AnalysisRequest(
        school_name="School",
        address="123 Main",
        ministry_type="schools",
        housing_target_population="senior_only",
    )
    elder = AnalysisRequest(
        school_name="Elder",
        address="123 Main",
        ministry_type="elder_care",
        housing_target_population="all_ages",
    )

    assert schools.ministry_type == "schools"
    assert elder.ministry_type == "elder_care"
