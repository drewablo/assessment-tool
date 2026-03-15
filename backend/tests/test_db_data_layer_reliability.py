import types

import pytest

from db import demographics as db_demographics
from modules import elder_care as elder_care_module
from modules import housing as housing_module


class _FakeSessionCtx:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_housing_db_uses_legacy_table_when_normalized_missing(monkeypatch):
    from db import connection, queries

    async def fake_latest_year(_session):
        return None

    async def fake_legacy(_session, **_kwargs):
        row = types.SimpleNamespace(
            project_name="Legacy HUD",
            lat=40.1,
            lon=-75.2,
            city="Flourtown",
            low_income_units=25,
        )
        return [(row, 1.234)]

    monkeypatch.setattr(connection, "get_session", lambda: _FakeSessionCtx())
    monkeypatch.setattr(queries, "get_latest_hud_property_dataset_year", fake_latest_year)
    monkeypatch.setattr(queries, "get_nearby_housing", fake_legacy)

    rows = await housing_module._get_nearby_housing_db(lat=40.1, lon=-75.2, radius_miles=10)

    assert len(rows) == 1
    assert rows[0]["name"] == "Legacy HUD"
    assert rows[0]["li_units"] == 25


@pytest.mark.asyncio
async def test_aggregate_demographics_uses_county_fallback_when_spatial_empty(monkeypatch):
    async def fake_spatial(*_args, **_kwargs):
        return []

    tract = types.SimpleNamespace(
        geoid="42091234567",
        state_fips="42",
        county_fips="42091",
        total_population=1000,
        population_under_18=200,
        population_5_17=120,
        population_under_5=60,
        population_65_74=80,
        population_75_plus=70,
        total_households=400,
        family_households=250,
        families_with_own_children=140,
        owner_occupied=220,
        renter_occupied=180,
        population_below_poverty=120,
        seniors_below_poverty=20,
        seniors_living_alone=40,
        enrolled_k_12=130,
        enrolled_private_k_12=20,
        income_bracket_under_10k=10,
        income_bracket_10k_15k=15,
        income_bracket_15k_25k=20,
        income_bracket_25k_35k=30,
        income_bracket_35k_50k=40,
        income_bracket_50k_75k=50,
        income_bracket_75k_100k=45,
        income_bracket_100k_150k=35,
        income_bracket_150k_200k=20,
        income_bracket_200k_plus=10,
        median_household_income=65000,
        income_cv=15.0,
        centroid=None,
    )

    async def fake_county(*_args, **_kwargs):
        return [tract]

    async def fake_hist(*_args, **_kwargs):
        return []

    monkeypatch.setattr(db_demographics, "get_tracts_in_catchment", fake_spatial)
    monkeypatch.setattr(db_demographics, "get_tracts_by_county", fake_county)
    monkeypatch.setattr(db_demographics, "get_historical_tracts", fake_hist)

    out = await db_demographics.aggregate_demographics(
        session=object(),
        lat=40.1,
        lon=-75.2,
        radius_miles=10,
        state_fips="42",
        county_fips="42091",
        isochrone_geojson=None,
    )

    assert out["tract_count"] == 1
    assert out["data_geography"] == "county_fallback"


@pytest.mark.asyncio
async def test_elder_care_uses_db_first_when_rows_present(monkeypatch):
    monkeypatch.setattr(elder_care_module, "USE_DB", True)

    async def fake_db(**_kwargs):
        return [{"name": "DB Facility", "lat": 40.1, "lon": -75.2, "distance_miles": 1.0, "certified_beds": 80, "occupancy_pct": 85.0}]

    async def should_not_call_live(*_args, **_kwargs):
        raise AssertionError("live fetch should not run")

    async def fake_benchmarks(**_kwargs):
        return None

    monkeypatch.setattr(elder_care_module, "_get_nearby_elder_care_db", fake_db)
    monkeypatch.setattr(elder_care_module, "get_nearby_elder_care_facilities", should_not_call_live)
    monkeypatch.setattr(elder_care_module, "compute_module_benchmarks", fake_benchmarks)
    monkeypatch.setattr(elder_care_module, "build_generic_hierarchical", lambda **kwargs: None)

    request = types.SimpleNamespace(
        school_name="x",
        care_level="all",
        min_mds_overall_rating=None,
        mission_mode=False,
        stage2_inputs=None,
        gender="coed",
        grade_level="k_8",
        weighting_profile="standard_baseline",
    )
    location = {"lat": 40.1, "lon": -75.2, "matched_address": "a", "state_name": "PA", "county_fips": "42091", "state_fips": "42"}
    demographics = {"seniors_65_plus": 1000, "seniors_75_plus": 600, "median_household_income": 55000, "total_households": 4000, "total_population": 9000}

    result = await elder_care_module.analyze_elder_care(
        location=location,
        demographics=demographics,
        request=request,
        radius_miles=10,
        drive_minutes=15,
        isochrone_polygon=None,
        catchment_type="radius",
    )

    assert result.total_private_school_count == 1


@pytest.mark.asyncio
async def test_housing_analysis_degrades_gracefully_without_tenant_or_qct(monkeypatch):
    monkeypatch.setattr(housing_module, "USE_DB", True)

    async def fake_housing_db(**_kwargs):
        return [{"name": "HUD Property", "lat": 40.1, "lon": -75.2, "distance_miles": 1.0, "city": "Flourtown", "li_units": 30}]

    async def fake_benchmarks(**_kwargs):
        return None

    monkeypatch.setattr(housing_module, "_get_nearby_housing_db", fake_housing_db)
    monkeypatch.setattr(housing_module, "compute_module_benchmarks", fake_benchmarks)
    monkeypatch.setattr(housing_module, "build_generic_hierarchical", lambda **kwargs: None)

    request = types.SimpleNamespace(
        school_name="x",
        mission_mode=False,
        stage2_inputs=None,
        gender="coed",
        grade_level="k_8",
        weighting_profile="standard_baseline",
        housing_target_population="all_ages",
    )
    location = {"lat": 40.1, "lon": -75.2, "matched_address": "a", "state_name": "PA", "county_fips": "42091", "state_fips": "42"}
    demographics = {
        "cost_burdened_renter_households": 800,
        "renter_households": 1600,
        "median_household_income": 55000,
        "total_households": 4000,
        "total_population": 9000,
    }

    result = await housing_module.analyze_housing(
        location=location,
        demographics=demographics,
        request=request,
        radius_miles=10,
        drive_minutes=15,
        isochrone_polygon=None,
        catchment_type="radius",
    )

    assert result.total_private_school_count == 1
    assert result.feasibility_score.overall >= 0
