"""Tests for HUD Section 202 ingestion, pipeline status, dependency policy,
analysis integration, and API serialization.

Follows the same testing conventions used for other ingestors in this codebase.
"""

import types
from unittest.mock import AsyncMock, patch

import pytest

from models.schemas import AnalysisRequest, CompetitorSchool
from pipeline.ingest_hud_section202 import (
    _rejection_reason,
    _to_int,
    _to_str,
    _transform_feature,
)
from services.dependency_policy import DEPENDENCY_REGISTRY, summarize_dependencies


# ---------------------------------------------------------------------------
# Unit tests: transform / normalization
# ---------------------------------------------------------------------------


def _make_feature(
    *,
    objectid="12345",
    name="Test Senior Residence",
    lon=-87.6298,
    lat=41.8781,
    **overrides,
):
    """Build a minimal GeoJSON feature dict."""
    props = {
        "OBJECTID": objectid,
        "SERVICING_SITE_NAME_TEXT": name,
        "PROPERTY_NAME_TEXT": overrides.get("property_name", "Alt Name"),
        "STD_ADDR": overrides.get("addr", "100 Main St"),
        "STD_CITY": overrides.get("city", "Chicago"),
        "STD_ST": overrides.get("state", "IL"),
        "STD_ZIP5": overrides.get("zip5", "60601"),
        "TOTAL_UNIT_COUNT": overrides.get("total_units", 80),
        "TOTAL_ASSISTED_UNIT_COUNT": overrides.get("assisted_units", 60),
        "CLIENT_GROUP_NAME": overrides.get("client_group", "Elderly"),
        "CLIENT_GROUP_TYPE": overrides.get("client_group_type", "Elderly"),
        "PROPERTY_CATEGORY_NAME": overrides.get("category", "Section 202/8"),
        "PRIMARY_FINANCING_TYPE": overrides.get("financing", "Section 202"),
        "PROPERTY_ON_SITE_PHONE_NUMBER": overrides.get("phone", "312-555-1234"),
        "REAC_LAST_INSPECTION_SCORE": overrides.get("reac_score", 85),
    }
    props.update({k: v for k, v in overrides.items() if k.isupper()})
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": props,
    }


def test_transform_feature_basic():
    feature = _make_feature()
    result = _transform_feature(feature)
    assert result is not None
    assert result["property_id"] == "12345"
    assert result["servicing_site_name"] == "Test Senior Residence"
    assert result["property_name"] == "Alt Name"
    assert result["street_address"] == "100 Main St"
    assert result["city"] == "Chicago"
    assert result["state"] == "IL"
    assert result["zip_code"] == "60601"
    assert result["lat"] == 41.8781
    assert result["lon"] == -87.6298
    assert result["total_units"] == 80
    assert result["total_assisted_units"] == 60
    assert result["client_group_name"] == "Elderly"
    assert result["property_category"] == "Section 202/8"
    assert result["primary_financing_type"] == "Section 202"
    assert result["phone_number"] == "312-555-1234"
    assert result["reac_inspection_score"] == 85
    assert isinstance(result["raw_payload"], dict)


def test_transform_feature_missing_name_rejected():
    feature = _make_feature()
    feature["properties"]["SERVICING_SITE_NAME_TEXT"] = None
    assert _transform_feature(feature) is None


def test_transform_feature_missing_coords_rejected():
    feature = _make_feature()
    feature["geometry"]["coordinates"] = []
    assert _transform_feature(feature) is None


def test_transform_feature_zero_coords_rejected():
    feature = _make_feature(lon=0, lat=0)
    assert _transform_feature(feature) is None


def test_transform_feature_missing_id_rejected():
    feature = _make_feature()
    feature["properties"]["OBJECTID"] = None
    # Still accepted if FHA_LOAN_ID_NUMBER exists
    feature["properties"]["FHA_LOAN_ID_NUMBER"] = "ABC123"
    result = _transform_feature(feature)
    assert result is not None
    assert result["property_id"] == "ABC123"


def test_transform_feature_no_id_at_all():
    feature = _make_feature()
    feature["properties"]["OBJECTID"] = None
    assert _transform_feature(feature) is None


def test_rejection_reason_missing_name():
    feature = _make_feature()
    feature["properties"]["SERVICING_SITE_NAME_TEXT"] = ""
    assert _rejection_reason(feature) == "missing_site_name"


def test_rejection_reason_missing_coordinates():
    feature = _make_feature()
    feature["geometry"]["coordinates"] = []
    assert _rejection_reason(feature) == "missing_coordinates"


def test_rejection_reason_valid():
    feature = _make_feature()
    assert _rejection_reason(feature) is None


# ---------------------------------------------------------------------------
# Unit tests: helper functions
# ---------------------------------------------------------------------------


def test_to_int():
    assert _to_int(42) == 42
    assert _to_int("85") == 85
    assert _to_int("3.7") == 3
    assert _to_int(None) is None
    assert _to_int("") is None
    assert _to_int("abc") is None


def test_to_str():
    assert _to_str("hello") == "hello"
    assert _to_str("  trimmed  ") == "trimmed"
    assert _to_str(None) is None
    assert _to_str("") is None
    assert _to_str("none") is None
    assert _to_str("null") is None
    assert _to_str("long_value", max_len=4) == "long"


# ---------------------------------------------------------------------------
# Idempotent upsert: transform produces deterministic output
# ---------------------------------------------------------------------------


def test_transform_idempotent():
    feature = _make_feature()
    r1 = _transform_feature(feature)
    r2 = _transform_feature(feature)
    # Remove raw_payload for comparison (dicts are equal)
    assert r1 == r2
    assert r1["property_id"] == r2["property_id"]


# ---------------------------------------------------------------------------
# Pipeline status and dependency policy
# ---------------------------------------------------------------------------


def test_section_202_in_dependency_registry():
    assert "hud_section_202" in DEPENDENCY_REGISTRY
    rule = DEPENDENCY_REGISTRY["hud_section_202"]
    assert rule.required is True
    assert rule.baseline_blocking is True
    assert rule.affects_confidence is True
    assert rule.export_blocking_in_strict is True


def test_summarize_dependencies_includes_section_202():
    counts = {
        "census_tracts": 1000,
        "competitors_schools": 500,
        "competitors_elder_care": 200,
        "hud_lihtc_property": 8000,
        "hud_lihtc_tenant": 5000,
        "hud_qct_dda": 3000,
        "hud_section_202": 0,
    }
    statuses = summarize_dependencies(counts)
    keys = [s.dataset for s in statuses]
    assert "hud_section_202" in keys
    s202 = next(s for s in statuses if s.dataset == "hud_section_202")
    assert s202.available is False
    assert s202.row_count == 0


def test_summarize_dependencies_section_202_available():
    counts = {"hud_section_202": 500}
    statuses = summarize_dependencies(counts)
    s202 = next(s for s in statuses if s.dataset == "hud_section_202")
    assert s202.available is True
    assert s202.row_count == 500


# ---------------------------------------------------------------------------
# CompetitorSchool serialization with Section 202 fields
# ---------------------------------------------------------------------------


def test_competitor_school_section_202_fields():
    cs = CompetitorSchool(
        name="Senior Village",
        lat=41.88,
        lon=-87.63,
        distance_miles=2.5,
        affiliation="HUD Section 202",
        is_catholic=False,
        city="Chicago",
        state="IL",
        street_address="100 Main St",
        zip_code="60601",
        enrollment=60,
        gender="N/A",
        grade_level="Section 202 Senior",
        total_units=80,
        client_group_name="Elderly",
        property_category="Section 202/8",
        primary_financing_type="Section 202",
        phone_number="312-555-1234",
        reac_inspection_score=85,
    )
    data = cs.model_dump()
    assert data["name"] == "Senior Village"
    assert data["affiliation"] == "HUD Section 202"
    assert data["street_address"] == "100 Main St"
    assert data["state"] == "IL"
    assert data["zip_code"] == "60601"
    assert data["total_units"] == 80
    assert data["reac_inspection_score"] == 85
    assert data["grade_level"] == "Section 202 Senior"


def test_competitor_school_without_section_202_fields():
    """Existing CompetitorSchool construction still works without new optional fields."""
    cs = CompetitorSchool(
        name="Regular Project",
        lat=41.0,
        lon=-87.0,
        distance_miles=1.0,
        affiliation="HUD LIHTC",
        is_catholic=False,
        enrollment=100,
    )
    data = cs.model_dump()
    assert data["total_units"] is None
    assert data["reac_inspection_score"] is None
    assert data["street_address"] is None


# ---------------------------------------------------------------------------
# Senior Housing analysis integration
# ---------------------------------------------------------------------------


async def _fake_projects(*, lat, lon, radius_miles):
    return [
        {
            "name": "LIHTC Project One",
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


async def _fake_section_202(*, lat, lon, radius_miles):
    return [
        {
            "name": "Section 202 Senior Villa",
            "lat": lat + 0.01,
            "lon": lon + 0.01,
            "distance_miles": 1.5,
            "city": "Nearby",
            "state": "IL",
            "street_address": "200 Oak Ave",
            "zip_code": "60602",
            "li_units": 45,
            "total_units": 60,
            "affiliation": "HUD Section 202",
            "is_catholic": False,
            "is_qct": False,
            "is_dda": False,
            "tenant_households": None,
            "property_name": "Villa Alt Name",
            "client_group_name": "Elderly",
            "property_category": "Section 202",
            "primary_financing_type": "Section 202",
            "phone_number": "312-555-9999",
            "reac_inspection_score": 90,
            "source_type": "hud_section_202",
        }
    ]


async def _empty_section_202(*, lat, lon, radius_miles):
    return []


async def _fake_benchmarks(**kwargs):
    return None


@pytest.mark.asyncio
async def test_senior_housing_includes_section_202(monkeypatch):
    from modules import housing

    monkeypatch.setattr(housing, "USE_DB", True)
    monkeypatch.setattr(housing, "_get_nearby_housing_db", _fake_projects)
    monkeypatch.setattr(housing, "_get_nearby_section_202_db", _fake_section_202)
    monkeypatch.setattr(housing, "compute_module_benchmarks", _fake_benchmarks)
    monkeypatch.setattr(housing, "build_generic_hierarchical", lambda **kwargs: None)

    location = {"lat": 41.0, "lon": -87.0, "matched_address": "A", "state_name": "IL", "state_fips": "17"}
    demographics = {
        "cost_burdened_renter_households": 900,
        "renter_households": 2500,
        "median_household_income": 48000,
        "total_households": 3000,
        "total_population": 9000,
        "seniors_65_plus": 14000,
    }

    request = types.SimpleNamespace(
        school_name="Senior Analysis",
        gender="coed",
        grade_level="k12",
        weighting_profile="standard_baseline",
        stage2_inputs=None,
        housing_target_population="senior_only",
    )

    result = await housing.analyze_housing(
        location=location,
        demographics=demographics,
        request=request,
        radius_miles=10,
        drive_minutes=20,
        isochrone_polygon=None,
        catchment_type="radius",
    )

    # Should include both LIHTC and Section 202 properties
    assert result.total_private_school_count == 2  # 1 LIHTC + 1 Section 202
    affiliations = [c.affiliation for c in result.competitor_schools]
    assert "HUD Section 202" in affiliations
    assert any("HUD LIHTC" in a for a in affiliations)

    # Section 202 competitor should have correct display fields
    s202 = next(c for c in result.competitor_schools if c.affiliation == "HUD Section 202")
    assert s202.name == "Section 202 Senior Villa"
    assert s202.city == "Nearby"
    assert s202.street_address == "200 Oak Ave"
    assert s202.state == "IL"
    assert s202.zip_code == "60602"
    assert s202.grade_level == "Section 202 Senior"
    assert s202.total_units == 60
    assert s202.enrollment == 45  # li_units mapped to enrollment
    assert s202.reac_inspection_score == 90

    # Data notes should mention Section 202
    assert any("Section 202" in note for note in result.data_notes)


@pytest.mark.asyncio
async def test_all_ages_housing_excludes_section_202(monkeypatch):
    from modules import housing

    monkeypatch.setattr(housing, "USE_DB", True)
    monkeypatch.setattr(housing, "_get_nearby_housing_db", _fake_projects)
    monkeypatch.setattr(housing, "_get_nearby_section_202_db", _fake_section_202)
    monkeypatch.setattr(housing, "compute_module_benchmarks", _fake_benchmarks)
    monkeypatch.setattr(housing, "build_generic_hierarchical", lambda **kwargs: None)

    location = {"lat": 41.0, "lon": -87.0, "matched_address": "A", "state_name": "IL", "state_fips": "17"}
    demographics = {
        "cost_burdened_renter_households": 900,
        "renter_households": 2500,
        "median_household_income": 48000,
        "total_households": 3000,
        "total_population": 9000,
        "seniors_65_plus": 14000,
    }

    request = types.SimpleNamespace(
        school_name="All Ages",
        gender="coed",
        grade_level="k12",
        weighting_profile="standard_baseline",
        stage2_inputs=None,
        housing_target_population="all_ages",
    )

    result = await housing.analyze_housing(
        location=location,
        demographics=demographics,
        request=request,
        radius_miles=10,
        drive_minutes=20,
        isochrone_polygon=None,
        catchment_type="radius",
    )

    # Should only include LIHTC projects, not Section 202
    assert result.total_private_school_count == 1
    affiliations = [c.affiliation for c in result.competitor_schools]
    assert "HUD Section 202" not in affiliations


@pytest.mark.asyncio
async def test_senior_housing_graceful_without_section_202(monkeypatch):
    """When Section 202 data is absent, Senior Housing analysis still works."""
    from modules import housing

    monkeypatch.setattr(housing, "USE_DB", True)
    monkeypatch.setattr(housing, "_get_nearby_housing_db", _fake_projects)
    monkeypatch.setattr(housing, "_get_nearby_section_202_db", _empty_section_202)
    monkeypatch.setattr(housing, "compute_module_benchmarks", _fake_benchmarks)
    monkeypatch.setattr(housing, "build_generic_hierarchical", lambda **kwargs: None)

    location = {"lat": 41.0, "lon": -87.0, "matched_address": "A", "state_name": "IL", "state_fips": "17"}
    demographics = {
        "cost_burdened_renter_households": 900,
        "renter_households": 2500,
        "median_household_income": 48000,
        "total_households": 3000,
        "total_population": 9000,
        "seniors_65_plus": 14000,
    }

    request = types.SimpleNamespace(
        school_name="Senior No 202",
        gender="coed",
        grade_level="k12",
        weighting_profile="standard_baseline",
        stage2_inputs=None,
        housing_target_population="senior_only",
    )

    result = await housing.analyze_housing(
        location=location,
        demographics=demographics,
        request=request,
        radius_miles=10,
        drive_minutes=20,
        isochrone_polygon=None,
        catchment_type="radius",
    )

    # Should still work with just LIHTC data
    assert result.total_private_school_count == 1
    assert result.feasibility_score.overall > 0
    # Should note that Section 202 data was not available
    assert any("not available" in note.lower() or "202" in note for note in result.data_notes)


# ---------------------------------------------------------------------------
# Pipeline diagnostics
# ---------------------------------------------------------------------------


def test_pipeline_diagnostics_section_202_missing():
    """Verify _build_pipeline_diagnostics includes Section 202 when data is missing.

    Uses importlib to selectively import _build_pipeline_diagnostics, skipping
    the test gracefully if full app dependencies are unavailable.
    """
    try:
        from main import _build_pipeline_diagnostics
    except ImportError:
        pytest.skip("main module requires full app dependencies (fastapi, anthropic, etc.)")

    counts = {
        "census_tracts": 100,
        "schools": 50,
        "elder_care_facilities": 30,
        "housing_projects": 20,
        "hud_lihtc_property": 100,
        "hud_lihtc_tenant": 50,
        "hud_qct_dda": 30,
        "hud_section_202": 0,
    }
    pipelines = {
        "census_acs": {"last_success": "2024-01-01T00:00:00", "freshness_status": "fresh"},
        "nces_pss": {"last_success": "2024-01-01T00:00:00", "freshness_status": "fresh"},
        "cms_elder_care": {"last_success": "2024-01-01T00:00:00", "freshness_status": "fresh"},
        "hud_lihtc_property": {"last_success": "2024-01-01T00:00:00", "freshness_status": "fresh"},
        "hud_lihtc_tenant": {"last_success": "2024-01-01T00:00:00", "freshness_status": "fresh"},
        "hud_qct_dda": {"last_success": "2024-01-01T00:00:00", "freshness_status": "fresh"},
        "hud_section_202": {},
    }

    diagnostics, db_ready, status = _build_pipeline_diagnostics(counts, pipelines)
    # Section 202 is now required — empty table blocks readiness
    assert db_ready is False
    assert status == "not_ready"
    # Blocking diagnostic about it
    assert any("hud_section_202" in d and "0 rows" in d for d in diagnostics)


# ---------------------------------------------------------------------------
# Pagination: exceededTransferLimit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_all_features_paginates_on_exceeded_transfer_limit(monkeypatch):
    """_fetch_all_features must continue fetching when exceededTransferLimit is True,
    even if the page has fewer features than ARCGIS_PAGE_SIZE."""
    from pipeline import ingest_hud_section202 as mod

    page1 = {
        "features": [_make_feature(objectid=str(i)) for i in range(1000)],
        "exceededTransferLimit": True,
    }
    page2 = {
        "features": [_make_feature(objectid=str(i)) for i in range(1000, 2000)],
        "exceededTransferLimit": True,
    }
    page3 = {
        "features": [_make_feature(objectid=str(i)) for i in range(2000, 2500)],
        # No exceededTransferLimit → last page
    }
    pages = [page1, page2, page3]
    call_count = 0

    async def fake_fetch_page(url, offset=0):
        nonlocal call_count
        idx = call_count
        call_count += 1
        return pages[idx]

    monkeypatch.setattr(mod, "_fetch_geojson_page", fake_fetch_page)
    # Set page size higher than any single page to verify old logic would break
    monkeypatch.setattr(mod, "ARCGIS_PAGE_SIZE", 2000)

    features = await mod._fetch_all_features()
    assert len(features) == 2500
    assert call_count == 3


@pytest.mark.asyncio
async def test_fetch_all_features_stops_without_exceeded_flag(monkeypatch):
    """When exceededTransferLimit is absent, pagination should stop after that page."""
    from pipeline import ingest_hud_section202 as mod

    page1 = {
        "features": [_make_feature(objectid=str(i)) for i in range(500)],
        # No exceededTransferLimit → only page
    }

    async def fake_fetch_page(url, offset=0):
        return page1

    monkeypatch.setattr(mod, "_fetch_geojson_page", fake_fetch_page)
    monkeypatch.setattr(mod, "ARCGIS_PAGE_SIZE", 2000)

    features = await mod._fetch_all_features()
    assert len(features) == 500
