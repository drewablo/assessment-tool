from models.schemas import (
    AnalysisRequest,
    AnalysisResponse,
    CompetitorSchool,
    ConfidenceSummary,
    DashboardResponse,
    DataFreshnessMetadata,
    DataFreshnessSource,
    DemographicData,
    ExportReadiness,
    FeasibilityScore,
    FallbackSummary,
    MetricScore,
)
from services.dashboard_service import build_dashboard_response
from services.projections import HistoricalValue, build_projection_envelope


def _metric(label: str, score: int, weight: int = 25) -> MetricScore:
    return MetricScore(score=score, label=label, description=label, weight=weight, rating="moderate")


def _response(ministry_type: str) -> AnalysisResponse:
    return AnalysisResponse(
        school_name="St. Example",
        ministry_type=ministry_type,
        analysis_address="123 Main St, Fort Myers, FL 33901",
        county_name="Lee County",
        state_name="Florida",
        lat=26.6406,
        lon=-81.8723,
        radius_miles=8.0,
        catchment_minutes=20,
        catchment_type="isochrone",
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
        catchment_mode="isochrone",
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


def test_projection_envelope_adds_bounds_to_projected_points():
    envelope = build_projection_envelope(
        [HistoricalValue(2020, 100), HistoricalValue(2021, 105), HistoricalValue(2022, 112)],
        [2023, 2024],
    )

    projected = [point for point in envelope.points if point.projected]
    assert projected
    assert all(point.lower_bound is not None for point in projected)
    assert all(point.upper_bound is not None for point in projected)
    assert envelope.confidence.band in {"high", "medium", "low"}



def test_dashboard_response_includes_zip_metrics_and_drilldowns():
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
    payload = build_dashboard_response(
        request=request,
        result=_response("schools"),
        location={"matched_address": "123 Main St, Fort Myers, FL 33901"},
    )

    assert isinstance(payload, DashboardResponse)
    assert payload.catchment.zip_codes
    assert payload.catchment.geojson["type"] == "FeatureCollection"
    assert "familiesWithChildren" in payload.data.metric_maps
    assert payload.data.drilldowns[payload.catchment.zip_codes[0]].metrics
    assert payload.metadata.projection_years



def test_housing_dashboard_maps_existing_resource_metrics():
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
    payload = build_dashboard_response(
        request=request,
        result=_response("housing"),
        location={"matched_address": "123 Main St, Fort Myers, FL 33901"},
    )

    assert "costBurdenedHouseholds" in payload.data.metric_maps
    assert payload.data.highlight_cards[2].value == "145"
