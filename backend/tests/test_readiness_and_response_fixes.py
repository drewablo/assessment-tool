"""Tests for readiness computation, census catchment fallbacks,
dependency policy typed returns, and Pydantic response model construction.
"""

import types

import pytest

from models.schemas import (
    AnalysisResponse,
    ConfidenceSummary,
    DataDependencyStatus,
    DemographicData,
    ExportReadiness,
    FallbackSummary,
    FeasibilityScore,
    MetricScore,
    SectionExplanation,
)
from services.dependency_policy import summarize_dependencies


# ---------------------------------------------------------------------------
# summarize_dependencies returns typed DataDependencyStatus instances
# ---------------------------------------------------------------------------


class TestSummarizeDependencies:
    def test_returns_list_of_typed_instances(self):
        counts = {"census_tracts": 100, "hud_qct_dda": 0}
        result = summarize_dependencies(counts)
        assert isinstance(result, list)
        assert all(isinstance(r, DataDependencyStatus) for r in result)

    def test_none_counts_produces_all_unavailable(self):
        result = summarize_dependencies(None)
        for dep in result:
            assert dep.row_count == 0
            assert dep.available is False

    def test_available_flag_matches_count(self):
        result = summarize_dependencies({"census_tracts": 5, "hud_qct_dda": 0})
        by_key = {r.dataset: r for r in result}
        assert by_key["census_tracts"].available is True
        assert by_key["census_tracts"].row_count == 5
        assert by_key["hud_qct_dda"].available is False
        assert by_key["hud_qct_dda"].row_count == 0


# ---------------------------------------------------------------------------
# AnalysisResponse validate_assignment coerces dicts to typed models
# ---------------------------------------------------------------------------


def _minimal_response(**overrides) -> AnalysisResponse:
    """Build a minimal valid AnalysisResponse for testing."""
    defaults = dict(
        school_name="Test",
        analysis_address="123 Main St",
        county_name="Test County",
        state_name="Test State",
        lat=41.0,
        lon=-87.0,
        radius_miles=10.0,
        demographics=DemographicData(total_population=1000),
        competitor_schools=[],
        catholic_school_count=0,
        total_private_school_count=0,
        feasibility_score=FeasibilityScore(
            overall=75,
            market_size=MetricScore(score=80, label="Market Size", description="", weight=30, rating="strong"),
            income=MetricScore(score=70, label="Income", description="", weight=25, rating="moderate"),
            competition=MetricScore(score=60, label="Competition", description="", weight=25, rating="moderate"),
            family_density=MetricScore(score=65, label="Family Density", description="", weight=20, rating="moderate"),
        ),
        recommendation="Viable",
        recommendation_detail="Test detail",
    )
    defaults.update(overrides)
    return AnalysisResponse(**defaults)


class TestResponseModelConstruction:
    def test_validate_assignment_coerces_fallback_summary_dict_to_model(self):
        resp = _minimal_response()
        # Assigning a dict should auto-coerce to FallbackSummary
        resp.fallback_summary = {"used": True, "notes": ["test"]}
        assert isinstance(resp.fallback_summary, FallbackSummary)
        assert resp.fallback_summary.used is True
        assert resp.fallback_summary.notes == ["test"]

    def test_validate_assignment_coerces_confidence_summary_dict_to_model(self):
        resp = _minimal_response()
        resp.confidence_summary = {"level": "high", "contributors": ["a", "b"]}
        assert isinstance(resp.confidence_summary, ConfidenceSummary)
        assert resp.confidence_summary.level == "high"

    def test_validate_assignment_coerces_export_readiness_dict_to_model(self):
        resp = _minimal_response()
        resp.export_readiness = {"ready": True, "status": "ready", "reasons": []}
        assert isinstance(resp.export_readiness, ExportReadiness)
        assert resp.export_readiness.ready is True

    def test_validate_assignment_coerces_data_dependencies_list_of_dicts(self):
        resp = _minimal_response()
        resp.data_dependencies = [
            {
                "dataset": "census_tracts",
                "required": True,
                "baseline_blocking": True,
                "affects_confidence": True,
                "export_blocking_in_strict": True,
                "available": True,
                "row_count": 100,
            }
        ]
        assert all(isinstance(d, DataDependencyStatus) for d in resp.data_dependencies)

    def test_validate_assignment_coerces_section_explanations_list_of_dicts(self):
        resp = _minimal_response()
        resp.section_explanations = [
            {"section": "catchment", "inputs_used": ["radius"], "inputs_missing": [], "fallback_used": [], "confidence_impact": "low"}
        ]
        assert all(isinstance(s, SectionExplanation) for s in resp.section_explanations)

    def test_model_dump_json_succeeds_without_warnings(self):
        resp = _minimal_response()
        resp.fallback_summary = FallbackSummary(used=False, notes=[])
        resp.confidence_summary = ConfidenceSummary(level="medium", contributors=["test"])
        resp.export_readiness = ExportReadiness(ready=True, status="ready", reasons=[])
        resp.data_dependencies = summarize_dependencies({"census_tracts": 10})
        resp.section_explanations = [
            SectionExplanation(section="test", inputs_used=["a"], inputs_missing=[], fallback_used=[], confidence_impact="none")
        ]
        # Should not raise or produce warnings
        json_str = resp.model_dump_json()
        assert '"fallback_summary"' in json_str
        assert '"confidence_summary"' in json_str

    def test_model_validate_json_round_trip_preserves_types(self):
        resp = _minimal_response()
        resp.fallback_summary = FallbackSummary(used=True, notes=["test"])
        resp.confidence_summary = ConfidenceSummary(level="high", contributors=["x"])
        resp.export_readiness = ExportReadiness(ready=True, status="ready", reasons=[])
        resp.data_dependencies = summarize_dependencies({"census_tracts": 5})
        resp.section_explanations = [
            SectionExplanation(section="test", inputs_used=["a"], inputs_missing=[], fallback_used=[], confidence_impact="none")
        ]

        json_str = resp.model_dump_json()
        restored = AnalysisResponse.model_validate_json(json_str)

        assert isinstance(restored.fallback_summary, FallbackSummary)
        assert restored.fallback_summary.used is True
        assert isinstance(restored.confidence_summary, ConfidenceSummary)
        assert restored.confidence_summary.level == "high"
        assert isinstance(restored.export_readiness, ExportReadiness)
        assert all(isinstance(d, DataDependencyStatus) for d in restored.data_dependencies)
        assert all(isinstance(s, SectionExplanation) for s in restored.section_explanations)


# ---------------------------------------------------------------------------
# Census catchment county FIPS lookup
# ---------------------------------------------------------------------------


class _FakeScalarResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeExecuteResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return _FakeScalarResult(self._rows)

    def scalar(self):
        return self._rows[0] if self._rows else None


class _InstrumentedSession:
    """Captures the IN-clause values from the executed statement."""

    def __init__(self):
        self.captured_in_values = None

    async def execute(self, stmt):
        try:
            compiled = stmt.compile(compile_kwargs={"literal_binds": True})
            sql_str = str(compiled)
            # Extract the IN values for inspection
            self.last_sql = sql_str
        except Exception:
            pass
        # Return empty results
        return _FakeExecuteResult([])


@pytest.mark.asyncio
async def test_get_tracts_by_county_handles_five_digit_fips():
    """5-digit FIPS from geocoder (e.g. '17031') should match DB format."""
    from db import queries

    session = _InstrumentedSession()
    result = await queries.get_tracts_by_county(session, county_fips="17031", state_fips="17")
    # Should include "17031" as a candidate (already 5-digit)
    assert "17031" in session.last_sql


@pytest.mark.asyncio
async def test_get_tracts_by_county_handles_three_digit_fips():
    """3-digit county code (e.g. '031') should be expanded to 5-digit with state prefix."""
    from db import queries

    session = _InstrumentedSession()
    result = await queries.get_tracts_by_county(session, county_fips="031", state_fips="17")
    # Should include both "031" and "17031" as candidates
    assert "17031" in session.last_sql
