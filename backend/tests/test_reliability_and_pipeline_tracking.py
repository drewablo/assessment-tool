import types

import pytest

import main
from db import queries
from models.schemas import (
    ConfidenceSummary,
    DataDependencyStatus,
    ExportReadiness,
    FallbackSummary,
    SectionExplanation,
)
from pipeline.base import finish_pipeline_run


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


class _QueryCaptureSession:
    def __init__(self):
        self.params = None

    async def execute(self, stmt):
        self.params = stmt.compile().params
        return _FakeExecuteResult([])


class _PipelineSession:
    def __init__(self, runs):
        self.runs = runs
        self.flushed = False

    async def get(self, _model, run_id):
        return self.runs.get(run_id)

    async def flush(self):
        self.flushed = True


@pytest.mark.asyncio
async def test_get_tracts_by_county_accepts_three_and_five_digit_county_fips():
    session = _QueryCaptureSession()

    await queries.get_tracts_by_county(session, county_fips="031", state_fips="17")

    county_values = set(session.params["county_fips_1"])
    assert "031" in county_values
    assert "17031" in county_values


@pytest.mark.asyncio
async def test_finish_pipeline_run_updates_persisted_run_when_detached():
    detached = types.SimpleNamespace(id=5, status="running")
    persisted = types.SimpleNamespace(id=5, status="running")
    session = _PipelineSession({5: persisted})

    await finish_pipeline_run(session, detached, status="success", records_processed=10, records_inserted=8)

    assert session.flushed is True
    assert persisted.status == "success"
    assert persisted.records_processed == 10
    assert persisted.records_inserted == 8


def test_apply_reliability_metadata_builds_typed_nested_models():
    result = types.SimpleNamespace(
        catchment_type="radius",
        catchment_mode="radius",
        ministry_type="schools",
        demographics=types.SimpleNamespace(data_confidence="medium"),
        feasibility_score=types.SimpleNamespace(stage2=types.SimpleNamespace(readiness="ready")),
        outcome="success",
    )

    updated = main._apply_reliability_metadata(
        result,
        run_mode="db_with_fallback",
        dependency_counts={"census_tracts": 10, "hud_qct_dda": 0},
        fallback_notes=["db fallback"],
        strict_blockers=[],
    )

    assert isinstance(updated.fallback_summary, FallbackSummary)
    assert isinstance(updated.confidence_summary, ConfidenceSummary)
    assert isinstance(updated.export_readiness, ExportReadiness)
    assert updated.section_explanations
    assert all(isinstance(row, SectionExplanation) for row in updated.section_explanations)
    assert all(isinstance(row, DataDependencyStatus) for row in updated.data_dependencies)


def test_section_explanations_filters_missing_by_ministry_type():
    """inputs_missing should only include datasets relevant to the ministry type."""
    # Elder care analysis — only census_tracts and competitors_elder_care are relevant
    result = types.SimpleNamespace(
        catchment_type="radius",
        catchment_mode="radius",
        ministry_type="elder_care",
        demographics=types.SimpleNamespace(data_confidence="medium"),
        feasibility_score=types.SimpleNamespace(stage2=types.SimpleNamespace(readiness="ready")),
        outcome="success",
    )

    updated = main._apply_reliability_metadata(
        result,
        run_mode="db_with_fallback",
        dependency_counts={"census_tracts": 50, "competitors_elder_care": 15},
        fallback_notes=[],
        strict_blockers=[],
    )

    demo_section = [s for s in updated.section_explanations if s.section == "demographics_and_competition"][0]
    # Census and elder care competitors are available, so nothing should be missing
    assert demo_section.inputs_missing == [], (
        f"Elder care with census+competitors available should have no missing inputs, got: {demo_section.inputs_missing}"
    )
    # Schools/housing datasets should NOT appear even though they have 0 rows
    assert "competitors_schools" not in demo_section.inputs_missing
    assert "hud_lihtc_property" not in demo_section.inputs_missing

    # Census and competitors should show in inputs_used
    assert "census" in demo_section.inputs_used
    assert "cms_elder_care_facilities" in demo_section.inputs_used
