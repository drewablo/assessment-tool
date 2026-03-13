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
