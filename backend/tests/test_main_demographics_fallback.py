import types

import pytest

import main
from models.schemas import AnalysisRequest


class _DummySession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def commit(self):
        return None

    def add(self, _obj):
        return None


@pytest.mark.asyncio
async def test_run_analysis_falls_back_to_live_demographics_when_db_empty(monkeypatch):
    monkeypatch.setattr(main, "USE_DB", True)

    async def fake_get_isochrone(_lat, _lon, _drive):
        return None

    async def fake_aggregate_demographics(**_kwargs):
        return {"tract_count": 0, "total_population": 0}

    async def fake_live_demographics(**_kwargs):
        return {"tract_count": 9, "total_population": 12345}

    async def fake_lookup_cached_isochrone(*_args, **_kwargs):
        return None

    async def fake_save_isochrone(*_args, **_kwargs):
        return None

    async def fake_data_freshness():
        return None

    def fake_benchmark(_result):
        return None

    class _Result:
        def __init__(self):
            self.ministry_type = None
            self.trace_id = None
            self.data_freshness = None
            self.benchmark_narrative = None
            self.recommendation = "ok"
            self.feasibility_score = types.SimpleNamespace(overall=70)

    async def fake_analyzer(**kwargs):
        assert kwargs["demographics"]["tract_count"] == 9
        return _Result()

    monkeypatch.setattr(main, "get_isochrone", fake_get_isochrone)
    monkeypatch.setattr(main, "get_demographics", fake_live_demographics)
    monkeypatch.setattr(main, "_build_data_freshness_metadata", fake_data_freshness)
    monkeypatch.setattr(main, "_build_benchmark_narrative", fake_benchmark)
    monkeypatch.setitem(main.MODULE_REGISTRY, "schools", types.SimpleNamespace(analyzer=fake_analyzer))

    # Patch DB-layer functions used by _run_analysis imports.
    import db.connection
    import db.demographics
    import db.queries

    monkeypatch.setattr(db.connection, "get_session", lambda: _DummySession())
    monkeypatch.setattr(db.demographics, "aggregate_demographics", fake_aggregate_demographics)
    monkeypatch.setattr(db.queries, "lookup_cached_isochrone", fake_lookup_cached_isochrone)
    monkeypatch.setattr(db.queries, "save_isochrone", fake_save_isochrone)

    request = AnalysisRequest(
        school_name="Fallback Test",
        address="123 Main St",
        ministry_type="schools",
    )
    location = {"lat": 41.0, "lon": -87.0, "state_fips": "17", "county_fips": "031"}

    result = await main._run_analysis(location, request)

    assert result.feasibility_score.overall == 70


@pytest.mark.asyncio
async def test_run_analysis_uses_db_demographics_when_available(monkeypatch):
    monkeypatch.setattr(main, "USE_DB", True)

    async def fake_get_isochrone(_lat, _lon, _drive):
        return None

    async def fake_aggregate_demographics(**_kwargs):
        return {"tract_count": 5, "total_population": 54321}

    async def should_not_call_live(**_kwargs):
        raise AssertionError("live demographics should not be called when DB demographics are available")

    async def fake_lookup_cached_isochrone(*_args, **_kwargs):
        return None

    async def fake_save_isochrone(*_args, **_kwargs):
        return None

    async def fake_data_freshness():
        return None

    def fake_benchmark(_result):
        return None

    class _Result:
        def __init__(self):
            self.ministry_type = None
            self.trace_id = None
            self.data_freshness = None
            self.benchmark_narrative = None
            self.recommendation = "ok"
            self.feasibility_score = types.SimpleNamespace(overall=70)

    async def fake_analyzer(**kwargs):
        assert kwargs["demographics"]["tract_count"] == 5
        return _Result()

    monkeypatch.setattr(main, "get_isochrone", fake_get_isochrone)
    monkeypatch.setattr(main, "get_demographics", should_not_call_live)
    monkeypatch.setattr(main, "_build_data_freshness_metadata", fake_data_freshness)
    monkeypatch.setattr(main, "_build_benchmark_narrative", fake_benchmark)
    monkeypatch.setitem(main.MODULE_REGISTRY, "schools", types.SimpleNamespace(analyzer=fake_analyzer))

    import db.connection
    import db.demographics
    import db.queries

    monkeypatch.setattr(db.connection, "get_session", lambda: _DummySession())
    monkeypatch.setattr(db.demographics, "aggregate_demographics", fake_aggregate_demographics)
    monkeypatch.setattr(db.queries, "lookup_cached_isochrone", fake_lookup_cached_isochrone)
    monkeypatch.setattr(db.queries, "save_isochrone", fake_save_isochrone)

    request = AnalysisRequest(
        school_name="DB Path Test",
        address="123 Main St",
        ministry_type="schools",
    )
    location = {"lat": 41.0, "lon": -87.0, "state_fips": "17", "county_fips": "031"}

    result = await main._run_analysis(location, request)

    assert result.feasibility_score.overall == 70
