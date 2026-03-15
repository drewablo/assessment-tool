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

    result, context = await main._run_analysis(location, request)

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

    result, context = await main._run_analysis(location, request)

    assert result.feasibility_score.overall == 70


@pytest.mark.asyncio
async def test_run_analysis_reports_effective_source_counts(monkeypatch):
    """Verify that _run_analysis context includes effective_source_counts
    so dependency tracking reflects actual data usage, not just DB table counts."""
    monkeypatch.setattr(main, "USE_DB", False)

    async def fake_get_isochrone(_lat, _lon, _drive):
        return None

    async def fake_live_demographics(**_kwargs):
        return {"tract_count": 12, "total_population": 5000}

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
            self.total_private_school_count = 8
            self.competitor_schools = [{"name": f"Facility {i}"} for i in range(8)]

    async def fake_analyzer(**kwargs):
        return _Result()

    monkeypatch.setattr(main, "get_isochrone", fake_get_isochrone)
    monkeypatch.setattr(main, "get_demographics", fake_live_demographics)
    monkeypatch.setattr(main, "_build_data_freshness_metadata", fake_data_freshness)
    monkeypatch.setattr(main, "_build_benchmark_narrative", fake_benchmark)
    monkeypatch.setitem(main.MODULE_REGISTRY, "elder_care", types.SimpleNamespace(analyzer=fake_analyzer))

    request = AnalysisRequest(
        school_name="Source Count Test",
        address="123 Main St",
        ministry_type="elder_care",
    )
    location = {"lat": 41.0, "lon": -87.0, "state_fips": "17", "county_fips": "031"}

    result, context = await main._run_analysis(location, request)

    esc = context["effective_source_counts"]
    assert esc["census_tracts"] == 12, "Census tracts should reflect actual tract_count from demographics"
    assert esc["competitors_elder_care"] == 8, "Elder care competitors should reflect actual facility count"
    assert "competitors_schools" not in esc, "Schools competitor key should not appear for elder_care ministry"


@pytest.mark.asyncio
async def test_run_analysis_enriches_legacy_elder_demographics_from_live(monkeypatch):
    monkeypatch.setattr(main, "USE_DB", True)

    async def fake_get_isochrone(_lat, _lon, _drive):
        return None

    async def fake_aggregate_demographics(**_kwargs):
        return {
            "tract_count": 12,
            "total_population": 50000,
            "seniors_65_plus": 3200,
            "seniors_75_plus": 1200,
            "seniors_living_alone": 0,
            "seniors_below_200pct_poverty": 0,
        }

    live_calls = {"count": 0}

    async def fake_live_demographics(**_kwargs):
        live_calls["count"] += 1
        return {
            "tract_count": 12,
            "total_population": 50000,
            "seniors_65_plus": 3200,
            "seniors_75_plus": 1200,
            "seniors_living_alone": 640,
            "seniors_below_200pct_poverty": 410,
            "seniors_by_direction": {"N": {"seniors_75_plus": 300}},
        }

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
        demo = kwargs["demographics"]
        assert demo["seniors_living_alone"] == 640
        assert demo["seniors_below_200pct_poverty"] == 410
        assert demo.get("senior_metrics_source") == "live_enriched"
        return _Result()

    monkeypatch.setattr(main, "get_isochrone", fake_get_isochrone)
    monkeypatch.setattr(main, "get_demographics", fake_live_demographics)
    monkeypatch.setattr(main, "_build_data_freshness_metadata", fake_data_freshness)
    monkeypatch.setattr(main, "_build_benchmark_narrative", fake_benchmark)
    monkeypatch.setitem(main.MODULE_REGISTRY, "elder_care", types.SimpleNamespace(analyzer=fake_analyzer))

    import db.connection
    import db.demographics
    import db.queries

    monkeypatch.setattr(db.connection, "get_session", lambda: _DummySession())
    monkeypatch.setattr(db.demographics, "aggregate_demographics", fake_aggregate_demographics)
    monkeypatch.setattr(db.queries, "lookup_cached_isochrone", fake_lookup_cached_isochrone)
    monkeypatch.setattr(db.queries, "save_isochrone", fake_save_isochrone)

    request = AnalysisRequest(
        school_name="Legacy Senior Data Test",
        address="123 Main St",
        ministry_type="elder_care",
    )
    location = {"lat": 41.0, "lon": -87.0, "state_fips": "17", "county_fips": "031"}

    _result, context = await main._run_analysis(location, request)

    assert live_calls["count"] == 1
    assert context["used_live_senior_enrichment"] is True
    assert any("senior demographics incomplete" in note.lower() for note in context["fallback_notes"])
