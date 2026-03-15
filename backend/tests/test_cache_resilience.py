import json
import pytest

import main
from models.schemas import AnalysisRequest, CompareAnalysisRequest


class _FakeRedis:
    def __init__(self, payload_by_key=None):
        self.payload_by_key = payload_by_key or {}
        self.set_calls = 0

    async def get(self, key: str):
        return self.payload_by_key.get(key)

    async def setex(self, key, _ttl, value):
        self.set_calls += 1
        self.payload_by_key[key] = value


class _Score:
    def __init__(self, overall):
        self.overall = overall
        self.scenario_conservative = overall - 10
        self.scenario_optimistic = overall + 10
        self.stage2 = None


class _Result:
    def __init__(self, ministry_type, overall):
        self.ministry_type = ministry_type
        self.feasibility_score = _Score(overall)
        self.recommendation = "ok"
        self.recommendation_detail = "ok"
        self.demographics = type("_Demographics", (), {"data_confidence": "medium"})()

    def model_dump_json(self):
        return "{}"


def test_cache_key_includes_schema_version(monkeypatch):
    req = AnalysisRequest(school_name="X", address="123 Main", ministry_type="elder_care")

    monkeypatch.setattr(main, "CACHE_SCHEMA_VERSION", "v1")
    key_v1 = main._cache_key(req)

    monkeypatch.setattr(main, "CACHE_SCHEMA_VERSION", "v2")
    key_v2 = main._cache_key(req)

    assert key_v1 != key_v2


@pytest.mark.asyncio
async def test_compare_uses_cached_analysis_when_available(monkeypatch):
    async def fake_geocode(_address):
        return {"lat": 41.0, "lon": -87.0, "state_fips": "17", "county_fips": "031"}

    run_calls = []

    async def fake_run(_location, request):
        run_calls.append(request.ministry_type)
        return _Result(request.ministry_type, 60), {}

    monkeypatch.setattr(main, "geocode_address", fake_geocode)
    monkeypatch.setattr(main, "_run_analysis", fake_run)

    cached_req = AnalysisRequest(school_name="X", address="123 Main", ministry_type="schools")
    cached_key = main._cache_key(cached_req)
    cached_payload = {"stub": True}

    monkeypatch.setattr(main.AnalysisResponse, "model_validate_json", staticmethod(lambda _payload: _Result("schools", 90)))
    fake_redis = _FakeRedis({cached_key: json.dumps(cached_payload)})

    async def fake_get_redis():
        return fake_redis

    monkeypatch.setattr(main, "_get_redis", fake_get_redis)

    req = CompareAnalysisRequest(
        school_name="X",
        address="123 Main",
        ministry_types=["schools", "housing"],
    )
    response = await main.analyze_compare(req)

    assert len(response.results) == 2
    assert run_calls == ["housing"]


@pytest.mark.asyncio
async def test_get_redis_recovers_after_transient_failure(monkeypatch):
    main._redis = None
    main._redis_retry_after = None

    class _Client:
        async def ping(self):
            return True

    calls = {"count": 0}

    def _from_url(*_args, **_kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("temporary")
        return _Client()

    import redis.asyncio as aioredis
    monkeypatch.setattr(aioredis, "from_url", _from_url)

    first = await main._get_redis()
    assert first is None

    main._redis_retry_after = main._now_utc()
    second = await main._get_redis()
    assert second is not None
