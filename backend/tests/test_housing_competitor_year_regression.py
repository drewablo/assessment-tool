import pytest

from modules import housing


class _FakeSessionCtx:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_get_nearby_housing_db_uses_latest_available_dataset_year(monkeypatch):
    from db import connection, queries

    calls: list[int] = []

    async def fake_latest_year(_session):
        return 2021

    async def fake_context(_session, *, lat, lon, radius_miles, dataset_year, limit):
        calls.append(dataset_year)
        assert lat == 40.1096
        assert lon == -75.2065
        assert radius_miles == 10
        assert limit == 50
        return [{"name": "Nearby HUD", "distance_miles": 1.1, "li_units": 42}]

    monkeypatch.setattr(connection, "get_session", lambda: _FakeSessionCtx())
    monkeypatch.setattr(queries, "get_latest_hud_property_dataset_year", fake_latest_year)
    monkeypatch.setattr(queries, "get_nearby_hud_housing_context", fake_context)

    rows = await housing._get_nearby_housing_db(lat=40.1096, lon=-75.2065, radius_miles=10)

    assert len(rows) == 1
    assert calls == [2021]


@pytest.mark.asyncio
async def test_get_nearby_housing_db_returns_empty_when_no_dataset_year(monkeypatch):
    from db import connection, queries

    async def fake_latest_year(_session):
        return None

    async def should_not_run(*args, **kwargs):
        raise AssertionError("normalized nearby query should not run when no dataset year exists")

    async def fake_legacy(*args, **kwargs):
        return []

    monkeypatch.setattr(connection, "get_session", lambda: _FakeSessionCtx())
    monkeypatch.setattr(queries, "get_latest_hud_property_dataset_year", fake_latest_year)
    monkeypatch.setattr(queries, "get_nearby_hud_housing_context", should_not_run)
    monkeypatch.setattr(queries, "get_nearby_housing", fake_legacy)

    rows = await housing._get_nearby_housing_db(lat=40.1096, lon=-75.2065, radius_miles=10)

    assert rows == []
