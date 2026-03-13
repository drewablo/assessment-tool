import types

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from db import queries
from db.maintenance import backfill_census_centroids
from pipeline import ingest_census


def test_tract_reference_point_expr_falls_back_to_boundary_point():
    expr = queries._tract_reference_point_expr()
    sql = str(select(expr).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert "coalesce" in sql.lower()
    assert "st_pointonsurface" in sql.lower()


@pytest.mark.asyncio
async def test_backfill_census_centroids_executes_update_sql():
    class _Result:
        rowcount = 7

    class _Session:
        def __init__(self):
            self.calls = []

        async def execute(self, stmt, params=None):
            self.calls.append((str(stmt), params or {}))
            return _Result()

    session = _Session()
    changed = await backfill_census_centroids(session, state_fips="42")

    assert changed == 7
    assert "ST_PointOnSurface(boundary)" in session.calls[0][0]
    assert session.calls[0][1] == {"state_fips": "42"}


@pytest.mark.asyncio
async def test_ingest_state_runs_centroid_backfill(monkeypatch):
    row = {
        "NAME": "Census Tract 1",
        "state": "42",
        "county": "091",
        "tract": "020100",
        "B01001_001E": "1000",
    }

    async def fake_fetch(_client, _state_fips, _vintage):
        return [row]

    class _Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def execute(self, _stmt, *_args, **_kwargs):
            return None

        async def commit(self):
            return None

    calls = {"backfill": 0}

    async def fake_backfill(_session, *, state_fips=None):
        calls["backfill"] += 1
        assert state_fips == "42"
        return 3

    monkeypatch.setattr(ingest_census, "_fetch_acs_state", fake_fetch)
    monkeypatch.setattr(ingest_census, "async_session_factory", lambda: _Session())
    monkeypatch.setattr(ingest_census, "backfill_census_centroids", fake_backfill)

    tracts_inserted, geo_enriched = await ingest_census._ingest_state("42", "2022")

    assert tracts_inserted == 1
    # TIGER geometry fetch is not mocked, so geo_enriched is 0
    assert geo_enriched == 0
    assert calls["backfill"] == 1
