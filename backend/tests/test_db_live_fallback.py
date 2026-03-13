import pytest

from modules import schools as schools_module


class _Req:
    school_name = "St Test"
    gender = "coed"
    grade_level = "k_8"
    weighting_profile = None
    stage2_inputs = None
    market_context = "suburban"


@pytest.mark.asyncio
async def test_schools_falls_back_to_live_when_db_empty(monkeypatch):
    monkeypatch.setattr(schools_module, "USE_DB", True)

    async def fake_db_fetch(**_kwargs):
        return []

    async def fake_live_fetch(**_kwargs):
        return [{"name": "Live School"}]

    seen = {}

    async def fake_calculate(**kwargs):
        seen["schools"] = kwargs["schools"]
        return {"ok": True}

    monkeypatch.setattr(schools_module, "_get_nearby_schools_db", fake_db_fetch)
    monkeypatch.setattr(schools_module, "get_nearby_schools", fake_live_fetch)
    monkeypatch.setattr(schools_module, "calculate_feasibility", fake_calculate)

    result = await schools_module.analyze_schools(
        location={"lat": 41.0, "lon": -87.0},
        demographics={"tract_count": 0},
        request=_Req(),
        radius_miles=10,
        drive_minutes=15,
        isochrone_polygon=None,
        catchment_type="radius",
    )

    assert result == {"ok": True}
    assert seen["schools"] == [{"name": "Live School"}]


@pytest.mark.asyncio
async def test_schools_uses_db_when_data_present(monkeypatch):
    monkeypatch.setattr(schools_module, "USE_DB", True)

    async def fake_db_fetch(**_kwargs):
        return [{"name": "DB School"}]

    async def fake_live_fetch(**_kwargs):
        raise AssertionError("live fetch should not be called when DB has rows")

    seen = {}

    async def fake_calculate(**kwargs):
        seen["schools"] = kwargs["schools"]
        return {"ok": True}

    monkeypatch.setattr(schools_module, "_get_nearby_schools_db", fake_db_fetch)
    monkeypatch.setattr(schools_module, "get_nearby_schools", fake_live_fetch)
    monkeypatch.setattr(schools_module, "calculate_feasibility", fake_calculate)

    await schools_module.analyze_schools(
        location={"lat": 41.0, "lon": -87.0},
        demographics={"tract_count": 2},
        request=_Req(),
        radius_miles=10,
        drive_minutes=15,
        isochrone_polygon=None,
        catchment_type="radius",
    )

    assert seen["schools"] == [{"name": "DB School"}]
