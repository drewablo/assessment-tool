from pathlib import Path

import pandas as pd
import pytest

from competitors import hud_lihtc
from api import geocoder


def test_lihtc_loader_memoizes_by_file_signature(tmp_path, monkeypatch):
    csv_path = tmp_path / "lihtc.csv"
    csv_path.write_text("lat,lon,project_name\n41.0,-87.0,A\n")

    monkeypatch.setattr(hud_lihtc, "DATA_FILE", csv_path)
    hud_lihtc._LIHTC_CACHE_DF = None
    hud_lihtc._LIHTC_CACHE_SIG = None

    reads = {"count": 0}
    real_read_csv = pd.read_csv

    def wrapped(*args, **kwargs):
        reads["count"] += 1
        return real_read_csv(*args, **kwargs)

    monkeypatch.setattr(pd, "read_csv", wrapped)

    hud_lihtc.get_nearby_lihtc_projects(41.0, -87.0, 5)
    hud_lihtc.get_nearby_lihtc_projects(41.0, -87.0, 5)
    assert reads["count"] == 1


def test_lihtc_loader_accepts_schema_variants(tmp_path, monkeypatch):
    csv_path = tmp_path / "lihtc.csv"
    csv_path.write_text("Y,X,PROJ_ADD,PROJ_CTY,LI_UNITS\n40.1096,-75.2065,Flourtown Homes,Flourtown,37\n")

    monkeypatch.setattr(hud_lihtc, "DATA_FILE", csv_path)
    hud_lihtc._LIHTC_CACHE_DF = None
    hud_lihtc._LIHTC_CACHE_SIG = None

    rows = hud_lihtc.get_nearby_lihtc_projects(40.1096, -75.2065, 10)

    assert len(rows) == 1
    assert rows[0]["name"] == "Flourtown Homes"
    assert rows[0]["city"] == "Flourtown"
    assert rows[0]["li_units"] == 37


@pytest.mark.asyncio
async def test_geocoder_normalized_cache_key(monkeypatch):
    geocoder._GEOCODE_CACHE.clear()

    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "result": {
                    "addressMatches": [
                        {
                            "coordinates": {"x": -87.0, "y": 41.0},
                            "matchedAddress": "123 Main St",
                            "geographies": {"Counties": [{"GEOID": "17031", "NAME": "Cook"}], "States": [{"GEOID": "17", "NAME": "IL"}]},
                        }
                    ]
                }
            }

    calls = {"count": 0}

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, *_args, **_kwargs):
            calls["count"] += 1
            return _Response()

    monkeypatch.setattr(geocoder.httpx, "AsyncClient", lambda *args, **kwargs: _Client())

    one = await geocoder.geocode_address("123 Main St")
    two = await geocoder.geocode_address("  123   MAIN st  ")

    assert one and two
    assert calls["count"] == 1
