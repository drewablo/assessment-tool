import asyncio
import pandas as pd

from pipeline import ingest_census, ingest_elder_care, ingest_schools


def test_schools_transform_prefers_22_coordinates_and_falls_back(caplog):
    df = pd.DataFrame(
        [
            {
                "PPIN": "A1",
                "PINST": "Preferred Coords School",
                "LATITUDE22": 40.0,
                "LONGITUDE22": -75.0,
                "LATITUDE20": 11.0,
                "LONGITUDE20": -11.0,
            },
            {
                "PPIN": "B2",
                "PINST": "Fallback Coords School",
                "LATITUDE20": 41.0,
                "LONGITUDE20": -76.0,
            },
            {
                "PPIN": "C3",
                "PINST": "No Coords School",
            },
        ]
    )

    caplog.set_level("INFO", logger="pipeline.schools")
    rows = ingest_schools._transform_schools(df)

    assert len(rows) == 2
    assert rows[0]["lat"] == 40.0 and rows[0]["lon"] == -75.0
    assert rows[1]["lat"] == 41.0 and rows[1]["lon"] == -76.0
    assert "coordinate columns discovered" in caplog.text
    assert "rejection reason counts" in caplog.text


def test_elder_fetch_uses_valid_socrata_parameters(monkeypatch):
    captured = {}

    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return {"results": [{"provider_name": "x"}]}

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url, params=None, timeout=None):
            captured["url"] = url
            captured["params"] = params
            return _Resp()

    monkeypatch.setattr(ingest_elder_care.httpx, "AsyncClient", _Client)

    rows = asyncio.run(ingest_elder_care._fetch_cms_facilities(offset=1000, limit=250))
    assert rows == [{"provider_name": "x"}]
    assert captured["url"].endswith("/4pq5-n9py/0")
    assert captured["params"] == {"limit": 250, "offset": 1000}


def test_shapefile_record_to_geometry_builds_multipolygon():
    """_shapefile_record_to_geometry converts a pyshp shape to MultiPolygon."""
    from shapely.geometry import Polygon, MultiPolygon
    import shapefile as pyshp

    # Create a mock pyshp shape with a Polygon geo_interface
    class _MockShape:
        shapeType = pyshp.POLYGON
        @property
        def __geo_interface__(self):
            return {
                "type": "Polygon",
                "coordinates": [[(-122.0, 37.0), (-121.0, 37.0), (-121.0, 38.0), (-122.0, 38.0), (-122.0, 37.0)]],
            }

    geom = ingest_census._shapefile_record_to_geometry(_MockShape())
    assert geom is not None
    assert geom.geom_type == "MultiPolygon"
