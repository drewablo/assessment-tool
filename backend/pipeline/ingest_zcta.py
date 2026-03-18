from __future__ import annotations

import gzip
import io
import json
import logging
import os
import zipfile
from pathlib import Path
from typing import Iterable

import httpx
import shapefile

logger = logging.getLogger(__name__)

ZCTA_SOURCE_URL = os.getenv(
    "ZCTA_SOURCE_URL",
    "https://www2.census.gov/geo/tiger/TIGER2024/ZCTA520/tl_2024_us_zcta520.zip",
)
ZCTA_CACHE_PATH = Path(os.getenv("ZCTA_CACHE_PATH", Path(__file__).resolve().parents[1] / "data" / "zcta_boundaries.json.gz"))
_SIMPLIFY_DECIMALS = int(os.getenv("ZCTA_SIMPLIFY_DECIMALS", "5"))


def _round_coords(coords):
    if isinstance(coords, (list, tuple)):
        if coords and isinstance(coords[0], (float, int)):
            return [round(float(coords[0]), _SIMPLIFY_DECIMALS), round(float(coords[1]), _SIMPLIFY_DECIMALS)]
        return [_round_coords(item) for item in coords]
    return coords


def _shape_to_feature(shape_rec: shapefile.ShapeRecord) -> dict:
    attrs = shape_rec.record.as_dict()
    zip_code = str(attrs.get("ZCTA5CE20") or attrs.get("GEOID20") or "").strip()
    geojson = shape_rec.shape.__geo_interface__
    bbox = list(getattr(shape_rec.shape, "bbox", []) or [])
    return {
        "type": "Feature",
        "properties": {
            "zipCode": zip_code,
            "name": zip_code,
            "source": "census_zcta",
            "bbox": [round(float(value), _SIMPLIFY_DECIMALS) for value in bbox] if bbox else None,
        },
        "geometry": {
            "type": geojson.get("type"),
            "coordinates": _round_coords(geojson.get("coordinates")),
        },
    }


async def ingest_zcta_boundaries(*, source_url: str = ZCTA_SOURCE_URL, zip_filter: Iterable[str] | None = None, destination: Path = ZCTA_CACHE_PATH) -> dict[str, int | str]:
    wanted = {str(code).zfill(5) for code in zip_filter or [] if str(code).strip()}
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        response = await client.get(source_url)
        response.raise_for_status()
        payload = response.content

    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        shp_name = next(name for name in zf.namelist() if name.endswith(".shp"))
        dbf_name = next(name for name in zf.namelist() if name.endswith(".dbf"))
        shx_name = next(name for name in zf.namelist() if name.endswith(".shx"))
        reader = shapefile.Reader(
            shp=io.BytesIO(zf.read(shp_name)),
            dbf=io.BytesIO(zf.read(dbf_name)),
            shx=io.BytesIO(zf.read(shx_name)),
        )
        features = []
        for shape_rec in reader.iterShapeRecords():
            zip_code = str(shape_rec.record.as_dict().get("ZCTA5CE20") or shape_rec.record.as_dict().get("GEOID20") or "").strip()
            if wanted and zip_code not in wanted:
                continue
            features.append(_shape_to_feature(shape_rec))

    destination.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(destination, "wt", encoding="utf-8") as fh:
        json.dump({"type": "FeatureCollection", "features": features}, fh)

    logger.info("Cached %d ZCTA boundaries at %s", len(features), destination)
    return {"features": len(features), "path": str(destination)}
