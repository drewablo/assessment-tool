from __future__ import annotations

import gzip
import json
import logging
import os
import shutil
import tempfile
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
ZCTA_CACHE_DIR = Path(os.getenv("ZCTA_CACHE_DIR", Path(__file__).resolve().parents[1] / "data" / "zcta"))
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


async def ingest_zcta_boundaries(*, source_url: str = ZCTA_SOURCE_URL, zip_filter: Iterable[str] | None = None, destination: Path = ZCTA_CACHE_DIR) -> dict[str, int | str]:
    wanted = {str(code).zfill(5) for code in zip_filter or [] if str(code).strip()}
    tmpdir = None
    tmp_zip_path = tempfile.mktemp(suffix=".zip")

    try:
        logger.info("Downloading ZCTA shapefile from %s ...", source_url)
        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            async with client.stream("GET", source_url) as response:
                response.raise_for_status()
                with open(tmp_zip_path, "wb") as tmp_zip:
                    async for chunk in response.aiter_bytes(chunk_size=256 * 1024):
                        tmp_zip.write(chunk)
        logger.info("Download complete: %s", tmp_zip_path)

        tmpdir = tempfile.mkdtemp(prefix="zcta_")
        with zipfile.ZipFile(tmp_zip_path) as zf:
            shp_name = next(name for name in zf.namelist() if name.endswith(".shp"))
            dbf_name = next(name for name in zf.namelist() if name.endswith(".dbf"))
            shx_name = next(name for name in zf.namelist() if name.endswith(".shx"))
            zf.extract(shp_name, tmpdir)
            zf.extract(dbf_name, tmpdir)
            zf.extract(shx_name, tmpdir)

        os.unlink(tmp_zip_path)
        tmp_zip_path = None

        reader = shapefile.Reader(
            shp=os.path.join(tmpdir, shp_name),
            dbf=os.path.join(tmpdir, dbf_name),
            shx=os.path.join(tmpdir, shx_name),
        )

        # Write individual per-ZIP files (~3-15KB each) + a bbox index (~200KB)
        # Use os.makedirs to avoid pathlib mkdir issues on Docker bind mounts
        os.makedirs(destination, exist_ok=True)
        count = 0
        bbox_index: dict[str, list[float]] = {}

        for shape_rec in reader.iterShapeRecords():
            zip_code = str(
                shape_rec.record.as_dict().get("ZCTA5CE20")
                or shape_rec.record.as_dict().get("GEOID20")
                or ""
            ).strip()
            if not zip_code:
                continue
            if wanted and zip_code not in wanted:
                continue

            feature = _shape_to_feature(shape_rec)

            zip_file_path = destination / f"{zip_code}.json.gz"
            with gzip.open(zip_file_path, "wt", encoding="utf-8") as fh:
                json.dump(feature, fh, separators=(",", ":"))

            bbox = feature.get("properties", {}).get("bbox")
            if bbox:
                bbox_index[zip_code] = bbox

            count += 1
            if count % 5000 == 0:
                logger.info("Processed %d ZCTA features...", count)

        index_path = destination / "bbox_index.json.gz"
        with gzip.open(index_path, "wt", encoding="utf-8") as fh:
            json.dump(bbox_index, fh, separators=(",", ":"))

        (destination / "_ready").write_text(f"{count}\n")

        logger.info("Cached %d individual ZCTA files + bbox index at %s", count, destination)
        return {"features": count, "path": str(destination), "index_path": str(index_path)}

    finally:
        if tmp_zip_path:
            try:
                os.unlink(tmp_zip_path)
            except OSError:
                pass
        if tmpdir:
            shutil.rmtree(tmpdir, ignore_errors=True)
