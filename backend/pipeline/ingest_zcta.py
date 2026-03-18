from __future__ import annotations

import gzip
import io
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
    tmpdir = None

    # --- Step 1: Stream download to a temp file (not memory) ---
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

        # --- Step 2: Extract shapefile components to disk (not BytesIO) ---
        tmpdir = tempfile.mkdtemp(prefix="zcta_")
        with zipfile.ZipFile(tmp_zip_path) as zf:
            shp_name = next(name for name in zf.namelist() if name.endswith(".shp"))
            dbf_name = next(name for name in zf.namelist() if name.endswith(".dbf"))
            shx_name = next(name for name in zf.namelist() if name.endswith(".shx"))
            zf.extract(shp_name, tmpdir)
            zf.extract(dbf_name, tmpdir)
            zf.extract(shx_name, tmpdir)

        # Free the zip file from disk now that we've extracted what we need
        os.unlink(tmp_zip_path)
        tmp_zip_path = None

        # --- Step 3: Stream features one at a time to gzipped JSON ---
        # This avoids building a list of all ~33k features in memory.
        reader = shapefile.Reader(
            shp=os.path.join(tmpdir, shp_name),
            dbf=os.path.join(tmpdir, dbf_name),
            shx=os.path.join(tmpdir, shx_name),
        )

        destination.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        bbox_index: dict[str, list[float]] = {}

        with gzip.open(destination, "wt", encoding="utf-8") as fh:
            fh.write('{"type":"FeatureCollection","features":[')
            first = True
            for shape_rec in reader.iterShapeRecords():
                zip_code = str(
                    shape_rec.record.as_dict().get("ZCTA5CE20")
                    or shape_rec.record.as_dict().get("GEOID20")
                    or ""
                ).strip()
                if wanted and zip_code not in wanted:
                    continue
                feature = _shape_to_feature(shape_rec)
                if not first:
                    fh.write(",")
                json.dump(feature, fh, separators=(",", ":"))

                # Collect bbox for the index
                bbox = feature.get("properties", {}).get("bbox")
                if bbox:
                    bbox_index[zip_code] = bbox

                first = False
                count += 1
                if count % 5000 == 0:
                    logger.info("Processed %d ZCTA features...", count)
            fh.write("]}")

        # Write a separate lightweight bbox index (~200KB vs ~100MB for the full file)
        index_path = destination.with_name("zcta_bbox_index.json.gz")
        with gzip.open(index_path, "wt", encoding="utf-8") as fh:
            json.dump(bbox_index, fh, separators=(",", ":"))
        logger.info("Wrote bbox index with %d entries at %s", len(bbox_index), index_path)

        logger.info("Cached %d ZCTA boundaries at %s", count, destination)
        return {"features": count, "path": str(destination), "index_path": str(index_path)}

    finally:
        # Clean up temp files
        if tmp_zip_path:
            try:
                os.unlink(tmp_zip_path)
            except OSError:
                pass
        if tmpdir:
            shutil.rmtree(tmpdir, ignore_errors=True)
