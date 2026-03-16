"""HUD Section 202 Senior Housing ingestion pipeline.

Fetches HUD Section 202 property data from the ArcGIS GeoJSON endpoint
and upserts into the hud_section_202_properties table.

Follows the same ingestion pattern as ingest_elder_care.py and ingest_housing.py:
  - start_pipeline_run / finish_pipeline_run for tracking
  - httpx-based fetch with pagination (ArcGIS limits to ~2000 features per request)
  - transform + rejection diagnostics
  - chunked upsert via pg_insert ON CONFLICT
"""

import asyncio
import logging
import math
import os
from collections import Counter

import httpx
from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.connection import async_session_factory
from db.models import HudSection202Property
from pipeline.base import finish_pipeline_run, start_pipeline_run
from pipeline.celery_app import celery_app

logger = logging.getLogger("pipeline.hud_section202")

HUD_SECTION_202_URL = os.getenv(
    "HUD_SECTION_202_GEOJSON_URL",
    "https://services.arcgis.com/VTyQ9soqVukalItT/arcgis/rest/services/"
    "HUD_Section_202_Properties/FeatureServer/0/query"
    "?outFields=*&where=1%3D1&f=json&outSR=4326",
)

HUD_SECTION_202_BATCH_SIZE = int(os.getenv("HUD_SECTION_202_BATCH_SIZE", "500"))

# ArcGIS pagination — the endpoint may cap results; we paginate with resultOffset
ARCGIS_PAGE_SIZE = int(os.getenv("HUD_SECTION_202_PAGE_SIZE", "2000"))


def _to_int(value) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def _to_str(value, max_len: int | None = None) -> str | None:
    if value in (None, ""):
        return None
    s = str(value).strip()
    if not s or s.lower() in ("none", "null", "nan"):
        return None
    if max_len:
        s = s[:max_len]
    return s


def _web_mercator_to_wgs84(x: float, y: float) -> tuple[float, float]:
    """Convert Web Mercator (EPSG:3857) coordinates to WGS84 (EPSG:4326)."""
    lon = x * 180.0 / 20037508.34
    lat = math.degrees(2 * math.atan(math.exp(y * math.pi / 20037508.34)) - math.pi / 2)
    return lon, lat


def _is_web_mercator(x: float, y: float) -> bool:
    """Detect if coordinates are likely Web Mercator rather than WGS84."""
    return abs(x) > 180 or abs(y) > 90


def _esri_feature_to_geojson(feature: dict) -> dict:
    """Convert an Esri JSON feature to GeoJSON-style dict.

    Esri JSON stores geometry as ``{"x": lon, "y": lat}`` while the rest
    of the pipeline expects ``{"geometry": {"coordinates": [lon, lat]},
    "properties": {...}}``.

    If coordinates appear to be in Web Mercator (EPSG:3857), they are
    automatically reprojected to WGS84 (EPSG:4326).
    """
    geom = feature.get("geometry") or {}
    props = feature.get("attributes") or {}
    x, y = geom.get("x"), geom.get("y")
    if x is not None and y is not None:
        try:
            x, y = float(x), float(y)
            if _is_web_mercator(x, y):
                x, y = _web_mercator_to_wgs84(x, y)
        except (ValueError, TypeError):
            pass
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [x, y],
        },
        "properties": props,
    }


async def _fetch_page(url: str, offset: int = 0) -> dict:
    """Fetch a page of features from the ArcGIS endpoint (Esri JSON)."""
    separator = "&" if "?" in url else "?"
    paginated_url = f"{url}{separator}resultOffset={offset}&resultRecordCount={ARCGIS_PAGE_SIZE}"
    async with httpx.AsyncClient() as client:
        resp = await client.get(paginated_url, timeout=180.0, follow_redirects=True)
        resp.raise_for_status()
        return resp.json()


async def _fetch_all_features() -> list[dict]:
    """Fetch all features from the ArcGIS endpoint, handling pagination.

    Uses ``f=json`` (Esri JSON) instead of ``f=geojson`` so that the
    ``exceededTransferLimit`` flag is included in the response — this is
    the only reliable way to know if more pages exist, since the server's
    ``maxRecordCount`` (often 1000) may be lower than our requested page
    size.
    """
    all_features: list[dict] = []
    offset = 0
    while True:
        payload = await _fetch_page(HUD_SECTION_202_URL, offset)
        raw_features = payload.get("features", [])
        if not raw_features:
            break
        features = [_esri_feature_to_geojson(f) for f in raw_features]
        all_features.extend(features)
        logger.info(
            "HUD Section 202 fetch: offset=%d fetched=%d total_so_far=%d",
            offset, len(features), len(all_features),
        )
        if not payload.get("exceededTransferLimit", False):
            break
        offset += len(features)
    logger.info("HUD Section 202 total features fetched: %d", len(all_features))
    return all_features


def _rejection_reason(feature: dict) -> str | None:
    """Return a rejection reason if the feature cannot be transformed, else None."""
    props = feature.get("properties") or {}
    geometry = feature.get("geometry") or {}

    # Need some kind of identifier
    property_id = _to_str(
        props.get("OBJECTID") or props.get("FHA_LOAN_ID_NUMBER")
        or props.get("PROPERTY_ID") or props.get("FID")
    )
    if not property_id:
        return "missing_property_id"

    name = _to_str(props.get("PROPERTY_NAME_TEXT")) or _to_str(props.get("SERVICING_SITE_NAME_TEXT"))
    if not name:
        return "missing_property_name"

    coords = geometry.get("coordinates")
    if not coords or len(coords) < 2:
        return "missing_coordinates"
    try:
        lon, lat = float(coords[0]), float(coords[1])
        if lon == 0 and lat == 0:
            return "zero_coordinates"
        if abs(lon) > 180 or abs(lat) > 90:
            return "coordinates_out_of_wgs84_range"
    except (ValueError, TypeError, IndexError):
        return "invalid_coordinates"

    return None


def _transform_feature(feature: dict) -> dict | None:
    """Transform a GeoJSON feature into a normalized row dict."""
    props = feature.get("properties") or {}
    geometry = feature.get("geometry") or {}

    property_id = _to_str(
        props.get("OBJECTID") or props.get("FHA_LOAN_ID_NUMBER")
        or props.get("PROPERTY_ID") or props.get("FID")
    )
    if not property_id:
        return None

    # Use PROPERTY_NAME_TEXT as the canonical display name.
    # Do NOT use HUB_NAME — it contains regional office labels, not property names.
    name = _to_str(props.get("PROPERTY_NAME_TEXT"), 300)
    if not name:
        # Fall back to SERVICING_SITE_NAME_TEXT if PROPERTY_NAME_TEXT is absent
        name = _to_str(props.get("SERVICING_SITE_NAME_TEXT"), 300)
    if not name:
        return None

    coords = geometry.get("coordinates")
    if not coords or len(coords) < 2:
        return None
    try:
        lon, lat = float(coords[0]), float(coords[1])
    except (ValueError, TypeError, IndexError):
        return None
    if lon == 0 and lat == 0:
        return None
    if abs(lon) > 180 or abs(lat) > 90:
        return None

    return {
        "property_id": str(property_id),
        "servicing_site_name": name,
        "property_name": _to_str(props.get("PROPERTY_NAME_TEXT"), 300),
        "street_address": _to_str(props.get("STD_ADDR"), 300),
        "city": _to_str(props.get("STD_CITY"), 100),
        "state": _to_str(props.get("STD_ST"), 2),
        "zip_code": _to_str(props.get("STD_ZIP5"), 10),
        "lat": lat,
        "lon": lon,
        "total_units": _to_int(props.get("TOTAL_UNIT_COUNT")),
        "total_assisted_units": _to_int(props.get("TOTAL_ASSISTED_UNIT_COUNT")),
        "client_group_name": _to_str(props.get("CLIENT_GROUP_NAME"), 120),
        "client_group_type": _to_str(props.get("CLIENT_GROUP_TYPE"), 80),
        "property_category": _to_str(props.get("PROPERTY_CATEGORY_NAME"), 120),
        "primary_financing_type": _to_str(props.get("PRIMARY_FINANCING_TYPE"), 120),
        "phone_number": _to_str(props.get("PROPERTY_ON_SITE_PHONE_NUMBER"), 30),
        "reac_inspection_score": _to_int(props.get("REAC_LAST_INSPECTION_SCORE")),
        "raw_payload": props,
    }


def _upsert_statement(chunk: list[dict]):
    """Build a PostgreSQL upsert statement for HudSection202Property."""
    stmt = pg_insert(HudSection202Property).values(chunk)
    return stmt.on_conflict_do_update(
        constraint="uq_hud_section_202_property_id",
        set_={col: stmt.excluded[col] for col in chunk[0].keys() if col != "property_id"},
    )


async def _ingest_hud_section202_async() -> dict:
    """Ingest HUD Section 202 properties from ArcGIS GeoJSON endpoint."""
    async with async_session_factory() as session:
        run = await start_pipeline_run(session, "hud_section_202")
        await session.commit()

    try:
        raw_features = await _fetch_all_features()
        logger.info("HUD Section 202 source_features_loaded=%d", len(raw_features))

        reject = Counter()
        rows: list[dict] = []
        for feature in raw_features:
            t = _transform_feature(feature)
            if t:
                rows.append(t)
            else:
                reject[_rejection_reason(feature) or "filtered_out_by_rule"] += 1

        # Add PostGIS geometry
        for rec in rows:
            rec["location"] = from_shape(Point(rec["lon"], rec["lat"]), srid=4326)

        logger.info(
            "HUD Section 202 normalized_rows_prepared=%d rejected=%d top_rejection_reasons=%s",
            len(rows),
            len(raw_features) - len(rows),
            ", ".join(f"{k}={v}" for k, v in reject.most_common(5)) if reject else "none",
        )

        total_upserted = 0
        if rows:
            batch_size = max(HUD_SECTION_202_BATCH_SIZE, 1)
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                async with async_session_factory() as session:
                    stmt = _upsert_statement(batch)
                    await session.execute(stmt)
                    await session.commit()
                    total_upserted += len(batch)
        else:
            logger.warning("HUD Section 202: zero rows after transform")

        async with async_session_factory() as session:
            await finish_pipeline_run(
                session,
                run,
                status="success",
                records_processed=len(raw_features),
                records_inserted=total_upserted,
            )
            await session.commit()

        return {"processed": len(raw_features), "upserted": total_upserted}
    except Exception as exc:
        logger.error("HUD Section 202 ingestion failed: %s", exc)
        async with async_session_factory() as session:
            await finish_pipeline_run(session, run, status="failed", error_message=str(exc))
            await session.commit()
        raise


@celery_app.task(name="pipeline.ingest_hud_section202.ingest_hud_section202", bind=True)
def ingest_hud_section202(self):
    return asyncio.new_event_loop().run_until_complete(_ingest_hud_section202_async())
