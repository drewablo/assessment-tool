"""CMS Provider Data ingestion pipeline for elder care facilities."""

import asyncio
import collections
import csv
import hashlib
import io
import logging
import os
import re
import socket
from urllib.parse import urlparse

import httpx
from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.connection import async_session_factory
from db.models import CompetitorElderCare
from pipeline.base import finish_pipeline_run, start_pipeline_run
from pipeline.celery_app import celery_app

logger = logging.getLogger("pipeline.elder_care")

# Official CMS Provider Data API datastore endpoint (Nursing Home Provider Information, 4pq5-n9py)
CMS_DEFAULT_BASE_URL = "https://data.cms.gov/provider-data/api/1/datastore/query/4pq5-n9py/0"
CMS_PROVIDER_ID_KEYS = [
    "federal_provider_number",
    "provider_id",
    "provider_number",
    "cms_certification_number_ccn",
    "ccn",
    "provnum",
]
CMS_NAME_KEYS = ["provider_name", "facility_name", "provname"]
CMS_LOCATION_STRING_KEYS = ["location", "geolocation", "LOCATION"]
ONEFACT_AL_CSV_URL = "https://github.com/onefact/assisted-living/raw/main/assisted-living-facilities.csv"


def _cms_base_url() -> str:
    return os.getenv("CMS_PROVIDER_DATA_API_URL", CMS_DEFAULT_BASE_URL).strip()


def _log_cms_endpoint_diagnostics() -> None:
    base_url = _cms_base_url()
    host = urlparse(base_url).hostname
    logger.info("CMS provider endpoint resolved to: %s", base_url)
    logger.info(
        "CMS_PROVIDER_DATA_API_URL override: %s",
        os.getenv("CMS_PROVIDER_DATA_API_URL", "<unset>"),
    )
    if not host:
        logger.warning("CMS endpoint hostname could not be parsed from %s", base_url)
        return
    try:
        socket.getaddrinfo(host, 443)
        logger.info("CMS hostname DNS resolution succeeded for host=%s", host)
    except socket.gaierror as exc:
        logger.error("CMS hostname DNS resolution failed for host=%s: %s", host, exc)


async def _fetch_cms_facilities(offset: int = 0, limit: int = 1000) -> list[dict]:
    """Fetch a page of CMS nursing home provider data via CMS datastore API."""
    params = {"limit": limit, "offset": offset}
    async with httpx.AsyncClient() as client:
        resp = await client.get(_cms_base_url(), params=params, timeout=60.0)
        resp.raise_for_status()
        payload = resp.json()
        if isinstance(payload, dict):
            results = payload.get("results")
            if isinstance(results, list):
                return results
        logger.warning("Unexpected CMS payload shape at offset %s: %s", offset, type(payload).__name__)
        return []


async def _fetch_all_cms() -> list[dict]:
    all_facilities: list[dict] = []
    offset = 0
    page_size = 1000
    while True:
        page = await _fetch_cms_facilities(offset, page_size)
        if not page:
            break
        all_facilities.extend(page)
        offset += page_size
        if len(page) < page_size:
            break
    logger.info("Fetched %s CMS facilities total", len(all_facilities))
    return all_facilities


async def _fetch_onefact_assisted_living_rows() -> list[dict]:
    """Fetch OneFact assisted-living facilities as CSV rows."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(ONEFACT_AL_CSV_URL, follow_redirects=True, timeout=90.0)
        resp.raise_for_status()

    text = resp.text
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    logger.info("Fetched %s OneFact assisted-living rows", len(rows))
    return rows




def _first_present(raw: dict, keys: list[str]):
    for key in keys:
        if key in raw and raw.get(key) not in (None, ""):
            return raw.get(key)
    return None

def _transform_facility(raw: dict) -> dict | None:
    provider_id = _first_present(raw, CMS_PROVIDER_ID_KEYS)
    if not provider_id:
        return None

    name = str(_first_present(raw, CMS_NAME_KEYS) or "").strip()
    if not name:
        return None

    lat = _first_present(raw, ["latitude", "lat"])
    lon = _first_present(raw, ["longitude", "lon"])
    if lat in (None, "") or lon in (None, ""):
        location = _first_present(raw, CMS_LOCATION_STRING_KEYS)
        if isinstance(location, str):
            # CMS exports can encode this as "(lat, lon)"
            match = re.search(r"\(?\s*([-+]?\d*\.?\d+)\s*,\s*([-+]?\d*\.?\d+)\s*\)?", location)
            if match:
                lat, lon = match.group(1), match.group(2)

    if lat is None or lon is None:
        return None
    try:
        lat = float(lat)
        lon = float(lon)
    except (ValueError, TypeError):
        return None
    if lat == 0 or lon == 0:
        return None

    beds = None
    raw_beds = raw.get("number_of_certified_beds")
    if raw_beds is not None:
        try:
            beds = int(float(raw_beds))
        except (ValueError, TypeError):
            pass

    adc = None
    raw_adc = raw.get("average_number_of_residents_per_day")
    if raw_adc is not None:
        try:
            adc = float(raw_adc)
        except (ValueError, TypeError):
            pass

    occupancy = None
    if beds and adc and beds > 0:
        occupancy = min(100.0, (adc / beds) * 100.0)

    rating = None
    raw_rating = raw.get("overall_rating")
    if raw_rating is not None:
        try:
            rating = int(float(raw_rating))
            if rating < 1 or rating > 5:
                rating = None
        except (ValueError, TypeError):
            pass

    county_fips = None
    if raw.get("county_fips"):
        county_fips = str(raw.get("county_fips")).zfill(5)

    city = str(raw.get("provider_city") or raw.get("city") or "").strip() or None
    state = str(raw.get("provider_state") or raw.get("state") or "").strip() or None
    ownership_type = str(raw.get("ownership_type") or raw.get("ownership") or "").strip() or None

    return {
        "provider_id": str(provider_id),
        "facility_name": name,
        "lat": lat,
        "lon": lon,
        "city": city,
        "state": state,
        "county_fips": county_fips,
        "care_level": "snf",
        "certified_beds": beds,
        "average_daily_census": adc,
        "occupancy_pct": occupancy,
        "ownership_type": ownership_type,
        "overall_rating": rating,
        "data_source": "cms",
    }


def _transform_onefact_facility(raw: dict) -> dict | None:
    name = str(raw.get("Facility Name") or raw.get("facility_name") or raw.get("name") or "").strip()
    if not name:
        return None

    lat_raw = raw.get("Latitude") or raw.get("latitude") or raw.get("lat")
    lon_raw = raw.get("Longitude") or raw.get("longitude") or raw.get("lon")
    if lat_raw in (None, "") or lon_raw in (None, ""):
        return None
    try:
        lat = float(lat_raw)
        lon = float(lon_raw)
    except (TypeError, ValueError):
        return None
    if lat == 0 or lon == 0:
        return None

    beds = None
    raw_beds = raw.get("Capacity") or raw.get("capacity") or raw.get("licensed_beds") or raw.get("beds")
    if raw_beds not in (None, ""):
        try:
            beds = int(float(raw_beds))
        except (TypeError, ValueError):
            beds = None

    city = str(raw.get("City") or raw.get("city") or "").strip() or None
    state = str(raw.get("State") or raw.get("state") or "").strip() or None
    ownership_type = str(raw.get("Ownership Type") or raw.get("ownership_type") or raw.get("ownership") or "").strip() or None

    stable_key = f"{name.lower()}|{lat:.6f}|{lon:.6f}"
    provider_id = f"onefact_{hashlib.sha1(stable_key.encode('utf-8')).hexdigest()[:22]}"

    return {
        "provider_id": provider_id,
        "facility_name": name,
        "lat": lat,
        "lon": lon,
        "city": city,
        "state": state,
        "county_fips": None,
        "care_level": "assisted_living",
        "certified_beds": beds,
        "average_daily_census": None,
        "occupancy_pct": None,
        "ownership_type": ownership_type,
        "overall_rating": None,
        "data_source": "onefact",
    }


@celery_app.task(name="pipeline.ingest_elder_care.ingest_cms_data", bind=True)
def ingest_cms_data(self):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_ingest_cms_async())
    finally:
        loop.close()


def _transform_rejection_reason(raw: dict) -> str | None:
    provider_id = _first_present(raw, CMS_PROVIDER_ID_KEYS)
    if not provider_id:
        return "missing_provider_id"

    name = str(_first_present(raw, CMS_NAME_KEYS) or "").strip()
    if not name:
        return "missing_name"

    lat = _first_present(raw, ["latitude", "lat"])
    lon = _first_present(raw, ["longitude", "lon"])
    if lat in (None, "") or lon in (None, ""):
        location = _first_present(raw, CMS_LOCATION_STRING_KEYS)
        if isinstance(location, str):
            match = re.search(r"\(?\s*([-+]?\d*\.?\d+)\s*,\s*([-+]?\d*\.?\d+)\s*\)?", location)
            if match:
                lat, lon = match.group(1), match.group(2)

    if lat is None or lon is None:
        return "missing_lat_lon"
    try:
        lat = float(lat)
        lon = float(lon)
    except (ValueError, TypeError):
        return "invalid_lat_lon"
    if lat == 0 or lon == 0:
        return "zero_lat_lon"
    return None


def _summarize_transform_diagnostics(raw_facilities: list[dict], transformed: list[dict]) -> None:
    rejection_counts: collections.Counter[str] = collections.Counter()
    for row in raw_facilities:
        reason = _transform_rejection_reason(row)
        if reason:
            rejection_counts[reason] += 1

    key_counts: collections.Counter[str] = collections.Counter()
    for row in raw_facilities:
        for key in row.keys():
            key_counts[key] += 1

    interesting_keys = [
        *CMS_PROVIDER_ID_KEYS,
        *CMS_NAME_KEYS,
        "latitude",
        "longitude",
        "lat",
        "lon",
        *CMS_LOCATION_STRING_KEYS,
    ]
    present = {k: key_counts[k] for k in interesting_keys if key_counts.get(k)}

    logger.info("Elder care transform diagnostics: transformed=%s rejected=%s", len(transformed), len(raw_facilities) - len(transformed))
    logger.info("Elder care rejection reasons: %s", dict(rejection_counts))
    logger.info("Elder care key-presence sample counts: %s", present)
    if raw_facilities:
        sample_keys = sorted(raw_facilities[0].keys())[:20]
        logger.info("Elder care first payload keys (sample): %s", sample_keys)


async def _ingest_cms_async():
    async with async_session_factory() as session:
        run = await start_pipeline_run(session, "cms_elder_care")
        await session.commit()

    try:
        _log_cms_endpoint_diagnostics()
        raw_facilities = await _fetch_all_cms()
        cms_records = [t for row in raw_facilities if (t := _transform_facility(row))]
        _summarize_transform_diagnostics(raw_facilities, cms_records)

        onefact_records: list[dict] = []
        try:
            raw_onefact_rows = await _fetch_onefact_assisted_living_rows()
            onefact_records = [t for row in raw_onefact_rows if (t := _transform_onefact_facility(row))]
        except Exception as e:
            logger.warning("OneFact assisted-living fetch/transform failed; proceeding with CMS-only ingest: %s", e)

        records = cms_records + onefact_records
        logger.info(
            "Transformed elder care records total=%s cms=%s onefact_assisted_living=%s",
            len(records),
            len(cms_records),
            len(onefact_records),
        )

        batch_size = 500
        total_upserted = 0
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            for rec in batch:
                rec["location"] = from_shape(Point(rec["lon"], rec["lat"]), srid=4326)

            async with async_session_factory() as session:
                stmt = pg_insert(CompetitorElderCare).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["provider_id"],
                    set_={col: stmt.excluded[col] for col in batch[0].keys() if col != "provider_id"},
                )
                await session.execute(stmt)
                await session.commit()
                total_upserted += len(batch)

        async with async_session_factory() as session:
            await finish_pipeline_run(
                session,
                run,
                status="success",
                records_processed=len(raw_facilities),
                records_inserted=total_upserted,
            )
            await session.commit()

        return {"processed": len(raw_facilities), "upserted": total_upserted}
    except Exception as e:
        logger.error("CMS ingestion failed: %s", e)
        async with async_session_factory() as session:
            await finish_pipeline_run(session, run, status="failed", error_message=str(e))
            await session.commit()
        raise
