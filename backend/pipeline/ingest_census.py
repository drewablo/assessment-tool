"""Census ACS 5-Year bulk data ingestion pipeline.

Downloads ACS Detailed Tables from the Census Bureau API in bulk (state by state),
transforms them into CensusTract rows, and upserts into PostgreSQL.

Also downloads TIGER/Line tract shapefiles for boundaries and centroids.
"""

import asyncio
import io
import logging
import os
import tempfile
import zipfile
import httpx
import shapefile as pyshp
from geoalchemy2.shape import from_shape
from shapely.geometry import MultiPolygon, Point, Polygon, shape
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.connection import async_session_factory
from db.models import CensusTract, CensusTractHistory
from db.maintenance import backfill_census_centroids
from pipeline.base import start_pipeline_run, finish_pipeline_run
from pipeline.celery_app import celery_app

logger = logging.getLogger("pipeline.census")

CENSUS_API_BASE = "https://api.census.gov/data"
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "")

# ACS 5-Year variables we need
ACS_VARIABLES = {
    # Population by age (B01001)
    "B01001_001E": "total_population",
    "B01001_002E": "male_total",
    "B01001_026E": "female_total",
    # Age breakdown computed from multiple rows (see _compute_age_groups)
    # Under 18 (B09001)
    "B09001_001E": "population_under_18",
    # School enrollment (B14002)
    "B14002_001E": "enrolled_total",
    # Income distribution (B19001)
    "B19001_002E": "income_bracket_under_10k",
    "B19001_003E": "income_bracket_10k_15k",
    "B19001_004E": "income_bracket_15k_25k",
    "B19001_005E": "income_bracket_25k_35k",
    "B19001_006E": "income_bracket_35k_50k",
    "B19001_007E": "income_bracket_50k_75k",
    "B19001_008E": "income_bracket_75k_100k",
    "B19001_009E": "income_bracket_100k_150k",
    "B19001_010E": "income_bracket_150k_200k",
    "B19001_011E": "income_bracket_200k_plus",
    # Median incomes
    "B19013_001E": "median_household_income",
    "B19125_001E": "median_family_income",
    # Households (B11001)
    "B11001_001E": "total_households",
    "B11001_002E": "family_households",
    # Families with children (B11003)
    "B11003_003E": "married_with_children",
    "B11003_010E": "male_hh_with_children",
    "B11003_016E": "female_hh_with_children",
    # Housing tenure (B25003)
    "B25003_002E": "owner_occupied",
    "B25003_003E": "renter_occupied",
    # Poverty (B17001)
    "B17001_002E": "population_below_poverty",
    "B17001_015E": "male_65_74_below_poverty",
    "B17001_016E": "male_75_plus_below_poverty",
    "B17001_029E": "female_65_74_below_poverty",
    "B17001_030E": "female_75_plus_below_poverty",
    # Seniors living alone (B11010)
    "B11010_003E": "male_65_plus_living_alone",
    "B11010_006E": "female_65_plus_living_alone",
    # Age detail for under-5, 5-17, 65-74, 75+ (B01001 detail)
    "B01001_003E": "male_under_5",
    "B01001_004E": "male_5_9",
    "B01001_005E": "male_10_14",
    "B01001_006E": "male_15_17",
    "B01001_020E": "male_65_66",
    "B01001_021E": "male_67_69",
    "B01001_022E": "male_70_74",
    "B01001_023E": "male_75_79",
    "B01001_024E": "male_80_84",
    "B01001_025E": "male_85_plus",
    "B01001_027E": "female_under_5",
    "B01001_028E": "female_5_9",
    "B01001_029E": "female_10_14",
    "B01001_030E": "female_15_17",
    "B01001_044E": "female_65_66",
    "B01001_045E": "female_67_69",
    "B01001_046E": "female_70_74",
    "B01001_047E": "female_75_79",
    "B01001_048E": "female_80_84",
    "B01001_049E": "female_85_plus",
    # MOE for median income (for confidence)
    "B19013_001M": "median_household_income_moe",
}

# All 50 states + DC FIPS codes


# TIGER/Line shapefile download — static zip files, one per state.
# Far more reliable than the ArcGIS REST API which frequently returns
# HTML error pages or empty bodies under load.
TIGER_SHAPEFILE_BASE = "https://www2.census.gov/geo/tiger/TIGER{year}/TRACT"
TIGER_SHAPEFILE_VINTAGE = os.getenv("TIGER_VINTAGE", "2022")

# Retry settings for Census API and TIGER downloads
_MAX_RETRIES = 4
_RETRY_BACKOFF_BASE = 4  # seconds; retry delays: 4, 8, 16, 32


async def _retry_get_json(client: httpx.AsyncClient, url: str, *, params: dict, timeout: float, label: str) -> dict | list:
    """GET with exponential-backoff retry; returns parsed JSON."""
    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            resp = await client.get(url, params=params, timeout=timeout)
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = _RETRY_BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    "%s: HTTP %d on attempt %d/%d; retrying in %ds",
                    label, resp.status_code, attempt + 1, _MAX_RETRIES + 1, wait,
                )
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            try:
                return resp.json()
            except ValueError as exc:
                last_exc = exc
                if attempt < _MAX_RETRIES:
                    wait = _RETRY_BACKOFF_BASE * (2 ** attempt)
                    logger.warning(
                        "%s: JSON decode failed on attempt %d/%d; retrying in %ds",
                        label, attempt + 1, _MAX_RETRIES + 1, wait,
                    )
                    await asyncio.sleep(wait)
                else:
                    raise
        except (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError) as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES:
                wait = _RETRY_BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    "%s: %s on attempt %d/%d; retrying in %ds",
                    label, type(exc).__name__, attempt + 1, _MAX_RETRIES + 1, wait,
                )
                await asyncio.sleep(wait)
            else:
                raise
    raise last_exc or RuntimeError(f"{label}: exhausted retries")


async def _retry_get_bytes(client: httpx.AsyncClient, url: str, *, timeout: float, label: str) -> bytes:
    """GET with retry; returns raw response bytes (for zip downloads)."""
    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            resp = await client.get(url, timeout=timeout, follow_redirects=True)
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = _RETRY_BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    "%s: HTTP %d on attempt %d/%d; retrying in %ds",
                    label, resp.status_code, attempt + 1, _MAX_RETRIES + 1, wait,
                )
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.content
        except (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError) as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES:
                wait = _RETRY_BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    "%s: %s on attempt %d/%d; retrying in %ds",
                    label, type(exc).__name__, attempt + 1, _MAX_RETRIES + 1, wait,
                )
                await asyncio.sleep(wait)
            else:
                raise
    raise last_exc or RuntimeError(f"{label}: exhausted retries")


def _shapefile_record_to_geometry(shp_shape) -> MultiPolygon | None:
    """Convert a pyshp shape record to a Shapely MultiPolygon."""
    if shp_shape.shapeType not in (pyshp.POLYGON, pyshp.POLYGONZ, pyshp.POLYGONM):
        return None
    try:
        geom = shape(shp_shape.__geo_interface__)
    except Exception:
        return None
    if geom.geom_type == "Polygon":
        geom = MultiPolygon([geom])
    if not isinstance(geom, MultiPolygon):
        return None
    return geom


async def _fetch_tiger_geometries_for_state(client: httpx.AsyncClient, state_fips: str) -> dict[str, MultiPolygon]:
    """Download TIGER/Line tract shapefile for a state and return {geoid: geometry}.

    Downloads a ~2-10 MB zip file from the Census FTP site, extracts the
    shapefile in memory, and reads tract boundaries using pyshp.
    """
    vintage = TIGER_SHAPEFILE_VINTAGE
    filename = f"tl_{vintage}_{state_fips}_tract.zip"
    url = f"{TIGER_SHAPEFILE_BASE.format(year=vintage)}/{filename}"

    try:
        zip_bytes = await _retry_get_bytes(
            client, url, timeout=120.0,
            label=f"TIGER shapefile state={state_fips}",
        )
    except Exception as exc:
        logger.error(
            "Failed to download TIGER shapefile for state %s (%s): %s",
            state_fips, url, exc,
        )
        return {}

    geom_by_geoid: dict[str, MultiPolygon] = {}
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            # Find the .shp, .shx, and .dbf files inside the zip
            shp_name = next((n for n in zf.namelist() if n.endswith(".shp")), None)
            shx_name = next((n for n in zf.namelist() if n.endswith(".shx")), None)
            dbf_name = next((n for n in zf.namelist() if n.endswith(".dbf")), None)

            if not shp_name or not dbf_name:
                logger.error("TIGER zip for state %s missing .shp or .dbf", state_fips)
                return {}

            shp_data = io.BytesIO(zf.read(shp_name))
            shx_data = io.BytesIO(zf.read(shx_name)) if shx_name else None
            dbf_data = io.BytesIO(zf.read(dbf_name))

            reader = pyshp.Reader(shp=shp_data, shx=shx_data, dbf=dbf_data)
            # Find the GEOID field index
            field_names = [f[0] for f in reader.fields[1:]]  # skip DeletionFlag
            geoid_idx = None
            for candidate in ("GEOID", "GEOID20", "GEOID10"):
                if candidate in field_names:
                    geoid_idx = field_names.index(candidate)
                    break

            if geoid_idx is None:
                logger.error(
                    "TIGER shapefile for state %s has no GEOID field. Fields: %s",
                    state_fips, field_names,
                )
                return {}

            for sr in reader.iterShapeRecords():
                geoid = str(sr.record[geoid_idx])
                geom = _shapefile_record_to_geometry(sr.shape)
                if geom is not None:
                    geom_by_geoid[geoid] = geom

    except (zipfile.BadZipFile, Exception) as exc:
        logger.error("Failed to parse TIGER shapefile for state %s: %s", state_fips, exc)
        return {}

    logger.info(
        "TIGER shapefile state=%s: %d tract geometries extracted (%.1f MB zip)",
        state_fips, len(geom_by_geoid), len(zip_bytes) / 1_048_576,
    )
    return geom_by_geoid

STATE_FIPS = [
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
    "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
    "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
    "56",
]


def _safe_int(val) -> int | None:
    if val is None or val == "" or val == "null":
        return None
    try:
        v = int(float(val))
        return v if v >= 0 else None
    except (ValueError, TypeError):
        return None


def _compute_age_groups(row: dict) -> dict:
    """Compute derived age group columns from detailed B01001 cells."""
    def _sum(*keys):
        vals = [_safe_int(row.get(k)) for k in keys]
        non_none = [v for v in vals if v is not None]
        return sum(non_none) if non_none else None

    return {
        "population_under_5": _sum("male_under_5", "female_under_5"),
        "population_5_17": _sum(
            "male_5_9", "male_10_14", "male_15_17",
            "female_5_9", "female_10_14", "female_15_17",
        ),
        "population_65_74": _sum(
            "male_65_66", "male_67_69", "male_70_74",
            "female_65_66", "female_67_69", "female_70_74",
        ),
        "population_75_plus": _sum(
            "male_75_79", "male_80_84", "male_85_plus",
            "female_75_79", "female_80_84", "female_85_plus",
        ),
    }


def _compute_income_cv(row: dict) -> float | None:
    """Compute coefficient of variation for median income estimate."""
    estimate = _safe_int(row.get("median_household_income"))
    moe = _safe_int(row.get("median_household_income_moe"))
    if estimate and moe and estimate > 0:
        se = moe / 1.645  # 90% MOE to standard error
        return (se / estimate) * 100.0
    return None


async def _fetch_acs_state(
    client: httpx.AsyncClient,
    state_fips: str,
    vintage: str = "2022",
) -> list[dict]:
    """Fetch ACS 5-Year data for all tracts in a state.

    Raises on failure so the caller can distinguish "no data" from "API error".
    """
    var_list = ",".join(ACS_VARIABLES.keys())
    params = {
        "get": f"NAME,{var_list}",
        "for": "tract:*",
        "in": f"state:{state_fips}",
    }
    if CENSUS_API_KEY:
        params["key"] = CENSUS_API_KEY

    url = f"{CENSUS_API_BASE}/{vintage}/acs/acs5"

    data = await _retry_get_json(
        client, url,
        params=params, timeout=90.0,
        label=f"ACS state={state_fips}",
    )

    if not data or len(data) < 2:
        logger.warning("ACS state=%s returned empty/header-only response", state_fips)
        return []

    headers = data[0]
    rows = []
    for record in data[1:]:
        row = dict(zip(headers, record))
        rows.append(row)
    return rows


def _transform_tract(row: dict, vintage: str = "2022") -> dict:
    """Transform a raw ACS API row into CensusTract column values."""
    state = row.get("state", "")
    county = row.get("county", "")
    tract = row.get("tract", "")
    geoid = f"{state}{county}{tract}"
    county_fips = f"{state}{county}"

    # Map API variable names to our internal names
    mapped = {}
    for api_var, col_name in ACS_VARIABLES.items():
        mapped[col_name] = row.get(api_var)

    age_groups = _compute_age_groups(mapped)
    income_cv = _compute_income_cv(mapped)

    families_with_children = None
    mc = _safe_int(mapped.get("married_with_children"))
    mhc = _safe_int(mapped.get("male_hh_with_children"))
    fhc = _safe_int(mapped.get("female_hh_with_children"))
    parts = [v for v in [mc, mhc, fhc] if v is not None]
    if parts:
        families_with_children = sum(parts)

    seniors_below_poverty = None
    male_65_74_below = _safe_int(mapped.get("male_65_74_below_poverty"))
    male_75_plus_below = _safe_int(mapped.get("male_75_plus_below_poverty"))
    female_65_74_below = _safe_int(mapped.get("female_65_74_below_poverty"))
    female_75_plus_below = _safe_int(mapped.get("female_75_plus_below_poverty"))
    seniors_poverty_parts = [v for v in [male_65_74_below, male_75_plus_below, female_65_74_below, female_75_plus_below] if v is not None]
    if seniors_poverty_parts:
        seniors_below_poverty = sum(seniors_poverty_parts)

    seniors_living_alone = None
    male_living_alone = _safe_int(mapped.get("male_65_plus_living_alone"))
    female_living_alone = _safe_int(mapped.get("female_65_plus_living_alone"))
    seniors_alone_parts = [v for v in [male_living_alone, female_living_alone] if v is not None]
    if seniors_alone_parts:
        seniors_living_alone = sum(seniors_alone_parts)

    return {
        "geoid": geoid,
        "state_fips": state,
        "county_fips": county_fips,
        "tract_name": row.get("NAME"),
        "total_population": _safe_int(mapped.get("total_population")),
        "male_total": _safe_int(mapped.get("male_total")),
        "female_total": _safe_int(mapped.get("female_total")),
        "population_under_18": _safe_int(mapped.get("population_under_18")),
        "population_under_5": age_groups.get("population_under_5"),
        "population_5_17": age_groups.get("population_5_17"),
        "population_18_64": None,  # computed as total - under_18 - 65+ if needed
        "population_65_74": age_groups.get("population_65_74"),
        "population_75_plus": age_groups.get("population_75_plus"),
        "median_household_income": _safe_int(mapped.get("median_household_income")),
        "median_family_income": _safe_int(mapped.get("median_family_income")),
        "income_bracket_under_10k": _safe_int(mapped.get("income_bracket_under_10k")),
        "income_bracket_10k_15k": _safe_int(mapped.get("income_bracket_10k_15k")),
        "income_bracket_15k_25k": _safe_int(mapped.get("income_bracket_15k_25k")),
        "income_bracket_25k_35k": _safe_int(mapped.get("income_bracket_25k_35k")),
        "income_bracket_35k_50k": _safe_int(mapped.get("income_bracket_35k_50k")),
        "income_bracket_50k_75k": _safe_int(mapped.get("income_bracket_50k_75k")),
        "income_bracket_75k_100k": _safe_int(mapped.get("income_bracket_75k_100k")),
        "income_bracket_100k_150k": _safe_int(mapped.get("income_bracket_100k_150k")),
        "income_bracket_150k_200k": _safe_int(mapped.get("income_bracket_150k_200k")),
        "income_bracket_200k_plus": _safe_int(mapped.get("income_bracket_200k_plus")),
        "total_households": _safe_int(mapped.get("total_households")),
        "family_households": _safe_int(mapped.get("family_households")),
        "families_with_own_children": families_with_children,
        "owner_occupied": _safe_int(mapped.get("owner_occupied")),
        "renter_occupied": _safe_int(mapped.get("renter_occupied")),
        "population_below_poverty": _safe_int(mapped.get("population_below_poverty")),
        "seniors_below_poverty": seniors_below_poverty,
        "seniors_living_alone": seniors_living_alone,
        "income_cv": income_cv,
        "acs_vintage": vintage,
        # Ensure geometry keys are always present (NULL until enriched)
        "boundary": None,
        "centroid": None,
    }


async def _ingest_state(state_fips: str, vintage: str = "2022") -> tuple[int, int]:
    """Ingest all tracts for one state. Returns (fetched, geo_enriched).

    Raises on ACS fetch failure so the orchestrator can track it as an error.
    """
    async with httpx.AsyncClient() as client:
        rows = await _fetch_acs_state(client, state_fips, vintage)

    if not rows:
        return 0, 0

    tract_dicts = [_transform_tract(row, vintage) for row in rows]

    async with httpx.AsyncClient() as client:
        geom_by_geoid = await _fetch_tiger_geometries_for_state(client, state_fips)

    geo_enriched = 0
    for tract in tract_dicts:
        geom = geom_by_geoid.get(tract["geoid"])
        if geom is None:
            continue
        tract["boundary"] = from_shape(geom, srid=4326)
        tract["centroid"] = from_shape(Point(geom.representative_point().x, geom.representative_point().y), srid=4326)
        geo_enriched += 1

    # Chunk upserts — large states (CA, TX, FL) have 8000+ tracts.
    # A single INSERT ... VALUES with geometry WKB for all of them
    # can exceed PostgreSQL bind-parameter limits or cause timeouts
    # when multiple states run concurrently.
    _CENSUS_BATCH = 500
    async with async_session_factory() as session:
        for chunk_start in range(0, len(tract_dicts), _CENSUS_BATCH):
            chunk = tract_dicts[chunk_start : chunk_start + _CENSUS_BATCH]
            stmt = pg_insert(CensusTract).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=["geoid"],
                set_={
                    col: stmt.excluded[col]
                    for col in chunk[0].keys()
                    if col != "geoid"
                },
            )
            await session.execute(stmt)
        backfilled = await backfill_census_centroids(session, state_fips=state_fips)
        await session.commit()

    if backfilled:
        logger.info("State %s: backfilled %s missing tract centroids from boundaries", state_fips, backfilled)

    logger.info(
        "State %s: %d tracts inserted, %d/%d with TIGER geometry",
        state_fips, len(tract_dicts), geo_enriched, len(tract_dicts),
    )
    if geo_enriched == 0 and len(tract_dicts) > 0:
        logger.warning(
            "State %s: 0 TIGER geometries attached; all %d tracts have NULL centroid/boundary. "
            "Spatial queries for this state will fail until geometry is available.",
            state_fips, len(tract_dicts),
        )

    return len(tract_dicts), geo_enriched


@celery_app.task(name="pipeline.ingest_census.ingest_acs_data", bind=True)
def ingest_acs_data(self, vintage: str = "2022", states: list[str] | None = None):
    """Celery task: Ingest ACS 5-Year data for all states (or a subset)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_ingest_acs_data_async(vintage, states))
    finally:
        loop.close()


async def _ingest_acs_data_async(vintage: str = "2022", states: list[str] | None = None):
    """Async implementation of Census ACS ingestion."""
    target_states = states or STATE_FIPS
    total_tracts = 0
    total_geo = 0
    errors = []
    succeeded_states = []
    failed_states = []

    if not CENSUS_API_KEY:
        logger.warning(
            "CENSUS_API_KEY is not set. Requests will be rate-limited by the Census Bureau. "
            "Get a free key at https://api.census.gov/data/key_signup.html"
        )

    async with async_session_factory() as session:
        run = await start_pipeline_run(session, "census_acs")
        await session.commit()

    # Process states in small concurrent batches.
    # Keep batch size modest — each state's DB upsert can be large
    # (8000+ tracts with geometry), and running too many concurrently
    # exhausts the connection pool or hits bind-parameter limits.
    batch_size = 2 if not CENSUS_API_KEY else 3
    inter_batch_delay = 2.0 if not CENSUS_API_KEY else 1.0

    for i in range(0, len(target_states), batch_size):
        batch = target_states[i : i + batch_size]
        tasks = [_ingest_state(st, vintage) for st in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for st, result in zip(batch, results):
            if isinstance(result, Exception):
                errors.append(f"State {st}: {result}")
                failed_states.append(st)
                logger.error("FAILED to ingest state %s: %s", st, result)
            else:
                tracts, geo = result
                total_tracts += tracts
                total_geo += geo
                if tracts > 0:
                    succeeded_states.append(st)
                else:
                    failed_states.append(st)
                    errors.append(f"State {st}: ACS returned 0 tracts (possible API issue)")
                    logger.error("State %s: ACS returned 0 tracts — treating as failure", st)

        # Pause between batches to respect Census API rate limits
        if i + batch_size < len(target_states):
            await asyncio.sleep(inter_batch_delay)

    # Retry failed states once — rate-limit failures are often transient
    if failed_states and len(failed_states) < len(target_states):
        retry_states = list(failed_states)
        logger.info("Retrying %d failed states after a cooldown pause...", len(retry_states))
        await asyncio.sleep(10.0)
        failed_states.clear()
        retry_errors = []
        for st in retry_states:
            try:
                tracts, geo = await _ingest_state(st, vintage)
                if tracts > 0:
                    total_tracts += tracts
                    total_geo += geo
                    succeeded_states.append(st)
                    # Remove the original error for this state
                    errors = [e for e in errors if not e.startswith(f"State {st}:")]
                    logger.info("Retry succeeded for state %s: %d tracts", st, tracts)
                else:
                    failed_states.append(st)
                    retry_errors.append(f"State {st}: ACS returned 0 tracts on retry")
            except Exception as exc:
                failed_states.append(st)
                retry_errors.append(f"State {st}: retry failed: {exc}")
                logger.error("Retry FAILED for state %s: %s", st, exc)
            await asyncio.sleep(inter_batch_delay)
        errors.extend(retry_errors)

    # Determine pipeline status
    if len(failed_states) == 0:
        status = "success"
    elif len(succeeded_states) > 0:
        status = "success"  # partial success — some states ingested
    else:
        status = "failed"

    logger.info(
        "Census ACS ingestion complete: %d tracts (%d with geometry) from %d/%d states. "
        "Failed states (%d): %s",
        total_tracts, total_geo, len(succeeded_states), len(target_states),
        len(failed_states), ", ".join(failed_states) if failed_states else "none",
    )

    if failed_states:
        logger.error(
            "Re-run failed states with: "
            "ingest_acs_data(states=%r)",
            failed_states,
        )

    async with async_session_factory() as session:
        await finish_pipeline_run(
            session,
            run,
            status=status,
            records_processed=total_tracts,
            records_inserted=total_tracts,
            error_message="; ".join(errors) if errors else None,
            metadata={
                "vintage": vintage,
                "states_requested": len(target_states),
                "states_succeeded": len(succeeded_states),
                "states_failed": len(failed_states),
                "failed_states": failed_states,
                "total_tracts": total_tracts,
                "tracts_with_geometry": total_geo,
            },
        )
        await session.commit()

    return {
        "total_tracts": total_tracts,
        "tracts_with_geometry": total_geo,
        "states_succeeded": len(succeeded_states),
        "states_failed": len(failed_states),
        "failed_states": failed_states,
        "errors": errors,
    }


async def ingest_historical(vintage: str = "2017", states: list[str] | None = None):
    """Ingest a historical ACS vintage into the history table for trend analysis."""
    target_states = states or STATE_FIPS
    total = 0

    for state_fips in target_states:
        async with httpx.AsyncClient() as client:
            try:
                rows = await _fetch_acs_state(client, state_fips, vintage)
            except Exception as exc:
                logger.error("Historical ingest: failed to fetch state %s: %s", state_fips, exc)
                continue

        if not rows:
            continue

        history_rows = []
        for row in rows:
            transformed = _transform_tract(row, vintage)
            history_rows.append({
                "geoid": transformed["geoid"],
                "acs_vintage": vintage,
                "total_population": transformed["total_population"],
                "population_5_17": transformed["population_5_17"],
                "median_household_income": transformed["median_household_income"],
                "families_with_own_children": transformed["families_with_own_children"],
                "population_65_74": transformed["population_65_74"],
                "population_75_plus": transformed["population_75_plus"],
                "total_households": transformed["total_households"],
            })

        async with async_session_factory() as session:
            stmt = pg_insert(CensusTractHistory).values(history_rows)
            stmt = stmt.on_conflict_do_update(
                constraint="ix_history_geoid_vintage",
                set_={
                    "total_population": stmt.excluded.total_population,
                    "population_5_17": stmt.excluded.population_5_17,
                    "median_household_income": stmt.excluded.median_household_income,
                    "families_with_own_children": stmt.excluded.families_with_own_children,
                    "population_65_74": stmt.excluded.population_65_74,
                    "population_75_plus": stmt.excluded.population_75_plus,
                    "total_households": stmt.excluded.total_households,
                },
            )
            await session.execute(stmt)
            await session.commit()
            total += len(history_rows)

    return {"vintage": vintage, "total_records": total}
