"""Census ACS 5-Year bulk data ingestion pipeline.

Downloads ACS Detailed Tables from the Census Bureau API in bulk (state by state),
transforms them into CensusTract rows, and upserts into PostgreSQL.

Also downloads TIGER/Line tract shapefiles for boundaries and centroids.
"""

import asyncio
import logging
import os
import httpx
from geoalchemy2.shape import from_shape
from shapely.geometry import MultiPolygon, Point, shape
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


TIGER_TRACTS_ARCGIS = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Tracts_Blocks/MapServer/4/query"


async def _fetch_tiger_geometries_for_state(client: httpx.AsyncClient, state_fips: str) -> list[dict]:
    """Fetch TIGER tract geometries for a state from Census ArcGIS endpoint."""
    offset = 0
    page_size = 1000
    features: list[dict] = []

    while True:
        params = {
            "where": f"STATE='{state_fips}'",
            "outFields": "GEOID",
            "outSR": 4326,
            "returnGeometry": "true",
            "resultOffset": offset,
            "resultRecordCount": page_size,
            "f": "geojson",
        }
        try:
            resp = await client.get(TIGER_TRACTS_ARCGIS, params=params, timeout=120.0)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            logger.warning("Failed to fetch TIGER geometries for state %s at offset %s: %s", state_fips, offset, exc)
            break

        page = payload.get("features", []) if isinstance(payload, dict) else []
        if not page:
            break
        features.extend(page)
        if len(page) < page_size:
            break
        offset += page_size

    return features


def _feature_to_geometry(feature: dict):
    geometry = feature.get("geometry")
    if not geometry:
        return None
    try:
        geom = shape(geometry)
    except Exception:
        return None

    if geom.geom_type == "Polygon":
        geom = MultiPolygon([geom])
    if not isinstance(geom, MultiPolygon):
        return None

    return geom


def _feature_geoid(feature: dict) -> str | None:
    props = feature.get("properties", {})
    geoid = props.get("GEOID") or props.get("geoid")
    if geoid:
        return str(geoid)
    return None

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
    """Fetch ACS 5-Year data for all tracts in a state."""
    var_list = ",".join(ACS_VARIABLES.keys())
    params = {
        "get": f"NAME,{var_list}",
        "for": "tract:*",
        "in": f"state:{state_fips}",
    }
    if CENSUS_API_KEY:
        params["key"] = CENSUS_API_KEY

    url = f"{CENSUS_API_BASE}/{vintage}/acs/acs5"

    try:
        resp = await client.get(url, params=params, timeout=60.0)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"Failed to fetch ACS for state {state_fips}: {e}")
        return []

    if not data or len(data) < 2:
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
        "income_cv": income_cv,
        "acs_vintage": vintage,
    }


async def _ingest_state(state_fips: str, vintage: str = "2022") -> tuple[int, int]:
    """Ingest all tracts for one state. Returns (processed, upserted)."""
    async with httpx.AsyncClient() as client:
        rows = await _fetch_acs_state(client, state_fips, vintage)

    if not rows:
        return 0, 0

    tract_dicts = [_transform_tract(row, vintage) for row in rows]

    async with httpx.AsyncClient() as client:
        tiger_features = await _fetch_tiger_geometries_for_state(client, state_fips)

    geom_by_geoid = {}
    for feature in tiger_features:
        geoid = _feature_geoid(feature)
        geom = _feature_to_geometry(feature)
        if geoid and geom is not None:
            geom_by_geoid[geoid] = geom

    for tract in tract_dicts:
        geom = geom_by_geoid.get(tract["geoid"])
        if geom is None:
            continue
        tract["boundary"] = from_shape(geom, srid=4326)
        tract["centroid"] = from_shape(Point(geom.representative_point().x, geom.representative_point().y), srid=4326)

    async with async_session_factory() as session:
        stmt = pg_insert(CensusTract).values(tract_dicts)
        stmt = stmt.on_conflict_do_update(
            index_elements=["geoid"],
            set_={
                col: stmt.excluded[col]
                for col in tract_dicts[0].keys()
                if col != "geoid"
            },
        )
        await session.execute(stmt)
        backfilled = await backfill_census_centroids(session, state_fips=state_fips)
        await session.commit()

    if backfilled:
        logger.info("State %s: backfilled %s missing tract centroids from boundaries", state_fips, backfilled)

    if geom_by_geoid:
        logger.info("State %s: attached %s TIGER tract geometries", state_fips, len(geom_by_geoid))
    else:
        logger.warning("State %s: no TIGER geometries attached; centroid availability depends on existing boundary data", state_fips)

    return len(rows), len(tract_dicts)


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
    total_processed = 0
    total_upserted = 0
    errors = []

    async with async_session_factory() as session:
        run = await start_pipeline_run(session, "census_acs")
        await session.commit()

    # Process states in batches of 5 to avoid overwhelming the Census API
    batch_size = 5
    for i in range(0, len(target_states), batch_size):
        batch = target_states[i : i + batch_size]
        tasks = [_ingest_state(st, vintage) for st in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for st, result in zip(batch, results):
            if isinstance(result, Exception):
                errors.append(f"State {st}: {result}")
                logger.error(f"Failed to ingest state {st}: {result}")
            else:
                processed, upserted = result
                total_processed += processed
                total_upserted += upserted
                logger.info(f"State {st}: {processed} tracts processed, {upserted} upserted")

    async with async_session_factory() as session:
        run = await start_pipeline_run(session, "census_acs")
        await finish_pipeline_run(
            session,
            run,
            status="success" if not errors else "partial",
            records_processed=total_processed,
            records_inserted=total_upserted,
            error_message="; ".join(errors) if errors else None,
            metadata={"vintage": vintage, "states": len(target_states)},
        )
        await session.commit()

    return {
        "processed": total_processed,
        "upserted": total_upserted,
        "errors": errors,
    }


async def ingest_historical(vintage: str = "2017", states: list[str] | None = None):
    """Ingest a historical ACS vintage into the history table for trend analysis."""
    target_states = states or STATE_FIPS
    total = 0

    for state_fips in target_states:
        async with httpx.AsyncClient() as client:
            rows = await _fetch_acs_state(client, state_fips, vintage)

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
