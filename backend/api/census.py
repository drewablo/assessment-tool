import asyncio
import logging
import math
import httpx
import os
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Tuple

from utils import bearing, decay_weight, direction_from_bearing, haversine_miles

logger = logging.getLogger(__name__)



_DEMOGRAPHICS_CACHE_TTL_SECONDS = int(os.getenv("DEMOGRAPHICS_CACHE_TTL_SECONDS", "1800"))
_DEMOGRAPHICS_CACHE_MAX = int(os.getenv("DEMOGRAPHICS_CACHE_MAX", "256"))
_DEMOGRAPHICS_CACHE: Dict[str, Tuple[datetime, dict]] = {}


def _demographics_cache_key(
    lat: float,
    lon: float,
    county_fips: str,
    state_fips: str,
    radius_miles: float,
    isochrone_polygon: Optional[dict],
) -> str:
    polygon_part = "none"
    if isochrone_polygon:
        import json
        import hashlib
        polygon_part = hashlib.sha256(json.dumps(isochrone_polygon, sort_keys=True).encode()).hexdigest()[:16]
    return "|".join([
        f"{lat:.5f}",
        f"{lon:.5f}",
        county_fips.strip(),
        state_fips.strip(),
        f"{radius_miles:.2f}",
        polygon_part,
    ])


def _demographics_cache_get(key: str) -> Optional[dict]:
    item = _DEMOGRAPHICS_CACHE.get(key)
    if not item:
        return None
    expires_at, payload = item
    if datetime.now(timezone.utc) >= expires_at:
        _DEMOGRAPHICS_CACHE.pop(key, None)
        return None
    return payload.copy()


def _demographics_cache_set(key: str, payload: dict) -> None:
    if len(_DEMOGRAPHICS_CACHE) >= _DEMOGRAPHICS_CACHE_MAX:
        oldest_key = next(iter(_DEMOGRAPHICS_CACHE))
        _DEMOGRAPHICS_CACHE.pop(oldest_key, None)
    _DEMOGRAPHICS_CACHE[key] = (datetime.now(timezone.utc) + timedelta(seconds=_DEMOGRAPHICS_CACHE_TTL_SECONDS), payload.copy())

def _centroid_in_polygon(tract_lat: float, tract_lon: float, polygon_geojson: dict) -> bool:
    """
    Check whether a census tract centroid falls within the isochrone polygon.
    Uses shapely for robust point-in-polygon arithmetic.
    Coordinates follow GeoJSON convention: [longitude, latitude].
    """
    try:
        from shapely.geometry import Point, shape  # lazy import — only needed for isochrone mode
        point = Point(tract_lon, tract_lat)         # shapely: (x=lon, y=lat)
        polygon = shape(polygon_geojson)
        return polygon.contains(point)
    except Exception as e:
        logger.warning("Point-in-polygon error: %s", e)
        return False


async def _get_tracts_in_polygon(
    polygon_geojson: dict,
    school_lat: float,
    school_lon: float,
) -> List[Dict]:
    """
    Query Census TIGER API using the isochrone polygon's bounding box,
    then filter to tracts whose centroid lies inside the polygon.
    Handles both Polygon and MultiPolygon geometries.
    """
    geom_type = polygon_geojson.get("type", "")
    if geom_type == "Polygon":
        all_coords = polygon_geojson.get("coordinates", [[]])[0]
    elif geom_type == "MultiPolygon":
        all_coords = [
            c
            for poly in polygon_geojson.get("coordinates", [])
            for ring in poly
            for c in ring
        ]
    else:
        return []

    if not all_coords:
        return []

    lons = [c[0] for c in all_coords]
    lats = [c[1] for c in all_coords]
    xmin, xmax = min(lons), max(lons)
    ymin, ymax = min(lats), max(lats)

    params = {
        "geometry": f"{xmin},{ymin},{xmax},{ymax}",
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "GEOID,INTPTLAT,INTPTLON,STATE,COUNTY,TRACT",
        "returnGeometry": "false",
        "resultRecordCount": "2000",
        "f": "json",
    }

    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        try:
            response = await client.get(TIGER_TRACTS_URL, params=params)
            response.raise_for_status()
            data = response.json()

            tracts = []
            for feature in data.get("features", []):
                attrs = feature.get("attributes", {})
                try:
                    tract_lat = float(attrs["INTPTLAT"])
                    tract_lon = float(attrs["INTPTLON"])
                except (TypeError, ValueError, KeyError):
                    continue

                if _centroid_in_polygon(tract_lat, tract_lon, polygon_geojson):
                    dist = haversine_miles(school_lat, school_lon, tract_lat, tract_lon)
                    tracts.append({
                        "geoid": attrs.get("GEOID", ""),
                        "state": attrs.get("STATE", ""),
                        "county": attrs.get("COUNTY", ""),
                        "tract": attrs.get("TRACT", ""),
                        "lat": tract_lat,
                        "lon": tract_lon,
                        "distance_miles": round(dist, 3),
                    })

            return tracts

        except Exception as e:
            logger.warning("TIGER API error (polygon): %s", e)
            return []

ACS_BASE_URL = "https://api.census.gov/data/2022/acs/acs5"
ACS_2017_BASE_URL = "https://api.census.gov/data/2017/acs/acs5"
TIGER_TRACTS_URL = (
    "https://tigerweb.geo.census.gov/arcgis/rest/services/"
    "TIGERweb/tigerWMS_Current/MapServer/8/query"
)
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "")
_HTTP_TIMEOUT = 30.0   # TIGER + ACS endpoint timeout (seconds)
_TREND_TIMEOUT = 20.0  # 2017 ACS trend baseline timeout (seconds)
_ACS_COUNTY_CONCURRENCY = 4  # max concurrent county-group ACS queries

# ACS 5-year variables we pull for the feasibility analysis
ACS_VARIABLES = [
    "NAME",
    "B01003_001E",  # Total population
    "B09001_001E",  # Population under 18 years
    "B09001_003E",  # Population under 5 years (pipeline indicator)
    "B09001_004E",  # Population 5 to 11 years
    "B09001_005E",  # Population 12 to 17 years
    "B19013_001E",  # Median household income (past 12 months)
    "B19013_001M",  # Median household income — margin of error (90% CI)
    # Median family income hierarchy (most targeted → most available):
    #   B19125_002E — median income for families WITH own children under 18 (ideal for school affordability)
    #   B19113_001E — median income for all family households (fallback when B19125 is suppressed)
    "B19125_002E",  # Median family income, families with own children under 18
    "B19113_001E",  # Median family income, all families (suppression fallback)
    "B11001_001E",  # Total households
    "B11003_001E",  # Family households with own children under 18
    "B25003_001E",  # Total occupied housing units
    "B25003_002E",  # Owner-occupied housing units
    "B25070_001E",  # Total renter households (gross rent as % income denominator)
    "B25070_007E",  # Gross rent 30.0–34.9% of income
    "B25070_008E",  # Gross rent 35.0–39.9% of income
    "B25070_009E",  # Gross rent 40.0–49.9% of income
    "B25070_010E",  # Gross rent 50.0% or more
    "B25070_011E",  # Gross rent not computed
    "B25003_003E",  # Renter-occupied units
    "B25002_003E",  # Vacant housing units
    # Income distribution (B19001) — full bracket breakdown for income-first
    # addressable market estimation (propensity by income bracket).
    "B19001_001E",  # Total households (income table)
    "B19001_002E",  # Households Less than $10,000
    "B19001_003E",  # Households $10,000–$14,999
    "B19001_004E",  # Households $15,000–$19,999
    "B19001_005E",  # Households $20,000–$24,999
    "B19001_006E",  # Households $25,000–$29,999
    "B19001_007E",  # Households $30,000–$34,999
    "B19001_008E",  # Households $35,000–$39,999
    "B19001_009E",  # Households $40,000–$44,999
    "B19001_010E",  # Households $45,000–$49,999
    "B19001_011E",  # Households $50,000–$59,999
    "B19001_012E",  # Households $60,000–$74,999
    "B19001_013E",  # Households $75,000–$99,999
    "B19001_014E",  # Households $100,000–$124,999
    "B19001_015E",  # Households $125,000–$149,999
    "B19001_016E",  # Households $150,000–$199,999
    "B19001_017E",  # Households $200,000+
    # Sex by age — used to size the market by gender and grade level
    "B01001_004E",  # Male, 5 to 9 years
    "B01001_005E",  # Male, 10 to 14 years
    "B01001_006E",  # Male, 15 to 17 years
    "B01001_028E",  # Female, 5 to 9 years
    "B01001_029E",  # Female, 10 to 14 years
    "B01001_030E",  # Female, 15 to 17 years
    # B14002: Sex by school enrollment by level by type of school
    # Each level has three consecutive variables: (total, public, private).
    # We only pull the variables we actually use.
    "B14002_003E",  # Male: enrolled in school (all levels) — total enrolled denominator
    "B14002_027E",  # Female: enrolled in school (all levels) — total enrolled denominator
    # Male private K–12 (nursery/preschool, K, gr 1–4, gr 5–8, gr 9–12)
    "B14002_006E",
    "B14002_009E",
    "B14002_012E",
    "B14002_015E",
    "B14002_018E",
    # Female private K–12
    "B14002_030E",
    "B14002_033E",
    "B14002_036E",
    "B14002_039E",
    "B14002_042E",
    "B01001_020E",  # Male 65-66
    "B01001_021E",  # Male 67-69
    "B01001_022E",  # Male 70-74
    "B01001_023E",  # Male 75-79
    "B01001_024E",  # Male 80-84
    "B01001_025E",  # Male 85+
    "B01001_044E",  # Female 65-66
    "B01001_045E",  # Female 67-69
    "B01001_046E",  # Female 70-74
    "B01001_047E",  # Female 75-79
    "B01001_048E",  # Female 80-84
    "B01001_049E",  # Female 85+
    "B11010_003E",  # Male householder 65+ living alone
    "B11010_006E",  # Female householder 65+ living alone
    "B17001_015E",  # Male 65-74 below poverty
    "B17001_016E",  # Male 75+ below poverty
    "B17001_029E",  # Female 65-74 below poverty
    "B17001_030E",  # Female 75+ below poverty
]

# Private K–12 variable IDs from B14002 (used in aggregation helpers below)
_B14002_PRIVATE_K12 = [
    "B14002_006E",  # Male: nursery/preschool: private
    "B14002_009E",  # Male: kindergarten: private
    "B14002_012E",  # Male: grade 1–4: private
    "B14002_015E",  # Male: grade 5–8: private
    "B14002_018E",  # Male: grade 9–12: private
    "B14002_030E",  # Female: nursery/preschool: private
    "B14002_033E",  # Female: kindergarten: private
    "B14002_036E",  # Female: grade 1–4: private
    "B14002_039E",  # Female: grade 5–8: private
    "B14002_042E",  # Female: grade 9–12: private
]

# Households earning $100k+ — used for the high-income scoring bonus.
# B19001_013E ($75–99k) is intentionally excluded; see analysis.py _HIGH_INCOME_BONUS_SEGMENTS.
_HIGH_INCOME_VARS = ["B19001_014E", "B19001_015E", "B19001_016E", "B19001_017E"]

# Households earning $75k+ — used for income-qualified directional analysis.
_INCOME_QUALIFIED_VARS = ["B19001_013E", "B19001_014E", "B19001_015E", "B19001_016E", "B19001_017E"]

# Full B19001 bracket variables with midpoint income values.
# Used to build the income distribution for the income-first addressable market model.
_B19001_BRACKET_VARS = [
    ("B19001_002E", 5_000),       # Less than $10,000
    ("B19001_003E", 12_500),      # $10,000–$14,999
    ("B19001_004E", 17_500),      # $15,000–$19,999
    ("B19001_005E", 22_500),      # $20,000–$24,999
    ("B19001_006E", 27_500),      # $25,000–$29,999
    ("B19001_007E", 32_500),      # $30,000–$34,999
    ("B19001_008E", 37_500),      # $35,000–$39,999
    ("B19001_009E", 42_500),      # $40,000–$44,999
    ("B19001_010E", 47_500),      # $45,000–$49,999
    ("B19001_011E", 55_000),      # $50,000–$59,999
    ("B19001_012E", 67_500),      # $60,000–$74,999
    ("B19001_013E", 87_500),      # $75,000–$99,999
    ("B19001_014E", 112_500),     # $100,000–$124,999
    ("B19001_015E", 137_500),     # $125,000–$149,999
    ("B19001_016E", 175_000),     # $150,000–$199,999
    ("B19001_017E", 250_000),     # $200,000 or more
]


def _safe_int(value: object) -> Optional[int]:
    """Coerce a value to a non-negative int, returning None on failure or negative values."""
    try:
        v = int(value)
        return v if v >= 0 else None
    except (TypeError, ValueError):
        return None


async def _get_tracts_in_radius(
    lat: float, lon: float, radius_miles: float
) -> List[Dict]:
    """
    Query Census TIGER REST API for census tracts within the bounding box,
    then filter to those whose internal point (centroid) falls within radius_miles.
    Returns list of dicts with geoid, state, county, tract fields.
    """
    lat_buffer = radius_miles / 69.0
    lon_buffer = radius_miles / (69.0 * math.cos(math.radians(lat)))

    xmin = lon - lon_buffer
    ymin = lat - lat_buffer
    xmax = lon + lon_buffer
    ymax = lat + lat_buffer

    params = {
        "geometry": f"{xmin},{ymin},{xmax},{ymax}",
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "GEOID,INTPTLAT,INTPTLON,STATE,COUNTY,TRACT",
        "returnGeometry": "false",
        "resultRecordCount": "2000",
        "f": "json",
    }

    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        try:
            response = await client.get(TIGER_TRACTS_URL, params=params)
            response.raise_for_status()
            data = response.json()

            tracts = []
            for feature in data.get("features", []):
                attrs = feature.get("attributes", {})
                try:
                    tract_lat = float(attrs["INTPTLAT"])
                    tract_lon = float(attrs["INTPTLON"])
                except (TypeError, ValueError, KeyError):
                    continue

                dist = haversine_miles(lat, lon, tract_lat, tract_lon)
                if dist <= radius_miles:
                    tracts.append({
                        "geoid": attrs.get("GEOID", ""),
                        "state": attrs.get("STATE", ""),
                        "county": attrs.get("COUNTY", ""),
                        "tract": attrs.get("TRACT", ""),
                        "lat": tract_lat,
                        "lon": tract_lon,
                        "distance_miles": round(dist, 3),
                    })

            return tracts

        except Exception as e:
            logger.warning("TIGER API error: %s", e)
            return []


_ACS_BATCH_SIZE = 48  # Census API allows max 50 vars; reserve slots for geo columns


def _chunk_variables(variables: List[str], batch_size: int = _ACS_BATCH_SIZE) -> List[List[str]]:
    """Split a variable list into batches. NAME is always included in every batch."""
    # Remove NAME so we can control placement; always prepend it
    vars_without_name = [v for v in variables if v != "NAME"]
    batches = []
    for i in range(0, len(vars_without_name), batch_size):
        batches.append(["NAME"] + vars_without_name[i : i + batch_size])
    return batches if batches else [["NAME"]]


async def _get_acs_for_county_tracts(
    state: str,
    county: str,
    tract_geoids: set,
    client: httpx.AsyncClient,
) -> List[dict]:
    """
    Query ACS for all tracts in a given state/county, then return only those
    whose GEOID is in tract_geoids.  Batches variables to stay under the
    Census API 50-variable limit.
    """
    batches = _chunk_variables(ACS_VARIABLES)

    # Collect all batch results; key rows by tract code so we can merge them
    merged: dict[str, dict] = {}

    for batch in batches:
        params = {
            "get": ",".join(batch),
            "for": "tract:*",
            "in": f"state:{state} county:{county}",
        }
        if CENSUS_API_KEY:
            params["key"] = CENSUS_API_KEY

        try:
            response = await client.get(ACS_BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            if len(data) < 2:
                continue

            headers = data[0]
            for values in data[1:]:
                row = dict(zip(headers, values))
                tract_code = row.get("tract", "").zfill(6)
                geoid = state.zfill(2) + county.zfill(3) + tract_code
                if geoid not in tract_geoids:
                    continue
                if geoid not in merged:
                    merged[geoid] = row
                else:
                    merged[geoid].update(row)

        except Exception as e:
            logger.warning("ACS tract query error for state=%s, county=%s: %s", state, county, e)

    return list(merged.values())


# ---------------------------------------------------------------------------
# Tract-row accumulation helpers — each mutates the accumulators dict `a`
# ---------------------------------------------------------------------------

def _accumulate_basic_pop(row: dict, dw: float, a: dict) -> tuple:
    """Accumulate population, school-age, and household counts.
    Returns (pop, raw_5_11, raw_12_17) for callers that need them."""
    pop = _safe_int(row.get("B01003_001E")) or 0
    a["total_pop"] += pop
    raw_5_11 = _safe_int(row.get("B09001_004E")) or 0
    raw_12_17 = _safe_int(row.get("B09001_005E")) or 0
    a["pop_under_18"] += _safe_int(row.get("B09001_001E")) or 0
    a["pop_under_5"] += (_safe_int(row.get("B09001_003E")) or 0) * dw
    a["pop_5_11"] += raw_5_11 * dw
    a["pop_12_17"] += raw_12_17 * dw
    a["gravity_weighted_school_age_pop"] += (raw_5_11 + raw_12_17) * dw
    a["total_households"] += (
        (_safe_int(row.get("B19001_001E")) or _safe_int(row.get("B11001_001E")) or 0) * dw
    )
    a["families_with_children"] += (_safe_int(row.get("B11003_001E")) or 0) * dw
    a["total_housing"] += _safe_int(row.get("B25003_001E")) or 0
    a["owner_occupied"] += _safe_int(row.get("B25003_002E")) or 0
    return pop, raw_5_11, raw_12_17


def _accumulate_housing(row: dict, dw: float, a: dict) -> None:
    """Accumulate renter, cost-burdened, and vacant housing unit counts."""
    a["renter_occupied_units"] += (_safe_int(row.get("B25003_003E")) or 0) * dw
    a["vacant_housing_units"] += (_safe_int(row.get("B25002_003E")) or 0) * dw
    a["renter_households"] += (_safe_int(row.get("B25070_001E")) or 0) * dw
    # B25070_007E–010E = rent ≥30% of income (HUD cost-burdened definition).
    # B25070_011E ("not computed") is excluded — it represents households with
    # zero/negative income or no cash rent, NOT cost-burdened households.
    a["cost_burdened_renter_households"] += (
        (_safe_int(row.get("B25070_007E")) or 0)
        + (_safe_int(row.get("B25070_008E")) or 0)
        + (_safe_int(row.get("B25070_009E")) or 0)
        + (_safe_int(row.get("B25070_010E")) or 0)
    ) * dw


def _accumulate_age_sex_school(row: dict, dw: float, a: dict) -> None:
    """Accumulate sex-by-age breakdowns, private school enrollment, and high-income households."""
    a["male_5_9"]   += (_safe_int(row.get("B01001_004E")) or 0) * dw
    a["male_10_14"] += (_safe_int(row.get("B01001_005E")) or 0) * dw
    a["male_15_17"] += (_safe_int(row.get("B01001_006E")) or 0) * dw
    a["female_5_9"]   += (_safe_int(row.get("B01001_028E")) or 0) * dw
    a["female_10_14"] += (_safe_int(row.get("B01001_029E")) or 0) * dw
    a["female_15_17"] += (_safe_int(row.get("B01001_030E")) or 0) * dw
    for v in _B14002_PRIVATE_K12:
        a["private_school_enrolled"] += (_safe_int(row.get(v)) or 0) * dw
    a["total_school_enrolled"] += (
        ((_safe_int(row.get("B14002_003E")) or 0)
        + (_safe_int(row.get("B14002_027E")) or 0)) * dw
    )
    # $100k+ households (B19001_014E and above); B19001_013E ($75–99k) excluded
    # because that bracket is too low to reliably afford Catholic school tuition.
    a["high_income"] += sum((_safe_int(row.get(v)) or 0) for v in _HIGH_INCOME_VARS) * dw
    # Full income distribution brackets (B19001_002E–B19001_017E)
    for var, _ in _B19001_BRACKET_VARS:
        a["income_brackets"][var] += (_safe_int(row.get(var)) or 0) * dw


def _accumulate_seniors(row: dict, dw: float, a: dict) -> None:
    """Accumulate senior population counts (65+, 75+, living alone, near-poverty)."""
    a["seniors_65_plus"] += (
        (_safe_int(row.get("B01001_020E")) or 0)
        + (_safe_int(row.get("B01001_021E")) or 0)
        + (_safe_int(row.get("B01001_022E")) or 0)
        + (_safe_int(row.get("B01001_023E")) or 0)
        + (_safe_int(row.get("B01001_024E")) or 0)
        + (_safe_int(row.get("B01001_025E")) or 0)
        + (_safe_int(row.get("B01001_044E")) or 0)
        + (_safe_int(row.get("B01001_045E")) or 0)
        + (_safe_int(row.get("B01001_046E")) or 0)
        + (_safe_int(row.get("B01001_047E")) or 0)
        + (_safe_int(row.get("B01001_048E")) or 0)
        + (_safe_int(row.get("B01001_049E")) or 0)
    ) * dw
    a["seniors_75_plus"] += (
        (_safe_int(row.get("B01001_023E")) or 0)
        + (_safe_int(row.get("B01001_024E")) or 0)
        + (_safe_int(row.get("B01001_025E")) or 0)
        + (_safe_int(row.get("B01001_047E")) or 0)
        + (_safe_int(row.get("B01001_048E")) or 0)
        + (_safe_int(row.get("B01001_049E")) or 0)
    ) * dw
    a["seniors_living_alone"] += (
        (_safe_int(row.get("B11010_003E")) or 0) + (_safe_int(row.get("B11010_006E")) or 0)
    ) * dw
    a["seniors_below_200pct_poverty"] += (
        (_safe_int(row.get("B17001_015E")) or 0)
        + (_safe_int(row.get("B17001_016E")) or 0)
        + (_safe_int(row.get("B17001_029E")) or 0)
        + (_safe_int(row.get("B17001_030E")) or 0)
    ) * dw


def _accumulate_income_stats(row: dict, pop: int, dw: float, a: dict) -> None:
    """Accumulate population-weighted income sums and coefficient-of-variation components."""
    median_inc = _safe_int(row.get("B19013_001E"))
    income_moe = _safe_int(row.get("B19013_001M"))
    if median_inc and median_inc > 0 and pop > 0:
        a["weighted_income_sum"] += median_inc * pop * dw
        a["income_pop_sum"] += pop * dw
        if income_moe and income_moe > 0:
            cv = income_moe / (1.645 * median_inc)
            a["income_cv_numerator"] += cv * pop * dw
            a["income_cv_denominator"] += pop * dw
    # Median family income (B19125_002E preferred; B19113_001E fallback when suppressed)
    # B19125_002E = families *with* own children — most relevant for school affordability.
    family_inc = _safe_int(row.get("B19125_002E")) or _safe_int(row.get("B19113_001E"))
    if family_inc and family_inc > 0 and pop > 0:
        a["family_income_sum"] += family_inc * pop * dw
        a["family_income_pop_sum"] += pop * dw


def _growth_signal(ratio: float) -> str:
    """Classify pipeline ratio into a growth signal label."""
    if ratio > 0.35:
        return "Growing"
    elif ratio >= 0.2:
        return "Stable"
    else:
        return "Declining"


def _build_direction_details(
    pop_by_dir: dict,
    income_qualified_by_dir: dict,
    pipeline_under5_by_dir: dict,
    pipeline_school_age_by_dir: dict,
) -> dict:
    """Build the rich per-direction detail dict from accumulated tract data."""
    details = {}
    for d in pop_by_dir:
        school_age = pop_by_dir[d]
        iq = int(round(income_qualified_by_dir[d]))
        under5 = pipeline_under5_by_dir[d]
        sa = pipeline_school_age_by_dir[d]
        ratio = under5 / sa if sa > 0 else None
        details[d] = {
            "school_age_pop": school_age,
            "income_qualified_pop": iq,
            "pipeline_ratio": round(ratio, 3) if ratio is not None else None,
            "growth_signal": _growth_signal(ratio) if ratio is not None else None,
        }
    return details


def _aggregate_tract_rows(
    rows: List[dict],
    tract_centroids: Optional[Dict[str, dict]] = None,
    school_lat: Optional[float] = None,
    school_lon: Optional[float] = None,
) -> dict:
    """
    Aggregate ACS tract rows into a single demographics dict.
    Counts are summed; median income uses a population-weighted average.
    """
    if not rows:
        return {}

    a: dict = {
        "total_pop": 0, "pop_under_18": 0, "pop_under_5": 0.0, "pop_5_11": 0.0, "pop_12_17": 0.0,
        "gravity_weighted_school_age_pop": 0.0,
        "total_households": 0.0, "families_with_children": 0.0,
        "total_housing": 0, "owner_occupied": 0,
        "renter_households": 0.0, "cost_burdened_renter_households": 0.0,
        "renter_occupied_units": 0.0, "vacant_housing_units": 0.0,
        "male_5_9": 0.0, "male_10_14": 0.0, "male_15_17": 0.0,
        "female_5_9": 0.0, "female_10_14": 0.0, "female_15_17": 0.0,
        "private_school_enrolled": 0.0, "total_school_enrolled": 0.0,
        "high_income": 0.0,
        "income_brackets": {var: 0.0 for var, _ in _B19001_BRACKET_VARS},
        "seniors_65_plus": 0.0, "seniors_75_plus": 0.0,
        "seniors_living_alone": 0.0, "seniors_below_200pct_poverty": 0.0,
        "weighted_income_sum": 0.0, "income_pop_sum": 0.0,
        "income_cv_numerator": 0.0, "income_cv_denominator": 0.0,
        "family_income_sum": 0.0, "family_income_pop_sum": 0.0,
    }

    county_names: set = set()
    _dirs = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    pop_by_dir = {d: 0 for d in _dirs}
    income_qualified_by_dir = {d: 0.0 for d in _dirs}
    pipeline_under5_by_dir = {d: 0.0 for d in _dirs}
    pipeline_school_age_by_dir = {d: 0.0 for d in _dirs}
    # Elder care directional buckets
    seniors_65_by_dir = {d: 0.0 for d in _dirs}
    seniors_75_by_dir = {d: 0.0 for d in _dirs}
    seniors_alone_by_dir = {d: 0.0 for d in _dirs}
    seniors_poverty_by_dir = {d: 0.0 for d in _dirs}
    # Housing directional buckets
    cost_burdened_by_dir = {d: 0.0 for d in _dirs}
    renter_hh_by_dir = {d: 0.0 for d in _dirs}
    use_directional = bool(
        tract_centroids and school_lat is not None and school_lon is not None and len(rows) >= 6
    )

    for row in rows:
        geoid = (
            f"{row.get('state', '').zfill(2)}{row.get('county', '').zfill(3)}{row.get('tract', '').zfill(6)}"
        )
        centroid = tract_centroids.get(geoid) if tract_centroids else None
        dist = (centroid or {}).get("distance_miles")
        dw = decay_weight(float(dist)) if (tract_centroids and dist is not None) else 1.0

        pop, raw_5_11, raw_12_17 = _accumulate_basic_pop(row, dw, a)
        _accumulate_housing(row, dw, a)
        _accumulate_age_sex_school(row, dw, a)
        _accumulate_seniors(row, dw, a)
        _accumulate_income_stats(row, pop, dw, a)

        if use_directional and centroid:
            b = bearing(school_lat, school_lon, centroid["lat"], centroid["lon"])
            d = direction_from_bearing(b)
            tract_school_age = raw_5_11 + raw_12_17
            pop_by_dir[d] += tract_school_age

            # Income-qualified school-age pop: share of HH >= $75k * school-age pop
            total_hh = _safe_int(row.get("B19001_001E")) or 0
            hh_75k_plus = sum((_safe_int(row.get(v)) or 0) for v in _INCOME_QUALIFIED_VARS)
            iq_share = hh_75k_plus / total_hh if total_hh > 0 else 0.0
            income_qualified_by_dir[d] += tract_school_age * iq_share

            # Pipeline ratio components (pop under 5 vs school-age 5-17)
            pop_under_5 = _safe_int(row.get("B09001_003E")) or 0
            pipeline_under5_by_dir[d] += pop_under_5
            pipeline_school_age_by_dir[d] += tract_school_age

            # Elder care populations by direction (raw counts, no decay weighting for direction bins)
            tract_seniors_65 = (
                (_safe_int(row.get("B01001_020E")) or 0) + (_safe_int(row.get("B01001_021E")) or 0)
                + (_safe_int(row.get("B01001_022E")) or 0) + (_safe_int(row.get("B01001_023E")) or 0)
                + (_safe_int(row.get("B01001_024E")) or 0) + (_safe_int(row.get("B01001_025E")) or 0)
                + (_safe_int(row.get("B01001_044E")) or 0) + (_safe_int(row.get("B01001_045E")) or 0)
                + (_safe_int(row.get("B01001_046E")) or 0) + (_safe_int(row.get("B01001_047E")) or 0)
                + (_safe_int(row.get("B01001_048E")) or 0) + (_safe_int(row.get("B01001_049E")) or 0)
            )
            tract_seniors_75 = (
                (_safe_int(row.get("B01001_023E")) or 0) + (_safe_int(row.get("B01001_024E")) or 0)
                + (_safe_int(row.get("B01001_025E")) or 0) + (_safe_int(row.get("B01001_047E")) or 0)
                + (_safe_int(row.get("B01001_048E")) or 0) + (_safe_int(row.get("B01001_049E")) or 0)
            )
            tract_seniors_alone = (
                (_safe_int(row.get("B11010_003E")) or 0) + (_safe_int(row.get("B11010_006E")) or 0)
            )
            tract_seniors_poverty = (
                (_safe_int(row.get("B17001_015E")) or 0) + (_safe_int(row.get("B17001_016E")) or 0)
                + (_safe_int(row.get("B17001_029E")) or 0) + (_safe_int(row.get("B17001_030E")) or 0)
            )
            seniors_65_by_dir[d] += tract_seniors_65
            seniors_75_by_dir[d] += tract_seniors_75
            seniors_alone_by_dir[d] += tract_seniors_alone
            seniors_poverty_by_dir[d] += tract_seniors_poverty

            # Housing populations by direction
            tract_renter_hh = _safe_int(row.get("B25070_001E")) or 0
            tract_cost_burdened = (
                (_safe_int(row.get("B25070_007E")) or 0) + (_safe_int(row.get("B25070_008E")) or 0)
                + (_safe_int(row.get("B25070_009E")) or 0) + (_safe_int(row.get("B25070_010E")) or 0)
            )
            renter_hh_by_dir[d] += tract_renter_hh
            cost_burdened_by_dir[d] += tract_cost_burdened

        # Extract county name from "Census Tract N, County Name, State"
        name = row.get("NAME", "")
        if "," in name:
            parts = [p.strip() for p in name.split(",")]
            if len(parts) >= 2:
                county_names.add(parts[1])

    median_income = int(a["weighted_income_sum"] / a["income_pop_sum"]) if a["income_pop_sum"] > 0 else None
    income_cv = a["income_cv_numerator"] / a["income_cv_denominator"] if a["income_cv_denominator"] > 0 else None
    median_family_income = int(a["family_income_sum"] / a["family_income_pop_sum"]) if a["family_income_pop_sum"] > 0 else None
    area_name = ", ".join(sorted(county_names)) if county_names else "selected area"

    return {
        "total_population": a["total_pop"] or None,
        "population_under_18": a["pop_under_18"] or None,
        "population_under_5": int(round(a["pop_under_5"])),
        "population_5_to_11": int(round(a["pop_5_11"])),
        "population_12_to_17": int(round(a["pop_12_17"])),
        "school_age_population": int(round(a["pop_5_11"] + a["pop_12_17"])),
        "gravity_weighted_school_age_pop": int(round(a["gravity_weighted_school_age_pop"])),
        "median_household_income": median_income,
        "median_family_income": median_family_income,
        "income_moe_pct": income_cv,
        "total_households": int(round(a["total_households"])) if a["total_households"] > 0 else None,
        "families_with_children": int(round(a["families_with_children"])) if a["families_with_children"] > 0 else None,
        "owner_occupied_units": a["owner_occupied"] or None,
        "total_housing_units": a["total_housing"] or None,
        "renter_households": int(round(a["renter_households"])) if a["renter_households"] > 0 else None,
        "cost_burdened_renter_households": int(round(a["cost_burdened_renter_households"])) if a["cost_burdened_renter_households"] > 0 else None,
        "renter_occupied_units": int(round(a["renter_occupied_units"])) if a["renter_occupied_units"] > 0 else None,
        "vacant_housing_units": int(round(a["vacant_housing_units"])) if a["vacant_housing_units"] > 0 else None,
        "high_income_households": int(round(a["high_income"])),
        "seniors_65_plus": int(round(a["seniors_65_plus"])) if a["seniors_65_plus"] > 0 else None,
        "seniors_75_plus": int(round(a["seniors_75_plus"])) if a["seniors_75_plus"] > 0 else None,
        "seniors_living_alone": int(round(a["seniors_living_alone"])) if a["seniors_living_alone"] > 0 else None,
        "seniors_below_200pct_poverty": int(round(a["seniors_below_200pct_poverty"])) if a["seniors_below_200pct_poverty"] > 0 else None,
        "male_5_9": int(round(a["male_5_9"])),
        "male_10_14": int(round(a["male_10_14"])),
        "male_15_17": int(round(a["male_15_17"])),
        "female_5_9": int(round(a["female_5_9"])),
        "female_10_14": int(round(a["female_10_14"])),
        "female_15_17": int(round(a["female_15_17"])),
        "private_school_enrolled": int(round(a["private_school_enrolled"])),
        "total_school_enrolled": int(round(a["total_school_enrolled"])),
        "income_distribution": [
            (midpoint, int(round(a["income_brackets"][var])))
            for var, midpoint in _B19001_BRACKET_VARS
        ],
        "county_name": area_name,
        "tract_count": len(rows),
        "data_geography": "radius",
        "gravity_weighted": bool(tract_centroids),
        "population_by_direction": pop_by_dir if use_directional else None,
        "direction_details": _build_direction_details(
            pop_by_dir, income_qualified_by_dir,
            pipeline_under5_by_dir, pipeline_school_age_by_dir,
        ) if use_directional else None,
        # Elder care directional data
        "seniors_by_direction": {
            d: {
                "seniors_65_plus": int(round(seniors_65_by_dir[d])),
                "seniors_75_plus": int(round(seniors_75_by_dir[d])),
                "seniors_living_alone": int(round(seniors_alone_by_dir[d])),
                "seniors_below_poverty": int(round(seniors_poverty_by_dir[d])),
                "isolation_ratio": round(seniors_alone_by_dir[d] / seniors_65_by_dir[d], 3)
                    if seniors_65_by_dir[d] > 0 else None,
            }
            for d in _dirs
        } if use_directional else None,
        # Housing directional data
        "housing_by_direction": {
            d: {
                "cost_burdened_renters": int(round(cost_burdened_by_dir[d])),
                "renter_households": int(round(renter_hh_by_dir[d])),
                "burden_ratio": round(cost_burdened_by_dir[d] / renter_hh_by_dir[d], 3)
                    if renter_hh_by_dir[d] > 0 else None,
            }
            for d in _dirs
        } if use_directional else None,
    }


async def get_demographics(
    # REVIEW[CACHE]: Demographics calls are live per request in non-DB mode; there is no request-level cache for identical tract/county queries.
    lat: float,
    lon: float,
    county_fips: str,
    state_fips: str,
    radius_miles: float = 10.0,
    isochrone_polygon: Optional[dict] = None,
) -> dict:
    """
    Fetch ACS 5-year demographic data for census tracts within the catchment area.

    When isochrone_polygon is provided (a GeoJSON Polygon/MultiPolygon from ORS),
    census tracts are selected by point-in-polygon test against the drive-time
    isochrone boundary using shapely. This produces a more human-realistic
    catchment than a simple radius.

    Falls back to radius-based tract selection if no polygon is supplied, and to
    county-level data if no tract-level data can be obtained.

    Also fetches 2017 ACS county-level data concurrently for trend comparison.
    """
    cache_key = _demographics_cache_key(lat, lon, county_fips, state_fips, radius_miles, isochrone_polygon)
    cached = _demographics_cache_get(cache_key)
    if cached is not None:
        return cached

    # Choose the tract-finding strategy based on whether we have an isochrone
    if isochrone_polygon:
        tract_task = _get_tracts_in_polygon(isochrone_polygon, lat, lon)
    else:
        tract_task = _get_tracts_in_radius(lat, lon, radius_miles)

    # Run tract lookup and trend fetches concurrently
    tracts, trend_2017, trend_2022 = await asyncio.gather(
        tract_task,
        get_county_trend_2017(county_fips, state_fips),
        get_county_trend_2022(county_fips, state_fips),
    )

    if not tracts:
        fallback_msg = (
            "No tracts found within isochrone — falling back to county-level data"
            if isochrone_polygon
            else "No tracts found within radius — falling back to county-level data"
        )
        logger.info(fallback_msg)
        data = await _get_county_demographics(county_fips, state_fips)
        data["historical_2017"] = trend_2017
        data["county_trend_2022"] = trend_2022
        _demographics_cache_set(cache_key, data)
        return data

    # Group qualifying tract GEOIDs by (state, county)
    county_groups: Dict[Tuple[str, str], set] = {}
    for t in tracts:
        key = (t["state"], t["county"])
        county_groups.setdefault(key, set()).add(t["geoid"])

    sem = asyncio.Semaphore(_ACS_COUNTY_CONCURRENCY)

    async def _limited_fetch(state, county, geoids, client):
        async with sem:
            return await _get_acs_for_county_tracts(state, county, geoids, client)

    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        tasks = [
            _limited_fetch(state, county, geoids, client)
            for (state, county), geoids in county_groups.items()
        ]
        results = await asyncio.gather(*tasks)

    all_rows = [row for result in results for row in result]

    if not all_rows:
        logger.info("No ACS tract data returned — falling back to county-level data")
        data = await _get_county_demographics(county_fips, state_fips)
        data["historical_2017"] = trend_2017
        data["county_trend_2022"] = trend_2022
        _demographics_cache_set(cache_key, data)
        return data

    tract_centroids = {
        t["geoid"]: {
            "lat": t["lat"],
            "lon": t["lon"],
            "distance_miles": t["distance_miles"],
        }
        for t in tracts
        if "lat" in t and "lon" in t
    }

    data = _aggregate_tract_rows(
        all_rows,
        tract_centroids=tract_centroids,
        school_lat=lat,
        school_lon=lon,
    )
    data["historical_2017"] = trend_2017
    data["county_trend_2022"] = trend_2022
    _demographics_cache_set(cache_key, data)
    return data


async def _get_county_demographics(county_fips: str, state_fips: str) -> dict:
    """Fallback: fetch ACS demographic data at the county level."""
    if len(county_fips) >= 5:
        state_code = county_fips[:2]
        county_code = county_fips[2:5]
    elif state_fips:
        state_code = state_fips[:2]
        county_code = county_fips
    else:
        return {"error": "Missing FIPS codes"}

    batches = _chunk_variables(ACS_VARIABLES)

    async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
        row: dict = {}
        for batch in batches:
            params = {
                "get": ",".join(batch),
                "for": f"county:{county_code}",
                "in": f"state:{state_code}",
            }
            if CENSUS_API_KEY:
                params["key"] = CENSUS_API_KEY
            try:
                response = await client.get(ACS_BASE_URL, params=params)
                response.raise_for_status()
                data = response.json()
                if len(data) < 2:
                    continue
                headers = data[0]
                values = data[1]
                row.update(dict(zip(headers, values)))
            except Exception as e:
                logger.error("Census API error: %s", e)

        if not row:
            return {}
        try:

            total_households = _safe_int(row.get("B19001_001E")) or _safe_int(row.get("B11001_001E"))

            # $100k+ threshold only (B19001_013E / $75–99k excluded)
            high_income_vars = [
                "B19001_014E",
                "B19001_015E",
                "B19001_016E",
                "B19001_017E",
            ]
            high_income_households = sum(
                _safe_int(row.get(v)) or 0 for v in high_income_vars
            )

            pop_5_11 = _safe_int(row.get("B09001_004E")) or 0
            pop_12_17 = _safe_int(row.get("B09001_005E")) or 0

            median_inc = _safe_int(row.get("B19013_001E"))
            income_moe = _safe_int(row.get("B19013_001M"))
            income_cv = None
            if median_inc and median_inc > 0 and income_moe and income_moe > 0:
                income_cv = income_moe / (1.645 * median_inc)

            median_family_inc = _safe_int(row.get("B19125_002E")) or _safe_int(row.get("B19113_001E"))

            return {
                "total_population": _safe_int(row.get("B01003_001E")),
                "population_under_18": _safe_int(row.get("B09001_001E")),
                "population_under_5": _safe_int(row.get("B09001_003E")) or 0,
                "population_5_to_11": pop_5_11,
                "population_12_to_17": pop_12_17,
                "school_age_population": pop_5_11 + pop_12_17,
                "median_household_income": median_inc,
                "median_family_income": median_family_inc if median_family_inc and median_family_inc > 0 else None,
                "income_moe_pct": income_cv,
                "total_households": total_households,
                "families_with_children": _safe_int(row.get("B11003_001E")),
                "owner_occupied_units": _safe_int(row.get("B25003_002E")),
                "total_housing_units": _safe_int(row.get("B25003_001E")),
                "renter_households": _safe_int(row.get("B25070_001E")),
                "cost_burdened_renter_households": (
                    (_safe_int(row.get("B25070_007E")) or 0)
                    + (_safe_int(row.get("B25070_008E")) or 0)
                    + (_safe_int(row.get("B25070_009E")) or 0)
                    + (_safe_int(row.get("B25070_010E")) or 0)
                ),
                "renter_occupied_units": _safe_int(row.get("B25003_003E")),
                "vacant_housing_units": _safe_int(row.get("B25002_003E")),
                "high_income_households": high_income_households,
                "male_5_9": _safe_int(row.get("B01001_004E")) or 0,
                "male_10_14": _safe_int(row.get("B01001_005E")) or 0,
                "male_15_17": _safe_int(row.get("B01001_006E")) or 0,
                "female_5_9": _safe_int(row.get("B01001_028E")) or 0,
                "female_10_14": _safe_int(row.get("B01001_029E")) or 0,
                "female_15_17": _safe_int(row.get("B01001_030E")) or 0,
                "private_school_enrolled": sum(
                    (_safe_int(row.get(v)) or 0) for v in _B14002_PRIVATE_K12
                ),
                "total_school_enrolled": (
                    (_safe_int(row.get("B14002_003E")) or 0)
                    + (_safe_int(row.get("B14002_027E")) or 0)
                ),
                "seniors_65_plus": (
                    (_safe_int(row.get("B01001_020E")) or 0)
                    + (_safe_int(row.get("B01001_021E")) or 0)
                    + (_safe_int(row.get("B01001_022E")) or 0)
                    + (_safe_int(row.get("B01001_023E")) or 0)
                    + (_safe_int(row.get("B01001_024E")) or 0)
                    + (_safe_int(row.get("B01001_025E")) or 0)
                    + (_safe_int(row.get("B01001_044E")) or 0)
                    + (_safe_int(row.get("B01001_045E")) or 0)
                    + (_safe_int(row.get("B01001_046E")) or 0)
                    + (_safe_int(row.get("B01001_047E")) or 0)
                    + (_safe_int(row.get("B01001_048E")) or 0)
                    + (_safe_int(row.get("B01001_049E")) or 0)
                ) or None,
                "seniors_75_plus": (
                    (_safe_int(row.get("B01001_023E")) or 0)
                    + (_safe_int(row.get("B01001_024E")) or 0)
                    + (_safe_int(row.get("B01001_025E")) or 0)
                    + (_safe_int(row.get("B01001_047E")) or 0)
                    + (_safe_int(row.get("B01001_048E")) or 0)
                    + (_safe_int(row.get("B01001_049E")) or 0)
                ) or None,
                "seniors_living_alone": (
                    (_safe_int(row.get("B11010_003E")) or 0)
                    + (_safe_int(row.get("B11010_006E")) or 0)
                ) or None,
                "seniors_below_200pct_poverty": (
                    (_safe_int(row.get("B17001_015E")) or 0)
                    + (_safe_int(row.get("B17001_016E")) or 0)
                    + (_safe_int(row.get("B17001_029E")) or 0)
                    + (_safe_int(row.get("B17001_030E")) or 0)
                ) or None,
                "income_distribution": [
                    (midpoint, _safe_int(row.get(var)) or 0)
                    for var, midpoint in _B19001_BRACKET_VARS
                ],
                "county_name": row.get("NAME", "Unknown County"),
                "data_geography": "county",
            }

        except httpx.TimeoutException:
            logger.error("Census API timeout")
            return {}
        except Exception as e:
            logger.error("Census API error: %s", e)
            return {}


# Minimal ACS 2017 variables used only for 5-year trend comparison
_ACS_2017_TREND_VARS = [
    "B01003_001E",  # Total population
    "B09001_004E",  # Population 5-11
    "B09001_005E",  # Population 12-17
    "B19013_001E",  # Median household income (last-resort fallback)
    "B19125_002E",  # Median family income, families with children under 18 (preferred)
    "B19113_001E",  # Median family income, all families (fallback when B19125 suppressed)
    "B11003_001E",  # Family households with children
]

# CPI-U change 2017→2022 (Census income dollars are adjusted to the survey end
# year, so 2017 ACS reports in 2017 dollars and 2022 ACS in 2022 dollars).
# Approximate US CPI-U increase 2017→2022: ~19%
INFLATION_ADJ_2017_TO_2022 = 0.19


async def get_county_trend_2017(county_fips: str, state_fips: str) -> dict:
    """
    Fetch 2017 ACS 5-year county-level data for the key trend variables.
    County-level comparison is used (rather than tract) because tract boundaries
    can differ between the 2010 and 2020 census definitions.
    Returns a minimal dict suitable for compute_trend().
    """
    return await _get_county_trend_data(county_fips, state_fips, ACS_2017_BASE_URL, "2017")


async def get_county_trend_2022(county_fips: str, state_fips: str) -> dict:
    """
    Fetch 2022 ACS 5-year county-level data for the same trend variables.
    Used to ensure trend comparison is county-to-county (apples to apples),
    not catchment-area-tracts vs full-county.
    """
    return await _get_county_trend_data(county_fips, state_fips, ACS_BASE_URL, "2022")


async def _get_county_trend_data(
    county_fips: str, state_fips: str, base_url: str, label: str
) -> dict:
    """Shared implementation for fetching county-level trend variables."""
    if len(county_fips) >= 5:
        state_code = county_fips[:2]
        county_code = county_fips[2:5]
    elif state_fips:
        state_code = state_fips[:2]
        county_code = county_fips
    else:
        return {}

    variables = ",".join(_ACS_2017_TREND_VARS)
    params = {
        "get": variables,
        "for": f"county:{county_code}",
        "in": f"state:{state_code}",
    }
    if CENSUS_API_KEY:
        params["key"] = CENSUS_API_KEY

    async with httpx.AsyncClient(timeout=_TREND_TIMEOUT) as client:
        try:
            response = await client.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()

            if len(data) < 2:
                return {}

            headers = data[0]
            values = data[1]
            row = dict(zip(headers, values))

            pop_5_11 = _safe_int(row.get("B09001_004E")) or 0
            pop_12_17 = _safe_int(row.get("B09001_005E")) or 0
            # Prefer B19125_002E (families with children) → B19113 (all families) → B19013 (HH)
            family_inc = (
                _safe_int(row.get("B19125_002E"))
                or _safe_int(row.get("B19113_001E"))
                or _safe_int(row.get("B19013_001E"))
            )
            return {
                "school_age_pop": pop_5_11 + pop_12_17,
                "median_income": family_inc if (family_inc and family_inc > 0) else None,
                "families_with_children": _safe_int(row.get("B11003_001E")),
            }

        except Exception as e:
            logger.warning("%s ACS trend fetch error: %s", label, e)
            return {}


def compute_trend(data_2022: dict, data_2017: dict) -> dict:
    """
    Compute 5-year demographic trend between ACS 2017 and 2022 county-level data.
    Returns a dict with pct-change fields and a summary trend_label.

    IMPORTANT: Both 2017 and 2022 data must be at the same geographic level
    (county) to avoid comparing catchment-area tract sums against full-county
    totals.  When county_trend_2022 is present in data_2022, we use that for
    the comparison instead of the (potentially tract-aggregated) fields.

    school_age_pop_pct: raw % change in school-age (5-17) population
    income_real_pct:    inflation-adjusted % change in median household income
    families_pct:       raw % change in family households with children
    trend_label:        "Growing" | "Stable" | "Declining" | "Mixed"
    """
    if not data_2017:
        return {}

    # Use county-level 2022 values when available (ensures county-to-county
    # comparison even when the main demographics dict is tract-aggregated).
    county_2022 = data_2022.get("county_trend_2022") or {}
    if county_2022:
        pop_2022 = county_2022.get("school_age_pop") or 0
        income_2022 = county_2022.get("median_income")
        fam_2022 = county_2022.get("families_with_children")
    else:
        # Fallback: data_2022 is already county-level (no tract data was available)
        pop_2022 = (
            (data_2022.get("population_5_to_11") or 0)
            + (data_2022.get("population_12_to_17") or 0)
        )
        income_2022 = data_2022.get("median_family_income") or data_2022.get("median_household_income")
        fam_2022 = data_2022.get("families_with_children")

    pop_2017 = data_2017.get("school_age_pop")
    school_age_pct = None
    if pop_2022 and pop_2017 and pop_2017 > 0:
        school_age_pct = round((pop_2022 - pop_2017) / pop_2017 * 100, 1)

    # Median income — real (inflation-adjusted) change.
    # income_2022 already set above (county-level when available).
    income_2017 = data_2017.get("median_income")
    income_real_pct = None
    if income_2022 and income_2017 and income_2017 > 0:
        nominal_change = (income_2022 - income_2017) / income_2017
        income_real_pct = round((nominal_change - INFLATION_ADJ_2017_TO_2022) * 100, 1)

    # Family household change
    # fam_2022 already set above (county-level when available).
    fam_2017 = data_2017.get("families_with_children")
    families_pct = None
    if fam_2022 and fam_2017 and fam_2017 > 0:
        families_pct = round((fam_2022 - fam_2017) / fam_2017 * 100, 1)

    # Trend label: primary signal is school-age population; income confirms or qualifies
    def _signal(val, pos_thresh=3.0, neg_thresh=-3.0):
        if val is None:
            return "unknown"
        return "up" if val > pos_thresh else "down" if val < neg_thresh else "flat"

    pop_sig = _signal(school_age_pct)
    inc_sig = _signal(income_real_pct, pos_thresh=1.0, neg_thresh=-1.0)
    fam_sig = _signal(families_pct)

    if pop_sig == "up" and inc_sig in ("up", "flat") and fam_sig != "down":
        trend_label = "Growing"
    elif pop_sig == "down" and inc_sig == "down":
        trend_label = "Declining"
    elif pop_sig == "down" and inc_sig == "up":
        trend_label = "Mixed"  # shrinking households but rising incomes (gentrifying)
    elif pop_sig == "up" and inc_sig == "down":
        trend_label = "Mixed"  # growing families but affordability squeeze
    elif pop_sig == "down" and fam_sig == "down":
        trend_label = "Declining"
    else:
        trend_label = "Stable"

    return {
        "school_age_pop_pct": school_age_pct,
        "income_real_pct": income_real_pct,
        "families_pct": families_pct,
        "trend_label": trend_label,
        "period": "ACS 2017 → 2022 (county-level)",
    }
