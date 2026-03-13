import io
import logging
import os
import platform
import re
import asyncio
from pathlib import Path

import httpx
import pandas as pd

from utils import haversine_miles

logger = logging.getLogger(__name__)


def _default_data_dir() -> Path:
    """Return a writable cross-platform cache directory for CMS data."""
    custom_dir = os.getenv("CMS_DATA_DIR")
    if custom_dir:
        return Path(custom_dir).expanduser()
    system = platform.system()
    home = Path.home()
    if system == "Darwin":
        return home / "Library" / "Caches" / "academy-feasibility" / "backend"
    xdg = os.getenv("XDG_CACHE_HOME")
    if xdg:
        return Path(xdg).expanduser() / "academy-feasibility" / "backend"
    return home / ".cache" / "academy-feasibility" / "backend"


_DATA_DIR = _default_data_dir()
DATA_FILE = _DATA_DIR / "cms_care_compare_facilities.csv"
PROVIDER_INFO_FILE = _DATA_DIR / "cms_provider_info_4pq5_n9py.csv"
PBJ_DAILY_STAFFING_FILE = _DATA_DIR / "cms_pbj_daily_staffing.csv"
MDS_QUALITY_FILE = _DATA_DIR / "cms_mds_quality_measures.csv"
AL_FACILITIES_FILE = _DATA_DIR / "onefact_assisted_living_facilities.csv"
_OVERPASS_TIMEOUT = 15.0  # Overpass API timeout (seconds)
_ELDER_CARE_DEBUG = os.getenv("ELDER_CARE_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}

# CMS Provider Data Catalog — Nursing Home Provider Information (dataset 4pq5-n9py)
# The old Socrata CSV endpoint was retired (HTTP 410 Gone).  Use the Provider Data
# API instead: first try the bulk CSV download, then fall back to the paginated JSON
# datastore endpoint.
_CMS_CSV_DOWNLOAD_URL = (
    "https://data.cms.gov/provider-data/api/1/datastore/query/4pq5-n9py/0/download?format=csv"
)
_CMS_JSON_API_URL = (
    "https://data.cms.gov/provider-data/api/1/datastore/query/4pq5-n9py/0"
)
_CMS_JSON_PAGE_SIZE = 5000  # max rows per request allowed by CMS API
_CMS_DOWNLOAD_TIMEOUT = 90.0
_CMS_MIN_FILE_SIZE = 100_000  # ~100 KB; full file is several MB
_cms_download_lock = asyncio.Lock()

# onefact/assisted-living — national AL facility dataset (~44k facilities, state-licensed)
# Source: https://github.com/onefact/assisted-living
_AL_CSV_URL = "https://github.com/onefact/assisted-living/raw/main/assisted-living-facilities.csv"
_AL_MIN_FILE_SIZE = 1_000_000  # ~1 MB; full file is ~13 MB
_al_download_lock = asyncio.Lock()


CARE_LEVEL_ALIASES = {
    "all": {"all"},
    "snf": {"snf", "skilled_nursing", "nursing_home"},
    "assisted_living": {"assisted_living"},
    "memory_care": {"memory_care"},
}


def _fallback_rows() -> list[dict]:
    """Return empty list when no CMS data is available.

    The caller (get_nearby_elder_care_facilities) will fall through to the
    Overpass API when cms_had_results is False, which provides real location-
    aware facility discovery via OpenStreetMap data.
    """
    logger.warning(
        "All CMS/AL data sources unavailable; returning empty facility list. "
        "Set CMS_DATA_DIR or ensure network access so provider data can be downloaded."
    )
    return []


def _cms_cache_is_valid() -> bool:
    """Return True if the cached CMS provider file exists and looks complete."""
    if not PROVIDER_INFO_FILE.exists() or PROVIDER_INFO_FILE.stat().st_size < _CMS_MIN_FILE_SIZE:
        return False
    try:
        headers = pd.read_csv(PROVIDER_INFO_FILE, nrows=0).columns
    except (OSError, pd.errors.ParserError, ValueError, KeyError):
        return False
    provider_name = _pick_column(pd.DataFrame(columns=headers), ["provider_name", "facility_name", "name", "provname"])
    provider_beds = _pick_column(
        pd.DataFrame(columns=headers),
        [
            "number_of_certified_beds",
            "number_of_certified_beds_in_facility",
            "certified_beds",
            "number_of_beds",
            "licensed_beds",
            "beds",
            "bedcert",
        ],
    )
    return bool(provider_name and provider_beds)


def _cms_pbj_cache_is_valid() -> bool:
    """Return True if the cached CMS PBJ file exists and looks complete."""
    if not PBJ_DAILY_STAFFING_FILE.exists() or PBJ_DAILY_STAFFING_FILE.stat().st_size < _CMS_MIN_FILE_SIZE:
        return False
    try:
        headers = pd.read_csv(PBJ_DAILY_STAFFING_FILE, nrows=0).columns
    except (OSError, pd.errors.ParserError, ValueError, KeyError):
        return False
    pbj_ccn = _pick_column(pd.DataFrame(columns=headers), ["ccn", "provider_ccn", "federal_provider_number", "provider_id", "provnum"])
    pbj_census = _pick_column(pd.DataFrame(columns=headers), ["mdscensus", "residents_daily", "avg_daily_census", "resident_census"])
    return bool(pbj_ccn and pbj_census)


async def _download_cms_provider_data() -> bool:
    """Download CMS Nursing Home Provider Information from the Provider Data API."""
    async with _cms_download_lock:
        if _cms_cache_is_valid():
            return True
        _DATA_DIR.mkdir(parents=True, exist_ok=True)

        # Strategy 1: bulk CSV download (fastest)
        if await _try_csv_download():
            return True

        # Strategy 2: paginated JSON API (more reliable)
        if await _try_json_api_download():
            return True

        return False


async def _download_cms_pbj_data() -> bool:
    """PBJ daily staffing download — currently disabled.

    The Provider Information dataset (4pq5-n9py) already includes
    'Average Number of Residents per Day' (MDS daily census) which,
    combined with 'Number of Certified Beds', gives us occupancy without
    downloading the massive (~1.3M row) PBJ daily staffing file.

    The old PBJ dataset ID 'xubh-q36u' was incorrect (Hospital General
    Information).  The correct v1 endpoint works but is too large for
    practical paginated download (~270 pages at 5000 rows/page).

    Returning False causes _load_provider_facilities() to use the
    provider-info-based occupancy fallback, which is fast and accurate.
    """
    return False


def _al_cache_is_valid() -> bool:
    """Return True if the cached onefact AL facility file exists and looks complete."""
    if not AL_FACILITIES_FILE.exists() or AL_FACILITIES_FILE.stat().st_size < _AL_MIN_FILE_SIZE:
        return False
    try:
        headers = pd.read_csv(AL_FACILITIES_FILE, nrows=0).columns
    except (OSError, pd.errors.ParserError, ValueError):
        return False
    norm = {c.strip().lower() for c in headers}
    return {"facility name", "latitude", "longitude"}.issubset(norm)


async def _download_al_data() -> bool:
    """Download the onefact national assisted-living facility dataset."""
    async with _al_download_lock:
        if _al_cache_is_valid():
            return True
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        logger.info("Downloading onefact AL facility data from %s", _AL_CSV_URL)
        try:
            async with httpx.AsyncClient(timeout=_CMS_DOWNLOAD_TIMEOUT) as client:
                response = await client.get(_AL_CSV_URL, follow_redirects=True)
                response.raise_for_status()
            content = response.content
            if len(content) < _AL_MIN_FILE_SIZE:
                logger.warning("AL download too small (%d bytes); skipping", len(content))
                return False
            AL_FACILITIES_FILE.write_bytes(content)
            logger.info("AL facility data saved to %s (%d KB)", AL_FACILITIES_FILE, len(content) // 1024)
            return True
        except (httpx.RequestError, httpx.HTTPStatusError, OSError, pd.errors.ParserError) as e:
            logger.warning("AL facility download failed: %s: %s", type(e).__name__, e)
            return False


async def _try_csv_download() -> bool:
    """Attempt to download the full CMS dataset as a CSV file."""
    logger.info("Downloading CMS provider data (CSV) from %s", _CMS_CSV_DOWNLOAD_URL)
    try:
        async with httpx.AsyncClient(timeout=_CMS_DOWNLOAD_TIMEOUT) as client:
            response = await client.get(_CMS_CSV_DOWNLOAD_URL, follow_redirects=True)
            response.raise_for_status()
        content = response.content
        if len(content) < _CMS_MIN_FILE_SIZE:
            logger.warning("CMS CSV download too small (%d bytes); skipping", len(content))
            return False
        df = pd.read_csv(io.BytesIO(content))
        if df.empty:
            logger.warning("CMS CSV download parsed to empty DataFrame; skipping")
            return False
        PROVIDER_INFO_FILE.write_bytes(content)
        logger.info(
            "CMS provider data saved to %s (%d KB, %d rows)",
            PROVIDER_INFO_FILE,
            len(content) // 1024,
            len(df),
        )
        return True
    except httpx.HTTPStatusError as e:
        logger.warning("CMS CSV download HTTP error: %s — %s", e.response.status_code, e.request.url)
        return False
    except (httpx.RequestError, OSError, pd.errors.ParserError) as e:
        logger.warning("CMS CSV download failed: %s: %s", type(e).__name__, e)
        return False


async def _try_json_api_download() -> bool:
    """Fetch all rows via the CMS datastore JSON API (paginated) and save as CSV."""
    logger.info("Downloading CMS provider data (JSON API) from %s", _CMS_JSON_API_URL)
    all_rows: list[dict] = []
    offset = 0
    try:
        async with httpx.AsyncClient(timeout=_CMS_DOWNLOAD_TIMEOUT) as client:
            while True:
                url = f"{_CMS_JSON_API_URL}?limit={_CMS_JSON_PAGE_SIZE}&offset={offset}"
                response = await client.get(url, follow_redirects=True)
                response.raise_for_status()
                payload = response.json()
                results = payload.get("results", [])
                if not results:
                    break
                all_rows.extend(results)
                if len(results) < _CMS_JSON_PAGE_SIZE:
                    break
                offset += len(results)

        if not all_rows:
            logger.warning("CMS JSON API returned zero rows")
            return False

        df = pd.DataFrame(all_rows)
        if df.empty:
            logger.warning("CMS JSON API produced empty DataFrame; skipping")
            return False
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        if len(csv_bytes) < _CMS_MIN_FILE_SIZE:
            logger.warning("CMS JSON API data too small (%d bytes); skipping", len(csv_bytes))
            return False
        PROVIDER_INFO_FILE.write_bytes(csv_bytes)
        logger.info(
            "CMS provider data saved to %s (%d KB, %d rows) via JSON API",
            PROVIDER_INFO_FILE,
            len(csv_bytes) // 1024,
            len(df),
        )
        return True
    except httpx.HTTPStatusError as e:
        logger.warning("CMS JSON API HTTP error: %s — %s", e.response.status_code, e.request.url)
        return False


def _load_al_facilities() -> list[dict]:
    """Load assisted living facilities from the onefact national AL dataset."""
    if not AL_FACILITIES_FILE.exists():
        return []
    try:
        df = pd.read_csv(AL_FACILITIES_FILE, low_memory=False)
    except (OSError, pd.errors.ParserError, ValueError):
        logger.exception("Failed to read onefact AL dataset from %s", AL_FACILITIES_FILE)
        return []

    # Normalize column names for flexible matching
    df.columns = [c.strip() for c in df.columns]
    name_col = _pick_column(df, ["Facility Name", "facility_name", "name"])
    lat_col = _pick_column(df, ["Latitude", "latitude", "lat"])
    lon_col = _pick_column(df, ["Longitude", "longitude", "lon"])
    city_col = _pick_column(df, ["City", "city"])
    capacity_col = _pick_column(df, ["Capacity", "capacity", "licensed_beds", "beds"])
    ownership_col = _pick_column(df, ["Ownership Type", "ownership_type", "ownership"])
    licensee_col = _pick_column(df, ["Licensee", "licensee", "owner_name"])

    if not name_col or not lat_col or not lon_col:
        logger.warning("onefact AL dataset missing required columns; skipping")
        return []

    facilities: list[dict] = []
    for _, row in df.iterrows():
        try:
            lat = float(row[lat_col])
            lon = float(row[lon_col])
        except (TypeError, ValueError):
            continue
        name = str(row[name_col]).strip() or "Assisted Living Facility"
        capacity = row[capacity_col] if capacity_col else None
        facilities.append({
            "name": name,
            "lat": lat,
            "lon": lon,
            "city": str(row[city_col]).strip() if city_col else None,
            "care_level": "assisted_living",
            "certified_beds": capacity,
            "licensed_beds": capacity,
            "owner_name": str(row[licensee_col]).strip() if licensee_col else None,
            "ownership": str(row[ownership_col]).strip() if ownership_col else "State Licensed",
            "certification": None,
            "ccn": None,
            "mds_overall_rating": None,
            "occupancy_pct": None,
        })

    logger.info("onefact AL facility load: rows=%d emitted=%d", len(df), len(facilities))
    return facilities


def _load_facilities() -> list[dict]:
    """Load elder-care facilities from provider data, legacy CSV, or fallback rows."""
    provider_rows = _load_provider_facilities()
    al_rows = _load_al_facilities()

    if provider_rows or al_rows:
        return provider_rows + al_rows

    if not DATA_FILE.exists():
        return _fallback_rows()
    try:
        df = pd.read_csv(DATA_FILE)
        expected = {"name", "lat", "lon"}
        if not expected.issubset(df.columns):
            return _fallback_rows()
        return df.to_dict(orient="records")
    except (OSError, pd.errors.ParserError, ValueError, KeyError):
        return _fallback_rows()


def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first column name from *candidates* that exists in *df* (case/space-insensitive)."""
    def _norm(s: str) -> str:
        return s.strip().lower().replace(" ", "_").replace("-", "_")

    normalized = {_norm(c): c for c in df.columns}
    for candidate in candidates:
        match = normalized.get(_norm(candidate))
        if match:
            return match
    return None


def _normalize_ccn(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float):
        if pd.isna(value):
            return None
        if value.is_integer():
            value = int(value)
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    # CSV parsing can coerce CCNs into floats and stringify them as "12345.0".
    # Treat those as integer identifiers so the normalized key stays 6 digits.
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".", 1)[0]
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return text.upper()
    return digits.zfill(6)


def _normalize_care_level(value: object) -> str:
    label = str(value or "").strip().lower()
    if "assist" in label:
        return "assisted_living"
    if "memory" in label or "alzheimer" in label or "dementia" in label:
        return "memory_care"
    if "nurs" in label or "snf" in label or "skilled" in label:
        return "skilled_nursing"
    return "all"


# ---------------------------------------------------------------------------
# Name-based exclusion for non-long-term-care facilities
# ---------------------------------------------------------------------------
# The CMS 4pq5-n9py dataset includes all Medicare/Medicaid-certified nursing
# facilities, which encompasses short-term rehab centers, hospital-based units,
# psychiatric facilities, and pediatric facilities.  These are not relevant
# competitors for long-term elder care and inflate bed counts / saturation.
#
# A facility is EXCLUDED if its name matches a rehab/hospital keyword UNLESS
# it also contains a long-term-care signal (e.g., "Rehabilitation & Nursing").
# ---------------------------------------------------------------------------

_EXCLUDE_FACILITY_KEYWORDS: list[str] = [
    "acute care",
    "post-acute",
    "transitional care",
    "outpatient",
    "hospice",
    "behavioral health",
    "psychiatric",
    "mental health",
    "pediatric",
    "children",
    "veterans home",
    "veterans affairs",
    "va medical",
]

# Rehab-only keywords — excluded UNLESS the name also contains a long-term-care signal
_REHAB_KEYWORDS: list[str] = [
    "rehabilitation",
    "rehab",
    "recovery center",
    "recovery suites",
]

_LONG_TERM_CARE_SIGNALS: list[str] = [
    "nursing",
    "care center",
    "health center",
    "healthcare center",
    "living",
    "senior",
    "convalescent",
]

# Explicit chain/facility names that are primarily rehab/hospital, not long-term care
_EXCLUDE_FACILITY_NAMES: list[str] = [
    "select specialty",       # Select Specialty Hospital — long-term acute care
    "encompass health",       # inpatient rehab facilities
    "kindred hospital",       # Kindred hospitals (not nursing homes)
]


def _is_excluded_facility(name: str) -> bool:
    """Return True if the facility name signals non-long-term-care (rehab, hospital, etc.)."""
    name_lower = name.lower()

    # Check explicit chain/facility names first
    for excluded in _EXCLUDE_FACILITY_NAMES:
        if excluded in name_lower:
            return True

    # Check hard-exclude keywords (no exceptions)
    for kw in _EXCLUDE_FACILITY_KEYWORDS:
        if kw in name_lower:
            return True

    # Check rehab keywords — only exclude if no long-term-care signal is present
    for kw in _REHAB_KEYWORDS:
        if kw in name_lower:
            has_ltc_signal = any(sig in name_lower for sig in _LONG_TERM_CARE_SIGNALS)
            if not has_ltc_signal:
                return True

    return False


def _parse_star_rating(value: object) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "not available"}:
        return None
    try:
        rating = int(float(text))
    except (TypeError, ValueError):
        return None
    if 1 <= rating <= 5:
        return rating
    return None


def _extract_occupancy_pct(row: pd.Series, occupancy_col: str | None, census_col: str | None, beds: object) -> float | None:
    if occupancy_col:
        raw = row.get(occupancy_col)
        if raw not in (None, ""):
            text = str(raw).strip().replace("%", "")
            try:
                pct = float(text)
                if 0 <= pct <= 100:
                    return round(pct, 1)
            except (TypeError, ValueError):
                pass
    if census_col and beds not in (None, ""):
        try:
            census = float(str(row.get(census_col)).strip())
            bed_count = float(str(beds).strip())
        except (TypeError, ValueError):
            return None
        if bed_count > 0 and census >= 0:
            return round((census / bed_count) * 100, 1)
    return None


def _parse_lat_lon(row: pd.Series, lat_col: str | None, lon_col: str | None, location_col: str | None) -> tuple[float, float] | None:
    if lat_col and lon_col:
        try:
            return float(row[lat_col]), float(row[lon_col])
        except (TypeError, ValueError):
            pass

    if location_col:
        raw = str(row.get(location_col, "")).strip()
        # Socrata locations often look like "(41.88, -87.62)"
        if raw.startswith("(") and raw.endswith(")") and "," in raw:
            left, right = raw[1:-1].split(",", 1)
            try:
                return float(left.strip()), float(right.strip())
            except (TypeError, ValueError):
                return None
    return None


def _load_provider_facilities() -> list[dict]:
    if not PROVIDER_INFO_FILE.exists():
        logger.info("Elder-care provider dataset not found at %s", PROVIDER_INFO_FILE)
        return []

    try:
        providers = pd.read_csv(PROVIDER_INFO_FILE)
    except (OSError, pd.errors.ParserError, ValueError, KeyError):
        logger.exception("Failed to read elder-care provider dataset from %s", PROVIDER_INFO_FILE)
        return []

    provider_ccn = _pick_column(providers, [
        "ccn", "provider_ccn", "federal_provider_number", "provider_id",
        "provnum",  # old Socrata column name
    ])
    provider_name = _pick_column(providers, [
        "provider_name", "facility_name", "name",
        "provname",  # old Socrata column name
    ])
    provider_lat = _pick_column(providers, ["latitude", "lat", "provider_latitude"])
    provider_lon = _pick_column(providers, ["longitude", "lon", "lng", "provider_longitude"])
    provider_location = _pick_column(providers, ["location", "provider_location"])
    provider_city = _pick_column(providers, [
        "city", "provider_city", "provider_city_name",
    ])
    provider_owner_name = _pick_column(providers, [
        "owner_name",
        "ownership_name",
        "operator_name",
        "legal_business_name",
        "organization_name",
        "owner",
        "operator",
    ])
    provider_ownership = _pick_column(providers, ["ownership", "ownership_type"])
    provider_beds = _pick_column(
        providers,
        [
            "number_of_certified_beds",
            "number_of_certified_beds_in_facility",
            "certified_beds",
            "number_of_beds",
            "licensed_beds",
            "beds",
            "bedcert",  # old Socrata column name
        ],
    )
    # "provider_type" in CMS data is the *certification* type (Medicare/Medicaid),
    # NOT the care level.  Pick it separately so it doesn't pollute care_level.
    provider_care_level = _pick_column(providers, ["facility_type", "care_level"])
    provider_certification = _pick_column(providers, ["provider_type", "certification"])
    provider_occupancy_pct = _pick_column(providers, ["occupancy_rate", "occupancy_pct", "occupancy_percentage"])
    provider_resident_census = _pick_column(providers, ["average_number_of_residents_per_day", "average_daily_census", "resident_census"])
    provider_overall_rating = _pick_column(
        providers,
        [
            "overall_rating",
            "overall_5_star_rating",
            "five_star_rating",
            "overall_five_star_rating",
            "overall_5_star",
        ],
    )

    required_provider = [provider_name]
    if any(col is None for col in required_provider):
        logger.warning(
            "Provider dataset missing required columns. columns=%s",
            sorted(str(c) for c in providers.columns.tolist()),
        )
        return []

    logger.debug(
        "Elder-care provider schema detected: ccn=%s name=%s lat=%s lon=%s location=%s "
        "beds=%s care_level=%s certification=%s owner_name=%s ownership=%s  [columns=%s]",
        provider_ccn,
        provider_name,
        provider_lat,
        provider_lon,
        provider_location,
        provider_beds,
        provider_care_level,
        provider_certification,
        provider_owner_name,
        provider_ownership,
        sorted(str(c) for c in providers.columns.tolist()),
    )

    providers = providers.copy()
    if provider_ccn:
        providers["_ccn_norm"] = providers[provider_ccn].map(_normalize_ccn)
    else:
        providers["_ccn_norm"] = None

    # MDS is optional for filtering/enrichment. Keep the join/filter behavior, but do
    # not use MDS as the source of truth for overall 5-star rating.
    if MDS_QUALITY_FILE.exists() and provider_ccn:
        try:
            mds = pd.read_csv(MDS_QUALITY_FILE)
            mds_ccn = _pick_column(mds, ["ccn", "provider_ccn", "federal_provider_number", "provider_id"])
            if mds_ccn:
                mds = mds.copy()
                mds["_ccn_norm"] = mds[mds_ccn].map(_normalize_ccn)
                mds_ccns = set(mds["_ccn_norm"].dropna().tolist())
                filtered = providers[providers["_ccn_norm"].isin(mds_ccns)]
                # only apply the filter if it still returns data
                if not filtered.empty:
                    providers = filtered
                elif _ELDER_CARE_DEBUG:
                    logger.info(
                        "MDS filter produced zero provider matches; keeping provider dataset rows. "
                        "provider_rows=%d mds_rows=%d",
                        len(providers),
                        len(mds),
                    )

        except (OSError, pd.errors.ParserError, ValueError, KeyError):
            logger.exception("Failed to read/apply MDS quality dataset from %s", MDS_QUALITY_FILE)

    pbj_census_by_ccn: dict[str, float] = {}
    pbj_available = False
    if PBJ_DAILY_STAFFING_FILE.exists():
        try:
            pbj = pd.read_csv(PBJ_DAILY_STAFFING_FILE)
            pbj_ccn = _pick_column(pbj, ["ccn", "provider_ccn", "federal_provider_number", "provider_id", "provnum"])
            pbj_census = _pick_column(pbj, ["mdscensus", "residents_daily", "avg_daily_census", "resident_census"])
            if pbj_ccn and pbj_census:
                pbj = pbj.copy()
                pbj["_ccn_norm"] = pbj[pbj_ccn].map(_normalize_ccn)
                pbj["_census_num"] = pd.to_numeric(pbj[pbj_census], errors="coerce")
                pbj = pbj.dropna(subset=["_ccn_norm", "_census_num"])
                grouped = pbj.groupby("_ccn_norm", as_index=False)["_census_num"].mean()
                pbj_census_by_ccn = {str(row["_ccn_norm"]): float(row["_census_num"]) for _, row in grouped.iterrows()}
                pbj_available = True
            else:
                logger.warning(
                    "PBJ dataset missing required columns for occupancy derivation; "
                    "falling back to provider-info census. Removing stale file %s",
                    PBJ_DAILY_STAFFING_FILE,
                )
                # Remove the stale/wrong file so we don't re-read it every time.
                try:
                    PBJ_DAILY_STAFFING_FILE.unlink()
                except OSError:
                    pass
                pbj_available = False
        except (OSError, pd.errors.ParserError, ValueError, KeyError):
            logger.exception("Failed to read PBJ dataset from %s", PBJ_DAILY_STAFFING_FILE)

    facilities: list[dict] = []
    rows_with_coordinates = 0
    rows_with_beds = 0
    rows_with_overall_rating = 0
    rows_with_pbj_occupancy = 0
    for _, row in providers.iterrows():
        coords = _parse_lat_lon(row, provider_lat, provider_lon, provider_location)
        if not coords:
            continue
        rows_with_coordinates += 1
        lat, lon = coords

        bed_value = row[provider_beds] if provider_beds else None
        occupancy_pct = None
        ccn_norm = row["_ccn_norm"]
        if pbj_available and ccn_norm in pbj_census_by_ccn and bed_value not in (None, ""):
            try:
                bed_count = float(str(bed_value).strip())
            except (TypeError, ValueError):
                bed_count = 0
            if bed_count > 0:
                occupancy_pct = min(100.0, round((pbj_census_by_ccn[ccn_norm] / bed_count) * 100, 1))
                rows_with_pbj_occupancy += 1
        elif not pbj_available:
            occupancy_pct = _extract_occupancy_pct(row, provider_occupancy_pct, provider_resident_census, bed_value)
        if bed_value not in (None, "") and str(bed_value).strip().lower() not in {"nan", "none", "null"}:
            rows_with_beds += 1

        name = str(row[provider_name]).strip() or "Elder Care Facility"
        mds_overall_rating = _parse_star_rating(row[provider_overall_rating]) if provider_overall_rating else None
        if mds_overall_rating is not None:
            rows_with_overall_rating += 1
        if _ELDER_CARE_DEBUG and "concordia at rebecca residence" in name.lower():
            logger.info(
                "Concordia debug: provider_ccn_raw=%r provider_ccn_norm=%s mds_overall_rating=%s",
                row.get(provider_ccn) if provider_ccn else None,
                ccn_norm,
                mds_overall_rating,
            )

        facilities.append(
            {
                "name": name,
                "lat": lat,
                "lon": lon,
                "city": str(row[provider_city]).strip() if provider_city else None,
                # Dataset 4pq5-n9py only contains nursing homes, so default to
                # "skilled_nursing" when no explicit care_level column is present.
                "care_level": _normalize_care_level(row[provider_care_level]) if provider_care_level else "skilled_nursing",
                "certified_beds": bed_value,
                "licensed_beds": bed_value,
                "owner_name": str(row[provider_owner_name]).strip() if provider_owner_name else None,
                "ownership": str(row[provider_ownership]).strip() if provider_ownership else "CMS Provider Data",
                "certification": str(row[provider_certification]).strip() if provider_certification else None,
                "ccn": ccn_norm,
                "mds_overall_rating": mds_overall_rating,
                "occupancy_pct": occupancy_pct,
            }
        )

    logger.info(
        "Elder-care provider load: rows=%d coords=%d emitted=%d beds=%d "
        "mds_rated=%d pbj_occupancy=%d",
        len(providers),
        rows_with_coordinates,
        len(facilities),
        rows_with_beds,
        rows_with_overall_rating,
        rows_with_pbj_occupancy,
    )

    if _ELDER_CARE_DEBUG and facilities:
        logger.info("Elder-care provider sample rows: %s", facilities[:3])

    return facilities


def _care_level_matches(requested_level: str, facility_level: str) -> bool:
    """Return True if the facility's care level matches the requested filter."""
    requested = (requested_level or "all").strip().lower()
    facility = (facility_level or "all").strip().lower()
    if requested == "all":
        return True

    aliases = CARE_LEVEL_ALIASES.get(requested, {requested})
    return any(alias in facility for alias in aliases)


async def get_nearby_elder_care_facilities(
    lat: float,
    lon: float,
    radius_miles: float,
    care_level: str = "all",
    min_mds_overall_rating: int | None = None,
) -> list[dict]:
    # Auto-download data sources on first request if not cached
    if not _cms_cache_is_valid():
        await _download_cms_provider_data()
    if not _cms_pbj_cache_is_valid():
        await _download_cms_pbj_data()
    if not _al_cache_is_valid():
        await _download_al_data()

    facilities = []
    cms_had_results = False  # True if CMS returned anything before the rating filter
    for row in _load_facilities():
        try:
            d = haversine_miles(lat, lon, float(row["lat"]), float(row["lon"]))
        except (TypeError, ValueError, KeyError):
            continue
        if d > radius_miles:
            continue
        level = str(row.get("care_level", "all")).lower()
        if not _care_level_matches(care_level, level):
            continue
        cms_had_results = True
        # Exclude rehab centers, hospitals, and other non-long-term-care facilities
        facility_name = str(row.get("name", ""))
        if _is_excluded_facility(facility_name):
            continue
        if min_mds_overall_rating is not None and level != "assisted_living":
            rating = _parse_star_rating(row.get("mds_overall_rating"))
            if rating is None or rating < min_mds_overall_rating:
                continue
        enriched = dict(row)
        enriched["distance_miles"] = round(d, 2)
        facilities.append(enriched)

    # Only fall back to Overpass when CMS genuinely had no facilities for this area.
    # If CMS had results but the rating filter reduced them to zero, skip the fallback
    # so we don't return Overpass rows that lack certified bed counts.
    if not facilities and not cms_had_results:
        facilities = await _query_overpass_elder_care(lat, lon, radius_miles, care_level)

    facilities.sort(key=lambda x: x["distance_miles"])
    return facilities


async def _query_overpass_elder_care(lat: float, lon: float, radius_miles: float, care_level: str = "all") -> list[dict]:
    radius_meters = int(radius_miles * 1609.34)
    query = f"""
    [out:json][timeout:25];
    (
      node[\"amenity\"=\"nursing_home\"](around:{radius_meters},{lat},{lon});
      node[\"healthcare\"=\"nursing_home\"](around:{radius_meters},{lat},{lon});
      node[\"social_facility\"=\"nursing_home\"](around:{radius_meters},{lat},{lon});
      node[\"social_facility\"=\"assisted_living\"](around:{radius_meters},{lat},{lon});
      way[\"amenity\"=\"nursing_home\"](around:{radius_meters},{lat},{lon});
      way[\"healthcare\"=\"nursing_home\"](around:{radius_meters},{lat},{lon});
      way[\"social_facility\"=\"nursing_home\"](around:{radius_meters},{lat},{lon});
      way[\"social_facility\"=\"assisted_living\"](around:{radius_meters},{lat},{lon});
    );
    out center tags;
    """

    try:
        async with httpx.AsyncClient(timeout=_OVERPASS_TIMEOUT) as client:
            resp = await client.post("https://overpass-api.de/api/interpreter", data=query)
        resp.raise_for_status()
        data = resp.json()
    except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as e:
        logger.warning("Overpass elder-care query failed: %s: %s", type(e).__name__, e)
        return []

    facilities: list[dict] = []
    for item in data.get("elements", []):
        tags = item.get("tags", {})
        item_lat = item.get("lat") or item.get("center", {}).get("lat")
        item_lon = item.get("lon") or item.get("center", {}).get("lon")
        if item_lat is None or item_lon is None:
            continue

        level_hint = str(tags.get("social_facility") or tags.get("amenity") or tags.get("healthcare") or "").lower()
        normalized = "assisted_living" if "assisted" in level_hint else "skilled_nursing"
        if not _care_level_matches(care_level, normalized):
            continue

        distance = haversine_miles(lat, lon, float(item_lat), float(item_lon))
        raw_capacity = tags.get("capacity") or tags.get("beds")
        try:
            bed_count = int(raw_capacity) if raw_capacity is not None else None
        except (TypeError, ValueError):
            bed_count = None
        facilities.append(
            {
                "name": tags.get("name") or "Elder Care Facility",
                "lat": float(item_lat),
                "lon": float(item_lon),
                "city": tags.get("addr:city"),
                "care_level": normalized,
                "certified_beds": bed_count,
                "ownership": tags.get("operator") or tags.get("owner") or "N/A",
                "distance_miles": round(distance, 2),
            }
        )

    dedup: dict[tuple[str, float, float], dict] = {}
    for facility in facilities:
        key = (facility["name"], round(facility["lat"], 4), round(facility["lon"], 4))
        if key not in dedup:
            dedup[key] = facility
    return sorted(dedup.values(), key=lambda x: x["distance_miles"])
