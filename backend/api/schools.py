"""
NCES Private School Survey (PSS) data integration.

The PSS is a biennial survey of all private K-12 schools in the US.
It includes school name, location (lat/lon), religious affiliation, and enrollment.
We download the public-use CSV file from NCES on first run and cache it locally.
"""

import asyncio
import io
import logging
import os
import platform
import zipfile
from pathlib import Path
from typing import List, Optional

import httpx
import pandas as pd

from utils import haversine_miles

logger = logging.getLogger(__name__)

def _default_data_dir() -> Path:
    """Return a writable cross-platform cache directory for NCES data."""
    custom_dir = os.getenv("PSS_DATA_DIR")
    if custom_dir:
        return Path(custom_dir).expanduser()

    system = platform.system()
    home = Path.home()
    if system == "Darwin":
        return home / "Library" / "Caches" / "academy-feasibility" / "backend"

    # Linux/Ubuntu default (XDG-compliant when available)
    xdg_cache_home = os.getenv("XDG_CACHE_HOME")
    if xdg_cache_home:
        return Path(xdg_cache_home).expanduser() / "academy-feasibility" / "backend"

    return home / ".cache" / "academy-feasibility" / "backend"


DATA_DIR = _default_data_dir()
PSS_FILE = DATA_DIR / "pss_schools.csv"

# Module-level DataFrame cache — loaded once on first request, reused thereafter.
# Avoids reading a ~15 MB CSV on every analysis call.
_pss_df: Optional[pd.DataFrame] = None
_pss_lock = asyncio.Lock()  # Guards _pss_df and cache-validity checks under concurrent requests.

# NCES PSS 2021-22 public use data (most recent available)
# FIX 1: corrected filename — was "pss2122_pu_data_csv.zip", actual file is "pss2122_pu_csv.zip"
PSS_DOWNLOAD_URL = "https://nces.ed.gov/surveys/pss/zip/pss2122_pu_csv.zip"
_PSS_DOWNLOAD_TIMEOUT = 120.0  # Large ZIP file; allow generous download time

# Minimum acceptable size for a cached CSV (bytes). Guards against re-using
# a corrupt or truncated download from a previous failed attempt.
PSS_MIN_FILE_SIZE = 500_000  # ~500 KB; full file is several MB

# NCES PSS affiliation codes — code 1 = Roman Catholic
PSS_AFFILIATION_NAMES = {
    "1": "Roman Catholic",
    "2": "African Methodist Episcopal",
    "3": "Amish",
    "4": "Assembly of God",
    "5": "Baptist",
    "6": "Brethren",
    "7": "Calvinist",
    "8": "Christian (nondenominational)",
    "9": "Church of Christ",
    "10": "Church of God",
    "11": "Church of God in Christ",
    "12": "Church of the Nazarene",
    "13": "Disciples of Christ",
    "14": "Episcopal",
    "15": "Friends (Quaker)",
    "16": "Greek Orthodox",
    "17": "Islamic",
    "18": "Jewish",
    "19": "Latter Day Saints",
    "20": "Lutheran (Missouri Synod)",
    "21": "Evangelical Lutheran",
    "22": "Mennonite",
    "23": "Methodist",
    "24": "Pentecostal",
    "25": "Presbyterian",
    "26": "Seventh-day Adventist",
    "27": "Unitarian",
    "28": "United Church of Christ",
    "29": "Other religious",
    "30": "Nonsectarian",
    "31": "Conservative Christian",
    "32": "Other Protestant",
    "33": "Christian, unspecified",
}

# PSS P335 codes (Q7A: Is School Coeducational)
# 1=Yes (co-ed), 2=No, all-girls, 3=No, all-boys
P335_NAMES = {
    "1": "Co-ed",
    "2": "All Girls",
    "3": "All Boys",
}

# PSS GRADE2 codes (Level of Instruction, Four Categories)
# 0=K-Terminal, 1=Elementary/Middle, 2=Secondary/High, 3=Combined/Other (includes K-12)
GRADE2_NAMES = {
    "0": "K-Terminal",
    "1": "Elementary/Middle",
    "2": "Secondary/High",
    "3": "Combined/Other",
}

# Which GRADE2 values overlap with each requested grade_level
# (we keep competitors whose grade range overlaps with the school being analyzed)
GRADE_LEVEL_FILTER = {
    "k5":          {"0", "1", "3"},  # K-Terminal + Elementary/Middle + Combined/Other (K-12)
    "k8":          {"0", "1", "3"},  # K-Terminal + Elementary/Middle + Combined/Other (K-12)
    "high_school": {"2", "3"},       # Secondary/High + Combined/Other (K-12)
    "k12":         {"0", "1", "2", "3"},  # All
}

# Which P335 values are relevant competitors for each gender type
# (schools that draw from the same student pool)
# P335: 1=Co-ed, 2=All-girls, 3=All-boys
GENDER_FILTER = {
    "boys":  {"3", "1"},      # All-boys + co-ed
    "girls": {"2", "1"},      # All-girls + co-ed
    "coed":  {"1", "2", "3"}, # All (co-ed competes with everyone)
}

# PSS TYPOLOGY codes (combined affiliation + program emphasis)
# In the 2021-22 PSS, the old POEMPH column was replaced by TYPOLOGY which
# encodes both religious orientation and program emphasis in a single field.
TYPOLOGY_NAMES = {
    "1": "Catholic Parochial",
    "2": "Catholic Diocesan",
    "3": "Catholic Private",
    "4": "Other Religious – Conservative Christian",
    "5": "Other Religious – Affiliated",
    "6": "Other Religious – Unaffiliated",
    "7": "Nonsectarian – Regular",
    "8": "Nonsectarian – Special Emphasis",
    "9": "Nonsectarian – Special Education",
}

# TYPOLOGY values for schools that should be EXCLUDED as competitors
# Special education schools serve fundamentally different populations
TYPOLOGY_EXCLUDE = {"9"}

# TYPOLOGY values that indicate "special emphasis" (Montessori, STEM, arts, etc.)
# These get a reduced tier weight — niche overlap only
TYPOLOGY_SPECIAL_EMPHASIS = {"8"}

# Legacy aliases so existing test imports don't break
POEMPH_NAMES = {"1": "Regular", "2": "Special Emphasis", "3": "Special Education"}
POEMPH_EXCLUDE = {"3"}

# ---------------------------------------------------------------------------
# Name-based exclusion filter
# ---------------------------------------------------------------------------
# The PSS TYPOLOGY field only distinguishes special emphasis/education for
# nonsectarian schools.  Many schools serving behavioral, therapeutic, or
# learning-disability populations get coded as regular.  A name-based
# keyword filter catches schools that the TYPOLOGY code misses.
#
# These keywords, when found in the school name (case-insensitive), indicate
# the school serves a fundamentally different population than a typical
# Catholic school would draw from.
# ---------------------------------------------------------------------------

# Keywords that signal behavioral / therapeutic / residential programs
_EXCLUDE_NAME_KEYWORDS: list[str] = [
    # Behavioral / therapeutic / clinical
    "behavioral",
    "therapeutic",
    "treatment",
    "intervention",
    "crisis",
    "rehabilitation",
    "rehab center",
    # Special needs / disabilities
    "special needs",
    "special education",
    "autism",
    "autistic",
    "learning differences",
    "learning disabilities",
    "learning disabled",
    "emotional support",
    "emotionally disturbed",
    "developmental disabilit",   # catches "disability" and "disabilities"
    "intellectual disabilit",
    # Residential / welfare / social services
    "children's aid",
    "childrens aid",
    "children's home",
    "childrens home",
    "children's village",
    "childrens village",
    "children's shelter",
    "childrens shelter",
    "youth services",
    "youth shelter",
    "group home",
    "residential treatment",
    "foster",
    "orphan",
    # Correctional / alternative
    "juvenile",
    "detention",
    "correctional",
    "adjudicated",
    # Hospital / clinical
    "hospital school",
    "psychiatric",
    "day treatment",
]

# Explicit school names to exclude — these schools exclusively serve
# specialized populations but have generic-sounding names that keyword
# matching alone cannot catch.  Matched case-insensitively against the
# full school name (substring match).
_EXCLUDE_SCHOOL_NAMES: list[str] = [
    "aim academy",              # language-based learning differences (dyslexia)
    "benchmark school",         # language-based learning differences
    "hill top preparatory",     # learning differences
    "vanguard school",          # learning differences / neurological
    "pathway school",           # learning / emotional challenges
    "daniel morris school",     # special education
    "green tree school",        # autism / emotional support (name alone is generic)
    "devereux",                 # behavioral health
    "wordsworth",               # residential treatment for youth
    "elwyn",                    # intellectual / developmental disabilities
    "woods services",           # intellectual disabilities / autism
    "melmark",                  # autism / intellectual disabilities
    "new story",                # special education network
]


def _is_excluded_by_name(school_name: str) -> bool:
    """Return True if the school name signals a specialized / non-competitor population."""
    name_lower = school_name.lower()
    for kw in _EXCLUDE_NAME_KEYWORDS:
        if kw in name_lower:
            return True
    for excluded in _EXCLUDE_SCHOOL_NAMES:
        if excluded in name_lower:
            return True
    return False

# Competitor tier classification based on ORIENT (affiliation) and TYPOLOGY
# Tier weights control how much each competitor type affects scoring
TIER_DIRECT_WEIGHT = 1.0    # Catholic schools — same family pool
TIER_STRONG_WEIGHT = 0.7    # Other religious schools — faith-based competition
TIER_MODERATE_WEIGHT = 0.4  # Secular/nonsectarian — quality competition only
TIER_WEAK_WEIGHT = 0.15     # Special emphasis (Montessori, STEM, arts) — niche overlap

# ORIENT codes that are Christian/religious (compete on faith-based value proposition)
# Excludes Catholic (1) which gets "direct" tier, and Nonsectarian (30) which gets "moderate"
_RELIGIOUS_ORIENT_CODES = {
    "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13",
    "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24",
    "25", "26", "27", "28", "29", "31", "32", "33",
}


def _cache_is_valid() -> bool:
    """Return True if the cached PSS file exists and looks complete."""
    return PSS_FILE.exists() and PSS_FILE.stat().st_size >= PSS_MIN_FILE_SIZE


async def _download_pss_data() -> bool:
    """Download and extract the NCES PSS data file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading NCES PSS data from %s", PSS_DOWNLOAD_URL)
    try:
        async with httpx.AsyncClient(timeout=_PSS_DOWNLOAD_TIMEOUT) as client:
            response = await client.get(PSS_DOWNLOAD_URL, follow_redirects=True)
            response.raise_for_status()

        logger.info("PSS download complete (%d KB). Extracting...", len(response.content) // 1024)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
        if not csv_files:
            logger.error("No CSV found in PSS ZIP archive")
            return False

        with z.open(csv_files[0]) as f:
            PSS_FILE.write_bytes(f.read())

        logger.info("PSS data saved to %s (%d KB)", PSS_FILE, PSS_FILE.stat().st_size // 1024)
        return True

    except httpx.HTTPStatusError as e:
        logger.error("PSS download HTTP error: %s — %s", e.response.status_code, e.request.url)
        return False
    except (httpx.RequestError, zipfile.BadZipFile, OSError) as e:
        logger.error("PSS download failed: %s: %s", type(e).__name__, e)
        return False


def _school_in_polygon(school_lat: float, school_lon: float, polygon_geojson: dict) -> bool:
    """Check whether a school's coordinates fall within a GeoJSON polygon."""
    try:
        from shapely.geometry import Point, shape
        point = Point(school_lon, school_lat)   # shapely: (x=lon, y=lat)
        return shape(polygon_geojson).contains(point)
    except Exception:  # shapely/GEOS raises many error types; any failure → False
        return False


async def get_nearby_schools(
    lat: float,
    lon: float,
    radius_miles: float,
    gender: str = "coed",
    grade_level: str = "k12",
    isochrone_polygon: Optional[dict] = None,
) -> List[dict]:
    """
    Return private schools within the catchment area of (lat, lon).

    When isochrone_polygon is provided, schools are filtered by whether their
    coordinates fall inside the drive-time polygon (using shapely point-in-polygon).
    The radius_miles parameter is still used as a generous bounding-box pre-filter
    (typically the isochrone's effective radius) to avoid testing every school
    in the country against the polygon.

    Falls back to pure radius filtering when no polygon is supplied.
    Filters by grade level overlap and gender compatibility.
    Downloads NCES PSS data on first call; cached locally after that.
    """
    # Ensure the file exists and is uncorrupted; re-download if needed.
    # After a successful file check, load into the module-level DataFrame cache
    # so subsequent requests skip the ~15 MB disk read entirely.
    global _pss_df
    async with _pss_lock:
        if not _cache_is_valid():
            success = await _download_pss_data()
            if not success:
                logger.warning("PSS data unavailable — returning empty school list")
                return []
            _pss_df = None  # Force cache reload after a fresh download

        needed_cols = [
            "PINST", "LATITUDE22", "LONGITUDE22", "ORIENT",
            "PCITY", "PZIP", "NUMSTUDS", "P335", "GRADE2", "TYPOLOGY",
        ]

        if _pss_df is None:
            raw = pd.read_csv(PSS_FILE, encoding="latin-1", low_memory=False)
            raw.columns = raw.columns.str.strip()
            existing_cols = [c for c in needed_cols if c in raw.columns]
            _pss_df = raw[existing_cols]
            logger.info("PSS DataFrame cached in memory (%d schools)", len(_pss_df))

    try:
        needed_cols = [
            "PINST", "LATITUDE22", "LONGITUDE22", "ORIENT",
            "PCITY", "PZIP", "NUMSTUDS", "P335", "GRADE2", "TYPOLOGY",
        ]

        df = _pss_df.copy()

        # Keep only the columns we need (ignore any extras)
        existing_cols = [c for c in needed_cols if c in df.columns]
        df = df[existing_cols]

        # Validate required coordinate columns are present
        if "LATITUDE22" not in df.columns or "LONGITUDE22" not in df.columns:
            logger.error("PSS data missing coordinate columns. Available: %s", list(df.columns))
            return []

        # Coerce coordinates to numeric and drop rows without them
        df["LATITUDE22"] = pd.to_numeric(df["LATITUDE22"], errors="coerce")
        df["LONGITUDE22"] = pd.to_numeric(df["LONGITUDE22"], errors="coerce")
        df = df.dropna(subset=["LATITUDE22", "LONGITUDE22"])

        # Quick bounding-box pre-filter before precise haversine
        lat_buf = radius_miles / 69.0
        lon_buf = radius_miles / 50.0
        df = df[
            (df["LATITUDE22"].between(lat - lat_buf, lat + lat_buf))
            & (df["LONGITUDE22"].between(lon - lon_buf, lon + lon_buf))
        ]

        if df.empty:
            return []

        df["distance_miles"] = df.apply(
            lambda r: haversine_miles(lat, lon, r["LATITUDE22"], r["LONGITUDE22"]),
            axis=1,
        )
        if isochrone_polygon:
            df = df[
                df.apply(
                    lambda r: _school_in_polygon(r["LATITUDE22"], r["LONGITUDE22"], isochrone_polygon),
                    axis=1,
                )
            ].sort_values("distance_miles")
        else:
            df = df[df["distance_miles"] <= radius_miles].sort_values("distance_miles")

        # --- Grade level filter ---
        allowed_grade_levels = GRADE_LEVEL_FILTER.get(grade_level, GRADE_LEVEL_FILTER["k12"])
        if "GRADE2" in df.columns:
            df["_gradelev_str"] = df["GRADE2"].apply(
                lambda v: str(int(v)) if pd.notna(v) else "-1"
            )
            df = df[
                df["_gradelev_str"].isin(allowed_grade_levels)
                | ~df["_gradelev_str"].isin(GRADE2_NAMES)
            ]

        # --- Gender filter ---
        allowed_genders = GENDER_FILTER.get(gender, GENDER_FILTER["coed"])
        if "P335" in df.columns:
            df["_gender_str"] = df["P335"].apply(
                lambda v: str(int(v)) if pd.notna(v) else "-1"
            )
            df = df[
                df["_gender_str"].isin(allowed_genders)
                | ~df["_gender_str"].isin(P335_NAMES)
            ]

        # --- Program emphasis filter: exclude special education/therapeutic ---
        has_typology = "TYPOLOGY" in df.columns
        if has_typology:
            df["_typology_str"] = df["TYPOLOGY"].apply(
                lambda v: str(int(v)) if pd.notna(v) else "-1"
            )
            df = df[~df["_typology_str"].isin(TYPOLOGY_EXCLUDE)]
        else:
            logger.warning(
                "PSS data missing TYPOLOGY column — skipping program emphasis filtering. "
                "All schools will be treated as 'moderate' tier."
            )

        # --- Name-based exclusion: catch behavioral/therapeutic/LD schools ---
        if "PINST" in df.columns:
            name_mask = df["PINST"].apply(
                lambda n: _is_excluded_by_name(str(n)) if pd.notna(n) else False
            )
            excluded_names = df.loc[name_mask, "PINST"].tolist()
            df = df[~name_mask]
            if excluded_names:
                logger.info(
                    "Name-based filter excluded %d school(s): %s",
                    len(excluded_names),
                    excluded_names[:10],  # Log at most 10 names
                )

        results = []
        for _, row in df.iterrows():
            affiliation_code = (
                str(int(row.get("ORIENT", 30))).strip()
                if pd.notna(row.get("ORIENT"))
                else "30"
            )
            is_catholic = affiliation_code == "1"

            raw_enrollment = row.get("NUMSTUDS")
            if pd.notna(raw_enrollment):
                try:
                    enrollment_int = int(raw_enrollment)
                    enrollment = enrollment_int if enrollment_int > 0 else None
                except (ValueError, TypeError):
                    enrollment = None
            else:
                enrollment = None

            gender_code = (
                str(int(row["P335"])) if "P335" in df.columns and pd.notna(row.get("P335")) else "-1"
            )
            gradelev_code = (
                str(int(row["GRADE2"])) if "GRADE2" in df.columns and pd.notna(row.get("GRADE2")) else "-1"
            )

            # --- Competitor tier classification ---
            typology_code = (
                str(int(row["TYPOLOGY"])) if has_typology and pd.notna(row.get("TYPOLOGY")) else "-1"
            )
            is_special_emphasis = typology_code in TYPOLOGY_SPECIAL_EMPHASIS

            if is_special_emphasis:
                competitor_tier = "weak"
                tier_weight = TIER_WEAK_WEIGHT
            elif is_catholic:
                competitor_tier = "direct"
                tier_weight = TIER_DIRECT_WEIGHT
            elif affiliation_code in _RELIGIOUS_ORIENT_CODES:
                competitor_tier = "strong"
                tier_weight = TIER_STRONG_WEIGHT
            else:
                # Nonsectarian / unknown affiliation with regular program
                competitor_tier = "moderate"
                tier_weight = TIER_MODERATE_WEIGHT

            results.append({
                "name": str(row.get("PINST", "Unknown School")),
                "lat": float(row["LATITUDE22"]),
                "lon": float(row["LONGITUDE22"]),
                "distance_miles": round(float(row["distance_miles"]), 1),
                "affiliation": PSS_AFFILIATION_NAMES.get(affiliation_code, f"Other ({affiliation_code})"),
                "is_catholic": is_catholic,
                "city": str(row.get("PCITY", "")),
                "enrollment": enrollment,
                "gender": P335_NAMES.get(gender_code, "Unknown"),
                "grade_level": GRADE2_NAMES.get(gradelev_code, "Unknown"),
                "competitor_tier": competitor_tier,
                "tier_weight": tier_weight,
            })

        return results

    except (KeyError, ValueError, AttributeError, TypeError) as e:
        logger.error("School data processing error: %s: %s", type(e).__name__, e)
        return []
