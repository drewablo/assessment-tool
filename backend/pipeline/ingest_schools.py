"""NCES Private School Survey (PSS) data ingestion pipeline.

Downloads the PSS CSV from NCES, classifies schools into competitor tiers,
and upserts into the competitors_schools table with PostGIS points.
"""

import asyncio
import logging
import tempfile
import zipfile
from collections import Counter

import httpx
import pandas as pd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.connection import async_session_factory
from db.models import CompetitorSchoolRecord
from pipeline.base import start_pipeline_run, finish_pipeline_run
from pipeline.celery_app import celery_app

logger = logging.getLogger("pipeline.schools")

PSS_URL = "https://nces.ed.gov/surveys/pss/zip/pss2122_pu_csv.zip"

# Religious affiliation codes (ORIENT column, from NCES PSS codebook)
# ORIENT code 1 = Roman Catholic; 30 = Nonsectarian; 2-29,31-33 = other religious
CATHOLIC_CODES = {1}
RELIGIOUS_CODES = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                   21, 22, 23, 24, 25, 26, 27, 28, 29, 31, 32, 33}

# Special education exclusion keywords
SPECIAL_ED_KEYWORDS = [
    "behavioral", "autism", "autistic", "special education",
    "therapeutic", "treatment", "residential treatment",
    "juvenile", "developmental", "disability",
]

# Schools explicitly excluded (known special-purpose institutions)
EXCLUDED_SCHOOLS = {
    "devereux", "melmark", "devereaux", "bancroft", "new story",
    "pathway school", "vista school", "camphill",
}

# Coeducation code mapping (P335)
# 1=Yes (co-ed), 2=All-female, 3=All-male
COED_MAP = {
    1: "Co-ed",
    2: "All Girls",
    3: "All Boys",
}

# Grade level mapping (GRADE2)
GRADE_LEVEL_MAP = {
    1: "Elementary",
    2: "Middle School",
    3: "High School",
    4: "K-12",
}


def _classify_tier(affiliation_code: int | None, typology: int | None) -> tuple[str, float]:
    """Classify a school into a competitor tier. Returns (tier, weight)."""
    if typology == 9:  # special education
        return "weak", 0.15

    if affiliation_code is None:
        return "moderate", 0.4

    if affiliation_code in CATHOLIC_CODES:
        return "direct", 1.0
    if affiliation_code in RELIGIOUS_CODES:
        return "strong", 0.7
    return "moderate", 0.4


def _is_excluded(name: str, typology: int | None) -> bool:
    """Check if a school should be excluded from competitor analysis."""
    if typology == 9:
        return True

    name_lower = name.lower()
    for keyword in SPECIAL_ED_KEYWORDS:
        if keyword in name_lower:
            return True
    for school in EXCLUDED_SCHOOLS:
        if school in name_lower:
            return True
    return False


def _get_affiliation_label(code: int | None) -> str:
    if code is None:
        return "Unknown"
    if code in CATHOLIC_CODES:
        return "Catholic"
    if code in RELIGIOUS_CODES:
        return "Other Religious"
    return "Nonsectarian"


async def _download_pss() -> pd.DataFrame:
    """Download and parse the NCES PSS CSV."""
    async with httpx.AsyncClient() as client:
        logger.info(f"Downloading PSS data from {PSS_URL}")
        resp = await client.get(PSS_URL, timeout=120.0, follow_redirects=True)
        resp.raise_for_status()

    with tempfile.NamedTemporaryFile(suffix=".zip") as tmp:
        tmp.write(resp.content)
        tmp.flush()
        with zipfile.ZipFile(tmp.name) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                raise ValueError("No CSV found in PSS zip")
            with zf.open(csv_names[0]) as csvf:
                df = pd.read_csv(csvf, low_memory=False)

    logger.info(f"PSS data loaded: {len(df)} schools")
    return df


def _transform_schools(df: pd.DataFrame) -> list[dict]:
    """Transform PSS DataFrame into list of dicts for DB insertion."""
    records = []

    lat_candidates = ["LATITUDE22", "LATITUDE20", "LATITUDE", "LAT"]
    lon_candidates = ["LONGITUDE22", "LONGITUDE20", "LONGITUDE", "LON"]
    available_lat = [col for col in lat_candidates if col in df.columns]
    available_lon = [col for col in lon_candidates if col in df.columns]
    logger.info("Schools coordinate columns discovered lat=%s lon=%s", available_lat, available_lon)

    rejection_counts: Counter[str] = Counter()

    def _pick_value(row: pd.Series, candidates: list[str]):
        for col in candidates:
            value = row.get(col)
            if pd.notna(value):
                return value
        return None

    for _, row in df.iterrows():
        name = str(row.get("PINST", "")).strip()
        lat = _pick_value(row, lat_candidates)
        lon = _pick_value(row, lon_candidates)

        if pd.isna(name) or not name:
            rejection_counts["missing_name"] += 1
            continue

        if lat is None or lon is None or pd.isna(lat) or pd.isna(lon):
            rejection_counts["missing_coordinates"] += 1
            continue

        try:
            lat = float(lat)
            lon = float(lon)
        except (TypeError, ValueError):
            rejection_counts["invalid_coordinates"] += 1
            continue

        if abs(lat) > 90 or abs(lon) > 180 or lat == 0 or lon == 0:
            rejection_counts["out_of_bounds_coordinates"] += 1
            continue

        affiliation_code = None
        raw_aff = row.get("ORIENT")
        if pd.notna(raw_aff):
            try:
                affiliation_code = int(raw_aff)
            except (ValueError, TypeError):
                pass

        typology = None
        raw_typ = row.get("TYPOLOGY")
        if pd.notna(raw_typ):
            try:
                typology = int(raw_typ)
            except (ValueError, TypeError):
                pass

        if _is_excluded(name, typology):
            rejection_counts["excluded_typology_or_name"] += 1
            continue

        tier, weight = _classify_tier(affiliation_code, typology)
        is_catholic = affiliation_code in CATHOLIC_CODES if affiliation_code else False

        enrollment = None
        raw_enr = row.get("NUMSTUDS")
        if pd.notna(raw_enr):
            try:
                enrollment = int(raw_enr)
            except (ValueError, TypeError):
                pass

        coed_code = row.get("P335")
        coeducation = COED_MAP.get(int(coed_code) if pd.notna(coed_code) else None, "Unknown")

        grade_code = row.get("GRADE2")
        grade_level = GRADE_LEVEL_MAP.get(int(grade_code) if pd.notna(grade_code) else None, "Unknown")

        ppin = str(row.get("PPIN", "")).strip()
        if not ppin:
            rejection_counts["missing_ppin"] += 1
            continue

        city = str(row.get("PCITY", "")).strip() if pd.notna(row.get("PCITY")) else None
        state = str(row.get("PSTABB", "")).strip() if pd.notna(row.get("PSTABB")) else None

        county_fips = None
        raw_county = row.get("COFIPS")
        raw_state_fips = row.get("FIPST")
        if pd.notna(raw_county) and pd.notna(raw_state_fips):
            county_fips = f"{int(raw_state_fips):02d}{int(raw_county):03d}"

        records.append({
            "ppin": ppin,
            "school_name": name,
            "lat": lat,
            "lon": lon,
            "city": city,
            "state": state,
            "county_fips": county_fips,
            "religious_affiliation_code": affiliation_code,
            "affiliation_label": _get_affiliation_label(affiliation_code),
            "is_catholic": is_catholic,
            "enrollment": enrollment,
            "coeducation": coeducation,
            "grade_level": grade_level,
            "typology_code": typology,
            "competitor_tier": tier,
            "tier_weight": weight,
            "pss_vintage": "2021-22",
        })

    if rejection_counts:
        logger.info("Schools first-pass rejection reason counts: %s", dict(rejection_counts))

    return records


@celery_app.task(name="pipeline.ingest_schools.ingest_pss_data", bind=True)
def ingest_pss_data(self):
    """Celery task: Download and ingest NCES PSS data."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_ingest_pss_async())
    finally:
        loop.close()


async def _ingest_pss_async():
    """Async implementation of NCES PSS ingestion."""
    async with async_session_factory() as session:
        run = await start_pipeline_run(session, "nces_pss")
        await session.commit()

    try:
        df = await _download_pss()
        records = _transform_schools(df)
        logger.info(f"Transformed {len(records)} school records")

        # Batch upsert with PostGIS point creation
        batch_size = 500
        total_upserted = 0

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]

            # Add WKT point for PostGIS
            for rec in batch:
                point = Point(rec["lon"], rec["lat"])
                rec["location"] = from_shape(point, srid=4326)

            async with async_session_factory() as session:
                stmt = pg_insert(CompetitorSchoolRecord).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["ppin"],
                    set_={
                        col: stmt.excluded[col]
                        for col in batch[0].keys()
                        if col != "ppin"
                    },
                )
                await session.execute(stmt)
                await session.commit()
                total_upserted += len(batch)

        async with async_session_factory() as session:
            await finish_pipeline_run(
                session, run,
                status="success",
                records_processed=len(df),
                records_inserted=total_upserted,
            )
            await session.commit()

        return {"processed": len(df), "upserted": total_upserted}

    except Exception as e:
        logger.error(f"PSS ingestion failed: {e}")
        async with async_session_factory() as session:
            await finish_pipeline_run(session, run, status="failed", error_message=str(e))
            await session.commit()
        raise
