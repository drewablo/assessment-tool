"""NAIS school reconciliation pipeline.

Matches NAIS schools to existing PSS records, flags overlaps, and inserts
NAIS-only schools into competitors_schools after Census geocoding.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.connection import async_session_factory
from db.models import CompetitorSchoolRecord
from pipeline.base import finish_pipeline_run, start_pipeline_run
from pipeline.celery_app import celery_app
from pipeline.ingest_schools import _classify_tier

logger = logging.getLogger("pipeline.nais")
NAIS_CSV_PATH = Path(__file__).resolve().parents[1] / "exsources" / "nais_schools.csv"
_COMMON_SUFFIXES = {
    "school", "academy", "the", "of", "and", "inc", "campus", "prep", "preparatory"
}


@dataclass
class MatchCandidate:
    ppin: str
    school_name: str
    state: str | None
    zip_code: str | None
    normalized_name: str



def _normalize_name(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9\s]", " ", (name or "").lower())
    tokens = [token for token in cleaned.split() if token and token not in _COMMON_SUFFIXES]
    return " ".join(tokens)



def _clean_zip(value: Any) -> str | None:
    if value is None:
        return None
    match = re.search(r"(\d{5})", str(value))
    return match.group(1) if match else None



def _sequence_distance(left: str, right: str) -> int:
    if left == right:
        return 0
    max_len = max(len(left), len(right), 1)
    return round((1.0 - SequenceMatcher(None, left, right).ratio()) * max_len)



def _find_pss_match(nais_row: dict[str, Any], pss_candidates: list[MatchCandidate]) -> MatchCandidate | None:
    normalized_name = _normalize_name(str(nais_row.get("name") or ""))
    state = str(nais_row.get("state") or "").strip().upper() or None
    zip_code = _clean_zip(nais_row.get("zip"))
    if not normalized_name:
        return None

    scoped = [candidate for candidate in pss_candidates if candidate.state == state or (zip_code and candidate.zip_code == zip_code)]
    search_pool = scoped or pss_candidates

    for candidate in search_pool:
        if candidate.normalized_name == normalized_name and (candidate.state == state or (zip_code and candidate.zip_code == zip_code)):
            return candidate

    fuzzy_matches = [
        candidate for candidate in search_pool
        if _sequence_distance(candidate.normalized_name, normalized_name) <= 3
        and (candidate.state == state or (zip_code and candidate.zip_code == zip_code))
    ]
    if not fuzzy_matches:
        return None
    return max(fuzzy_matches, key=lambda candidate: SequenceMatcher(None, candidate.normalized_name, normalized_name).ratio())



def _infer_affiliation(name: str) -> tuple[str, bool]:
    lowered = (name or "").lower()
    if "catholic" in lowered:
        return "Catholic", True
    if "jewish" in lowered or "hebrew" in lowered:
        return "Jewish", False
    if "christian" in lowered:
        return "Christian", False
    if "montessori" in lowered:
        return "Montessori", False
    return "Independent", False



def _parse_enrollment_midpoint(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    match = re.search(r"(\d+)\s*-\s*(\d+)", str(value))
    if match:
        low, high = int(match.group(1)), int(match.group(2))
        return (low + high) // 2
    match = re.search(r"(\d+)\+", str(value))
    if match:
        return int(match.group(1))
    return None



def _map_student_body(value: Any) -> str:
    mapping = {"CoEd": "Co-ed", "All Boys": "All Boys", "All Girls": "All Girls"}
    return mapping.get(str(value or "").strip(), "Unknown")



def _map_grade_level(value: Any) -> str:
    text = str(value or "").lower()
    has_elementary = "elementary" in text or "primary" in text or "pre-school" in text
    has_middle = "middle school" in text or "junior high" in text
    has_high = "high school" in text or "secondary" in text
    if has_high and (has_elementary or has_middle):
        return "Combined/Other"
    if has_high:
        return "Secondary/High"
    if has_elementary or has_middle:
        return "Elementary/Middle"
    return "Unknown"


async def _geocode_address(client: httpx.AsyncClient, row: dict[str, Any]) -> tuple[float | None, float | None]:
    params = {
        "street": row.get("street") or "",
        "city": row.get("city") or "",
        "state": row.get("state") or "",
        "zip": _clean_zip(row.get("zip")) or "",
        "benchmark": "Public_AR_Current",
        "format": "json",
    }
    response = await client.get(
        "https://geocoding.geo.census.gov/geocoder/locations/address",
        params=params,
        timeout=30.0,
    )
    response.raise_for_status()
    matches = (((response.json() or {}).get("result") or {}).get("addressMatches") or [])
    if not matches:
        return None, None
    coordinates = matches[0].get("coordinates") or {}
    return coordinates.get("y"), coordinates.get("x")


async def _load_existing_pss_candidates() -> list[MatchCandidate]:
    async with async_session_factory() as session:
        rows = (await session.execute(select(CompetitorSchoolRecord))).scalars().all()
    return [
        MatchCandidate(
            ppin=row.ppin,
            school_name=row.school_name,
            state=row.state,
            zip_code=None,
            normalized_name=_normalize_name(row.school_name),
        )
        for row in rows
    ]


async def _flag_pss_matches(matches: dict[str, str]) -> None:
    if not matches:
        return
    async with async_session_factory() as session:
        schools = (await session.execute(select(CompetitorSchoolRecord).where(CompetitorSchoolRecord.ppin.in_(list(matches))))).scalars().all()
        for school in schools:
            school.also_in_nais = True
            school.nais_id = matches[school.ppin]
        await session.commit()


@celery_app.task(name="pipeline.ingest_nais.ingest_nais_data", bind=True)
def ingest_nais_data(self):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_ingest_nais_async())
    finally:
        loop.close()


async def _ingest_nais_async(csv_path: Path | None = None) -> dict[str, int]:
    csv_file = csv_path or NAIS_CSV_PATH
    if not csv_file.exists():
        logger.warning("NAIS CSV not found at %s — skipping NAIS ingest", csv_file)
        return {"inserted": 0, "matched_to_pss": 0, "geocode_failures": 0, "skipped": 0}
    df = pd.read_csv(csv_file)

    async with async_session_factory() as session:
        run = await start_pipeline_run(session, "nais_schools")
        await session.commit()

    try:
        pss_candidates = await _load_existing_pss_candidates()
        matched_to_pss = 0
        inserted = 0
        geocode_failures = 0
        matched_flags: dict[str, str] = {}
        insert_rows: list[dict[str, Any]] = []

        async with httpx.AsyncClient() as client:
            for nais_row in df.to_dict(orient="records"):
                match = _find_pss_match(nais_row, pss_candidates)
                if match is not None:
                    matched_to_pss += 1
                    matched_flags[match.ppin] = str(nais_row.get("id"))
                    continue

                lat, lon = await _geocode_address(client, nais_row)
                if lat is None or lon is None:
                    geocode_failures += 1
                    await asyncio.sleep(1.0)
                    continue

                affiliation_label, is_catholic = _infer_affiliation(str(nais_row.get("name") or ""))
                enrollment = _parse_enrollment_midpoint(nais_row.get("enrollment_size"))
                tier, weight = _classify_tier(1 if is_catholic else None, None)
                insert_rows.append({
                    "ppin": f"NAIS-{nais_row['id']}",
                    "school_name": str(nais_row.get("name") or "").strip(),
                    "lat": float(lat),
                    "lon": float(lon),
                    "location": from_shape(Point(float(lon), float(lat)), srid=4326),
                    "city": str(nais_row.get("city") or "").strip() or None,
                    "state": str(nais_row.get("state") or "").strip().upper() or None,
                    "county_fips": None,
                    "religious_affiliation_code": 1 if is_catholic else None,
                    "affiliation_label": affiliation_label,
                    "is_catholic": is_catholic,
                    "enrollment": enrollment,
                    "coeducation": _map_student_body(nais_row.get("student_body")),
                    "grade_level": _map_grade_level(nais_row.get("grade_levels")),
                    "typology_code": None,
                    "competitor_tier": tier,
                    "tier_weight": weight,
                    "data_source": "nais",
                    "also_in_nais": True,
                    "nais_id": str(nais_row.get("id")),
                    "pss_vintage": "NAIS",
                })
                inserted += 1
                await asyncio.sleep(1.0)

        await _flag_pss_matches(matched_flags)

        if insert_rows:
            async with async_session_factory() as session:
                stmt = pg_insert(CompetitorSchoolRecord).values(insert_rows)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["ppin"],
                    set_={col: stmt.excluded[col] for col in insert_rows[0].keys() if col != "ppin"},
                )
                await session.execute(stmt)
                await session.commit()

        result = {
            "total_records": len(df),
            "matched_to_pss": matched_to_pss,
            "inserted": inserted,
            "geocode_failures": geocode_failures,
        }
        logger.info("NAIS ingestion summary: %s", result)

        async with async_session_factory() as session:
            await finish_pipeline_run(
                session,
                run,
                status="success",
                records_processed=len(df),
                records_inserted=inserted,
                metadata_json=result,
            )
            await session.commit()
        return result
    except Exception as exc:
        logger.error("NAIS ingestion failed: %s", exc)
        async with async_session_factory() as session:
            await finish_pipeline_run(session, run, status="failed", error_message=str(exc))
            await session.commit()
        raise
