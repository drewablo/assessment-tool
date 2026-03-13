"""HUD phased ingestion foundation for LIHTC property/tenant and QCT-DDA datasets."""

from __future__ import annotations

import csv
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from geoalchemy2.shape import from_shape
from shapely import wkt
from shapely.geometry import MultiPolygon, Point, Polygon
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    HudIngestRun,
    HudLihtcProperty,
    HudLihtcTenant,
    HudPropertyDesignationMatch,
    HudQctDdaDesignation,
    HudRawSnapshot,
)
from pipeline.hud_contracts import CONTRACTS, resolve_field, validate_columns

HUD_RAW_ROOT = Path("backend/data/raw/hud")


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_geoid(value: Any) -> str | None:
    if value is None:
        return None
    raw = "".join(ch for ch in str(value) if ch.isdigit())
    if len(raw) == 11:
        return raw
    if len(raw) in {10, 9, 8, 7, 6, 5, 4, 3, 2, 1}:
        return raw.zfill(11)
    return None


def _normalize_county_fips(value: Any) -> str | None:
    if value is None:
        return None
    raw = "".join(ch for ch in str(value) if ch.isdigit())
    if not raw:
        return None
    return raw.zfill(5)[-5:]


def determine_tenant_join_method(hud_id: str | None, geoid11: str | None, property_keys: set[tuple[str, int]], dataset_year: int) -> tuple[str, float]:
    """Deterministically choose tenant->property join method and confidence."""
    if hud_id and (hud_id, dataset_year) in property_keys:
        return "hud_id_exact", 1.0
    if geoid11:
        return "tract_exact", 0.7
    return "unmatched", 0.0


def _load_rows(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text())
        if isinstance(payload, list):
            return [dict(item) for item in payload]
        if isinstance(payload, dict):
            data = payload.get("data") or payload.get("results") or payload.get("rows")
            if isinstance(data, list):
                return [dict(item) for item in data]
        return []

    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return [dict(row) for row in reader]


def _parse_boundary_wkt(wkt_value: Any, designation_type: str):
    if not wkt_value:
        return None
    try:
        parsed = wkt.loads(str(wkt_value))
        if isinstance(parsed, Polygon):
            parsed = MultiPolygon([parsed])
        if not isinstance(parsed, MultiPolygon):
            raise ValueError("Geometry must be Polygon or MultiPolygon")
        return from_shape(parsed, srid=4326)
    except Exception as exc:
        raise ValueError(f"Malformed geospatial input for {designation_type}: {exc}") from exc


async def create_ingest_run(
    session: AsyncSession,
    *,
    source_family: str,
    source_identifier: str,
    dataset_year: int,
    source_version: str | None,
) -> HudIngestRun:
    run = HudIngestRun(
        source_family=source_family,
        source_identifier=source_identifier,
        dataset_year=dataset_year,
        source_version=source_version,
        status="running",
        started_at=_utc_now_naive(),
    )
    session.add(run)
    await session.flush()
    return run


async def snapshot_raw_source(
    session: AsyncSession,
    *,
    run: HudIngestRun,
    source_uri: str,
    source_file: Path,
    schema_version: str | None = None,
) -> HudRawSnapshot:
    checksum = _sha256_file(source_file)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot_dir = HUD_RAW_ROOT / run.source_family / str(run.dataset_year) / f"run_{run.id}_{stamp}"
    snapshot_dir.mkdir(parents=True, exist_ok=False)
    snapshot_path = snapshot_dir / source_file.name
    shutil.copy2(source_file, snapshot_path)

    snap = HudRawSnapshot(
        ingest_run_id=run.id,
        source_family=run.source_family,
        dataset_year=run.dataset_year,
        source_version=run.source_version,
        source_uri=source_uri,
        snapshot_path=str(snapshot_path),
        file_name=source_file.name,
        file_format=source_file.suffix.lower().lstrip("."),
        size_bytes=snapshot_path.stat().st_size,
        checksum_sha256=checksum,
        schema_version=schema_version,
        validated=False,
    )
    session.add(snap)

    run.checksum_sha256 = checksum
    run.snapshot_root = str(snapshot_dir)
    await session.flush()
    return snap


async def validate_snapshot_contract(session: AsyncSession, snapshot: HudRawSnapshot) -> list[str]:
    rows = _load_rows(Path(snapshot.snapshot_path))
    if not rows:
        errors = ["Snapshot contains no rows"]
    else:
        errors = validate_columns(snapshot.source_family, set(rows[0].keys()))

    snapshot.validated = not errors
    snapshot.validation_errors = {"errors": errors} if errors else None
    await session.flush()
    return errors


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def normalize_lihtc_property(session: AsyncSession, snapshot: HudRawSnapshot, dataset_year: int) -> int:
    rows = _load_rows(Path(snapshot.snapshot_path))
    aliases = CONTRACTS["lihtc_property"].aliases
    loaded = 0
    for row in rows:
        hud_id = resolve_field(row, "HUD_ID", aliases)
        project_name = resolve_field(row, "PROJECT", aliases)
        lat = _float_or_none(resolve_field(row, "LATITUDE", aliases))
        lon = _float_or_none(resolve_field(row, "LONGITUDE", aliases))
        if not hud_id or not project_name or lat is None or lon is None:
            continue
        if abs(lat) > 90 or abs(lon) > 180 or lat == 0 or lon == 0:
            continue

        geoid11 = _normalize_geoid(resolve_field(row, "TRACT", aliases))
        county_fips = _normalize_county_fips(resolve_field(row, "FIPS", aliases))
        point = from_shape(Point(lon, lat), srid=4326)

        stmt = pg_insert(HudLihtcProperty).values(
            hud_id=str(hud_id),
            dataset_year=dataset_year,
            source_version=snapshot.source_version,
            source_snapshot_id=snapshot.id,
            project_name=str(project_name).strip(),
            street_address=(resolve_field(row, "ADDRESS", aliases) or None),
            city=(resolve_field(row, "PROJ_CTY", aliases) or None),
            state=((resolve_field(row, "PROJ_ST", aliases) or "")[:2] or None),
            zip_code=(resolve_field(row, "ZIP", aliases) or None),
            county_fips=county_fips,
            geoid11=geoid11,
            lat=lat,
            lon=lon,
            location=point,
            total_units=_int_or_none(resolve_field(row, "N_UNITS", aliases)),
            low_income_units=_int_or_none(resolve_field(row, "LI_UNITS", aliases)),
            placed_in_service_year=_int_or_none(resolve_field(row, "YR_PIS", aliases)),
            compliance_end_year=_int_or_none(resolve_field(row, "YR_COMP_END", aliases)),
            extended_use_end_year=_int_or_none(resolve_field(row, "YR_EXT_END", aliases)),
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_hud_lihtc_property_hudid_year",
            set_={
                "source_version": stmt.excluded.source_version,
                "source_snapshot_id": stmt.excluded.source_snapshot_id,
                "project_name": stmt.excluded.project_name,
                "street_address": stmt.excluded.street_address,
                "city": stmt.excluded.city,
                "state": stmt.excluded.state,
                "zip_code": stmt.excluded.zip_code,
                "county_fips": stmt.excluded.county_fips,
                "geoid11": stmt.excluded.geoid11,
                "lat": stmt.excluded.lat,
                "lon": stmt.excluded.lon,
                "location": stmt.excluded.location,
                "total_units": stmt.excluded.total_units,
                "low_income_units": stmt.excluded.low_income_units,
                "placed_in_service_year": stmt.excluded.placed_in_service_year,
                "compliance_end_year": stmt.excluded.compliance_end_year,
                "extended_use_end_year": stmt.excluded.extended_use_end_year,
            },
        )
        await session.execute(stmt)
        loaded += 1

    await session.flush()
    return loaded


async def normalize_lihtc_tenant(session: AsyncSession, snapshot: HudRawSnapshot, dataset_year: int) -> int:
    rows = _load_rows(Path(snapshot.snapshot_path))
    aliases = CONTRACTS["lihtc_tenant"].aliases
    loaded = 0
    for row in rows:
        reporting_year = _int_or_none(resolve_field(row, "REPORTING_YEAR", aliases))
        household_count = _int_or_none(resolve_field(row, "HOUSEHOLD_COUNT", aliases))
        if reporting_year is None or household_count is None:
            continue

        tenant = HudLihtcTenant(
            dataset_year=dataset_year,
            reporting_year=reporting_year,
            source_version=snapshot.source_version,
            source_snapshot_id=snapshot.id,
            hud_id=(resolve_field(row, "HUD_ID", aliases) or None),
            geoid11=_normalize_geoid(resolve_field(row, "TRACT", aliases)),
            household_type=(resolve_field(row, "HOUSEHOLD_TYPE", aliases) or None),
            income_bucket=(resolve_field(row, "INCOME_BUCKET", aliases) or None),
            household_count=household_count,
            average_household_income=_float_or_none(resolve_field(row, "AVG_HH_INCOME", aliases)),
        )
        session.add(tenant)
        loaded += 1

    await session.flush()
    return loaded


async def normalize_qct_dda(session: AsyncSession, snapshot: HudRawSnapshot) -> int:
    rows = _load_rows(Path(snapshot.snapshot_path))
    aliases = CONTRACTS["qct_dda"].aliases
    loaded = 0
    for row in rows:
        designation_year = _int_or_none(resolve_field(row, "DESIGNATION_YEAR", aliases))
        designation_type = (resolve_field(row, "DESIGNATION_TYPE", aliases) or "").upper()
        if designation_year is None or designation_type not in {"QCT", "DDA"}:
            continue

        geom = _parse_boundary_wkt(resolve_field(row, "WKT", aliases), designation_type)

        stmt = pg_insert(HudQctDdaDesignation).values(
            designation_year=designation_year,
            designation_type=designation_type,
            source_snapshot_id=snapshot.id,
            source_version=snapshot.source_version,
            geoid11=_normalize_geoid(resolve_field(row, "TRACT", aliases)),
            state_fips=(str(resolve_field(row, "STATE_FIPS", aliases) or "").zfill(2) or None),
            county_fips=_normalize_county_fips(resolve_field(row, "COUNTY_FIPS", aliases)),
            area_name=(resolve_field(row, "AREA_NAME", aliases) or None),
            boundary=geom,
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_hud_qct_dda_designation_year_type_geoid",
            set_={
                "source_snapshot_id": stmt.excluded.source_snapshot_id,
                "source_version": stmt.excluded.source_version,
                "state_fips": stmt.excluded.state_fips,
                "county_fips": stmt.excluded.county_fips,
                "area_name": stmt.excluded.area_name,
                "boundary": stmt.excluded.boundary,
            },
        )
        await session.execute(stmt)
        loaded += 1

    await session.flush()
    return loaded


async def build_deterministic_joins(session: AsyncSession, designation_year: int) -> int:
    await session.execute(delete(HudPropertyDesignationMatch).where(HudPropertyDesignationMatch.designation_year == designation_year))

    properties = (await session.execute(select(HudLihtcProperty).where(HudLihtcProperty.dataset_year == designation_year))).scalars().all()
    designations = (await session.execute(select(HudQctDdaDesignation).where(HudQctDdaDesignation.designation_year == designation_year))).scalars().all()

    by_geoid: dict[tuple[str, str], list[HudQctDdaDesignation]] = {}
    for d in designations:
        if d.geoid11:
            by_geoid.setdefault((d.geoid11, d.designation_type), []).append(d)

    inserted = 0
    for prop in properties:
        if not prop.geoid11:
            continue
        for dtype in ("QCT", "DDA"):
            for hit in by_geoid.get((prop.geoid11, dtype), []):
                session.add(
                    HudPropertyDesignationMatch(
                        property_row_id=prop.id,
                        designation_row_id=hit.id,
                        designation_year=designation_year,
                        join_method="tract_exact",
                        join_confidence=1.0,
                    )
                )
                inserted += 1

    tenants = (await session.execute(select(HudLihtcTenant).where(HudLihtcTenant.dataset_year == designation_year))).scalars().all()
    property_ids = {(p.hud_id, p.dataset_year): p for p in properties}
    for t in tenants:
        method, confidence = determine_tenant_join_method(
            hud_id=t.hud_id,
            geoid11=t.geoid11,
            property_keys=set(property_ids.keys()),
            dataset_year=t.dataset_year,
        )
        t.join_method = method
        t.join_confidence = confidence

    await session.flush()
    return inserted
