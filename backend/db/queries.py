"""Spatial and data queries against the precomputed database.

These replace the live Census/NCES/CMS API calls in the v1 pipeline.
"""

import json
from datetime import datetime, timezone, timedelta
from typing import Optional

from geoalchemy2 import Geography
from geoalchemy2.functions import ST_DWithin, ST_MakePoint, ST_SetSRID, ST_AsGeoJSON
from sqlalchemy import select, func, text, and_, cast, Float
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    CensusTract,
    CensusTractHistory,
    CompetitorSchoolRecord,
    CompetitorElderCare,
    CompetitorHousing,
    IsochroneCache,
    TractFeasibilityScore,
    HudLihtcProperty,
    HudLihtcTenant,
    HudQctDdaDesignation,
    HudPropertyDesignationMatch,
)


# Approximate degrees per mile at mid-latitudes (used for ST_DWithin with geography)
DEGREES_PER_MILE = 1.0 / 69.0
METERS_PER_MILE = 1609.34

_GEOGRAPHY = Geography(srid=4326)


def _utc_now_naive() -> datetime:
    """Return current UTC time as a timezone-naive datetime for DB DateTime columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _make_point(lat: float, lon: float):
    """Create a PostGIS geography point expression."""
    return func.ST_SetSRID(func.ST_MakePoint(lon, lat), 4326)


def _geo(expr):
    """Cast a geometry expression to geography for accurate distance calculations."""
    return cast(expr, _GEOGRAPHY)


def _tract_reference_point_expr():
    """Best-available tract point for spatial queries (centroid, else polygon interior point)."""
    return func.coalesce(
        CensusTract.centroid,
        func.ST_PointOnSurface(CensusTract.boundary),
    )


async def get_tracts_in_radius(
    session: AsyncSession,
    lat: float,
    lon: float,
    radius_miles: float,
) -> list[CensusTract]:
    """Return all census tracts whose centroid falls within radius_miles of (lat, lon)."""
    point = _make_point(lat, lon)
    distance_meters = radius_miles * METERS_PER_MILE

    stmt = (
        select(CensusTract)
        .where(
            func.ST_DWithin(
                _geo(_tract_reference_point_expr()),
                _geo(point),
                distance_meters,
            )
        )
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_tracts_in_polygon(
    session: AsyncSession,
    polygon_geojson: dict,
) -> list[CensusTract]:
    """Return all census tracts whose centroid falls within the given GeoJSON polygon."""
    geojson_str = json.dumps(polygon_geojson)
    polygon_geom = func.ST_SetSRID(func.ST_GeomFromGeoJSON(geojson_str), 4326)

    stmt = (
        select(CensusTract)
        .where(func.ST_Within(_tract_reference_point_expr(), polygon_geom))
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_tracts_in_catchment(
    session: AsyncSession,
    lat: float,
    lon: float,
    radius_miles: float,
    isochrone_geojson: Optional[dict] = None,
) -> list[CensusTract]:
    """Get tracts using isochrone polygon if available, otherwise radius."""
    if isochrone_geojson:
        return await get_tracts_in_polygon(session, isochrone_geojson)
    return await get_tracts_in_radius(session, lat, lon, radius_miles)




async def get_tracts_by_county(
    session: AsyncSession,
    county_fips: str,
    *,
    state_fips: str | None = None,
    limit: int = 200,
) -> list[CensusTract]:
    """Return tracts for a county (degraded fallback when tract geometry is unavailable)."""
    county_only = (county_fips or "").strip().zfill(3)
    county_candidates = {
        (county_fips or "").strip(),
        county_only,
    }
    if state_fips:
        state_norm = state_fips.strip().zfill(2)
        county_candidates.add(f"{state_norm}{county_only}")

    candidate_values = [value for value in county_candidates if value]
    stmt = select(CensusTract).where(CensusTract.county_fips.in_(candidate_values)).limit(limit)
    result = await session.execute(stmt)
    return list(result.scalars().all())
async def get_historical_tracts(
    session: AsyncSession,
    geoids: list[str],
    vintage: str = "2017",
) -> list[CensusTractHistory]:
    """Get historical demographic data for a set of tract GEOIDs."""
    stmt = (
        select(CensusTractHistory)
        .where(
            and_(
                CensusTractHistory.geoid.in_(geoids),
                CensusTractHistory.acs_vintage == vintage,
            )
        )
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_nearby_schools(
    session: AsyncSession,
    lat: float,
    lon: float,
    radius_miles: float,
    isochrone_geojson: Optional[dict] = None,
    limit: int = 25,
) -> list[CompetitorSchoolRecord]:
    """Get private schools within catchment, ordered by distance."""
    point = _make_point(lat, lon)

    if isochrone_geojson:
        geojson_str = json.dumps(isochrone_geojson)
        polygon_geom = func.ST_SetSRID(func.ST_GeomFromGeoJSON(geojson_str), 4326)
        spatial_filter = func.ST_Within(CompetitorSchoolRecord.location, polygon_geom)
    else:
        distance_meters = radius_miles * METERS_PER_MILE
        spatial_filter = func.ST_DWithin(
            _geo(CompetitorSchoolRecord.location),
            _geo(point),
            distance_meters,
        )

    distance_expr = func.ST_Distance(
        _geo(CompetitorSchoolRecord.location),
        _geo(point),
    ) / METERS_PER_MILE

    stmt = (
        select(CompetitorSchoolRecord, distance_expr.label("distance_miles"))
        .where(spatial_filter)
        .order_by(distance_expr)
        .limit(limit)
    )
    result = await session.execute(stmt)
    return [(row[0], row[1]) for row in result.all()]


async def get_nearby_elder_care(
    session: AsyncSession,
    lat: float,
    lon: float,
    radius_miles: float,
    care_level: str = "all",
    limit: int = 25,
) -> list[CompetitorElderCare]:
    """Get elder care facilities within radius, ordered by distance."""
    point = _make_point(lat, lon)
    distance_meters = radius_miles * METERS_PER_MILE

    conditions = [
        func.ST_DWithin(
            _geo(CompetitorElderCare.location),
            _geo(point),
            distance_meters,
        )
    ]
    if care_level != "all":
        conditions.append(CompetitorElderCare.care_level == care_level)

    distance_expr = func.ST_Distance(
        _geo(CompetitorElderCare.location),
        _geo(point),
    ) / METERS_PER_MILE

    stmt = (
        select(CompetitorElderCare, distance_expr.label("distance_miles"))
        .where(and_(*conditions))
        .order_by(distance_expr)
        .limit(limit)
    )
    result = await session.execute(stmt)
    return [(row[0], row[1]) for row in result.all()]


async def get_nearby_housing(
    session: AsyncSession,
    lat: float,
    lon: float,
    radius_miles: float,
    limit: int = 25,
) -> list[CompetitorHousing]:
    """Get LIHTC housing projects within radius, ordered by distance."""
    point = _make_point(lat, lon)
    distance_meters = radius_miles * METERS_PER_MILE

    distance_expr = func.ST_Distance(
        _geo(CompetitorHousing.location),
        _geo(point),
    ) / METERS_PER_MILE

    stmt = (
        select(CompetitorHousing, distance_expr.label("distance_miles"))
        .where(
            func.ST_DWithin(
                _geo(CompetitorHousing.location),
                _geo(point),
                distance_meters,
            )
        )
        .order_by(distance_expr)
        .limit(limit)
    )
    result = await session.execute(stmt)
    return [(row[0], row[1]) for row in result.all()]


async def get_hud_property_designations(
    session: AsyncSession,
    *,
    hud_id: str,
    dataset_year: int,
) -> list[tuple[HudLihtcProperty, HudQctDdaDesignation, HudPropertyDesignationMatch]]:
    """Fetch normalized HUD property designation matches for an exact property/year key."""
    stmt = (
        select(HudLihtcProperty, HudQctDdaDesignation, HudPropertyDesignationMatch)
        .join(HudPropertyDesignationMatch, HudPropertyDesignationMatch.property_row_id == HudLihtcProperty.id)
        .join(HudQctDdaDesignation, HudQctDdaDesignation.id == HudPropertyDesignationMatch.designation_row_id)
        .where(
            HudLihtcProperty.hud_id == hud_id,
            HudLihtcProperty.dataset_year == dataset_year,
        )
        .order_by(HudQctDdaDesignation.designation_type.asc())
    )
    result = await session.execute(stmt)
    return list(result.all())


async def get_hud_tenant_rows(
    session: AsyncSession,
    *,
    reporting_year: int,
    geoid11: str | None = None,
) -> list[HudLihtcTenant]:
    """Fetch normalized HUD tenant rows by reporting year and optional exact GEOID."""
    filters = [HudLihtcTenant.reporting_year == reporting_year]
    if geoid11:
        filters.append(HudLihtcTenant.geoid11 == geoid11)

    stmt = select(HudLihtcTenant).where(and_(*filters))
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_latest_hud_property_dataset_year(session: AsyncSession) -> int | None:
    """Return the newest HUD LIHTC property dataset year available in DB."""
    stmt = select(func.max(HudLihtcProperty.dataset_year))
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_nearby_hud_housing_context(
    session: AsyncSession,
    *,
    lat: float,
    lon: float,
    radius_miles: float,
    dataset_year: int,
    limit: int = 50,
) -> list[dict]:
    """Return nearby normalized HUD LIHTC properties enriched with QCT/DDA + tenant context."""
    point = _make_point(lat, lon)
    distance_meters = radius_miles * METERS_PER_MILE

    distance_expr = func.ST_Distance(_geo(HudLihtcProperty.location), _geo(point)) / METERS_PER_MILE
    stmt = (
        select(HudLihtcProperty, distance_expr.label("distance_miles"))
        .where(
            HudLihtcProperty.dataset_year == dataset_year,
            func.ST_DWithin(_geo(HudLihtcProperty.location), _geo(point), distance_meters),
        )
        .order_by(distance_expr)
        .limit(limit)
    )
    property_rows = list((await session.execute(stmt)).all())
    if not property_rows:
        return []

    properties = [r[0] for r in property_rows]
    property_ids = [p.id for p in properties]
    geoids = sorted({p.geoid11 for p in properties if p.geoid11})

    designation_stmt = (
        select(HudPropertyDesignationMatch.property_row_id, HudQctDdaDesignation.designation_type)
        .join(HudQctDdaDesignation, HudQctDdaDesignation.id == HudPropertyDesignationMatch.designation_row_id)
        .where(
            HudPropertyDesignationMatch.property_row_id.in_(property_ids),
            HudPropertyDesignationMatch.designation_year == dataset_year,
        )
    )
    designation_rows = list((await session.execute(designation_stmt)).all())
    designation_map: dict[int, set[str]] = {}
    for property_row_id, designation_type in designation_rows:
        designation_map.setdefault(property_row_id, set()).add(designation_type)

    tenant_map: dict[str, dict] = {}
    if geoids:
        tenant_stmt = (
            select(
                HudLihtcTenant.geoid11,
                func.sum(HudLihtcTenant.household_count).label("households"),
                func.avg(HudLihtcTenant.average_household_income).label("avg_income"),
            )
            .where(
                HudLihtcTenant.reporting_year == dataset_year,
                HudLihtcTenant.geoid11.in_(geoids),
                HudLihtcTenant.join_confidence >= 0.7,
            )
            .group_by(HudLihtcTenant.geoid11)
        )
        for geoid11, households, avg_income in list((await session.execute(tenant_stmt)).all()):
            tenant_map[geoid11] = {
                "tenant_households": int(households or 0),
                "avg_tenant_income": float(avg_income) if avg_income is not None else None,
            }

    output: list[dict] = []
    for prop, distance in property_rows:
        types = designation_map.get(prop.id, set())
        tenant_ctx = tenant_map.get(prop.geoid11 or "", {})
        output.append(
            {
                "name": prop.project_name,
                "hud_id": prop.hud_id,
                "lat": prop.lat,
                "lon": prop.lon,
                "distance_miles": round(float(distance), 2),
                "city": prop.city,
                "li_units": prop.low_income_units,
                "geoid11": prop.geoid11,
                "is_qct": "QCT" in types,
                "is_dda": "DDA" in types,
                "tenant_households": tenant_ctx.get("tenant_households"),
                "avg_tenant_income": tenant_ctx.get("avg_tenant_income"),
            }
        )
    return output


async def lookup_cached_isochrone(
    session: AsyncSession,
    lat: float,
    lon: float,
    drive_minutes: int,
    tolerance_miles: float = 0.5,
    max_age_hours: int | None = None,
) -> Optional[IsochroneCache]:
    """Find a cached isochrone within spatial tolerance of the given point."""
    # REVIEW[CACHE]: Isochrone cache lookup has no freshness bound; stale polygons can persist indefinitely with no TTL-based invalidation.
    point = _make_point(lat, lon)
    tolerance_meters = tolerance_miles * METERS_PER_MILE

    filters = [
        IsochroneCache.drive_minutes == drive_minutes,
        func.ST_DWithin(
            _geo(IsochroneCache.location_point),
            _geo(point),
            tolerance_meters,
        ),
    ]
    if max_age_hours is not None:
        cutoff = _utc_now_naive() - timedelta(hours=max_age_hours)
        filters.append(IsochroneCache.created_at >= cutoff)

    stmt = (
        select(IsochroneCache)
        .where(and_(*filters))
        .order_by(
            func.ST_Distance(
                _geo(IsochroneCache.location_point),
                _geo(point),
            )
        )
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def save_isochrone(
    session: AsyncSession,
    lat: float,
    lon: float,
    drive_minutes: int,
    polygon_geojson: dict,
    effective_radius_miles: float,
):
    """Cache a computed isochrone polygon."""
    geojson_str = json.dumps(polygon_geojson)
    point = _make_point(lat, lon)

    cache_entry = IsochroneCache(
        lat=lat,
        lon=lon,
        drive_minutes=drive_minutes,
        location_point=point,
        polygon=func.ST_SetSRID(func.ST_GeomFromGeoJSON(geojson_str), 4326),
        polygon_geojson=geojson_str,
        effective_radius_miles=effective_radius_miles,
    )
    session.add(cache_entry)
    await session.flush()
    return cache_entry


async def get_top_opportunities(
    session: AsyncSession,
    ministry_type: str,
    state_fips: Optional[str] = None,
    min_score: int = 0,
    limit: int = 50,
) -> list[TractFeasibilityScore]:
    """Get top-scoring tracts for opportunity discovery."""
    conditions = [
        TractFeasibilityScore.ministry_type == ministry_type,
        TractFeasibilityScore.overall_score >= min_score,
    ]
    if state_fips:
        conditions.append(
            TractFeasibilityScore.geoid.startswith(state_fips)
        )

    stmt = (
        select(TractFeasibilityScore)
        .where(and_(*conditions))
        .order_by(TractFeasibilityScore.overall_score.desc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())
