from __future__ import annotations

import gzip
import json
import logging
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

from shapely.geometry import Point, shape
from shapely.prepared import prep

from models.schemas import (
    AnalysisRequest,
    AnalysisResponse,
    DashboardCatchment,
    DashboardDistributionBucket,
    DashboardDrilldownMetric,
    DashboardMetadata,
    DashboardMetricOption,
    DashboardModuleData,
    DashboardResponse,
    DashboardSeriesDescriptor,
    DashboardSidebarItem,
    DashboardTabItem,
    DashboardTimeSeriesPoint,
    DashboardViewCard,
    DashboardZipDrilldown,
)
from services.projections import HistoricalValue, build_projection_envelope

logger = logging.getLogger(__name__)

# --- New per-ZIP directory layout produced by ingest-zcta ---
# data/zcta/bbox_index.json.gz  (~200KB — zip_code -> [minx, miny, maxx, maxy])
# data/zcta/33901.json.gz       (~5-15KB per ZIP)
# data/zcta/33971.json.gz
# data/zcta/_ready               (marker file)
ZCTA_CACHE_DIR = Path(os.getenv("ZCTA_CACHE_DIR", Path(__file__).resolve().parents[1] / "data" / "zcta"))
# Legacy single-file path (for backward-compatible cache status reporting)
ZCTA_CACHE_PATH = Path(os.getenv("ZCTA_CACHE_PATH", Path(__file__).resolve().parents[1] / "data" / "zcta_boundaries.json.gz"))

DASHBOARD_DATA_YEAR = int(os.getenv("DASHBOARD_DATA_YEAR", "2024"))
DASHBOARD_PROJECTION_HORIZON = int(os.getenv("DASHBOARD_PROJECTION_HORIZON", "5"))
DASHBOARD_MAX_ZIPS = int(os.getenv("DASHBOARD_MAX_ZIPS", "24"))


@dataclass(frozen=True)
class ZctaBboxEntry:
    """Minimal bbox-only record. The full US (~33k entries) fits in ~5MB."""
    zip_code: str
    bounds: tuple[float, float, float, float]


@dataclass(frozen=True)
class DashboardSpatialContext:
    zip_codes: list[str]
    feature_collection: dict[str, Any]
    geometry_source: str
    selection_method: str
    catchment_geometry: Any
    intersection_weights: dict[str, float]


def zcta_cache_status() -> dict[str, Any]:
    """Report cache readiness. Checks for the new per-ZIP directory first, falls back to legacy."""
    new_ready = (ZCTA_CACHE_DIR / "_ready").exists()
    new_index = ZCTA_CACHE_DIR / "bbox_index.json.gz"
    if new_ready and new_index.exists():
        return {
            "path": str(ZCTA_CACHE_DIR),
            "exists": True,
            "size_bytes": new_index.stat().st_size,
            "ready": True,
            "format": "per_zip_directory",
        }
    # Legacy single-file check
    exists = ZCTA_CACHE_PATH.exists()
    size_bytes = ZCTA_CACHE_PATH.stat().st_size if exists else 0
    return {
        "path": str(ZCTA_CACHE_PATH),
        "exists": exists,
        "size_bytes": size_bytes,
        "ready": bool(exists and size_bytes > 0),
        "format": "legacy_single_file" if exists else "none",
    }


# ---------------------------------------------------------------------------
# Memory-efficient ZCTA loading
#
# Permanent memory: ~5MB (bbox index only)
# Per-request memory: ~1-2MB (read ~100 individual 5-15KB gzip files)
# Peak total: <50MB
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_zcta_bbox_index() -> dict[str, ZctaBboxEntry]:
    """Load the bbox index (~200KB). Never touches individual ZIP geometry files."""

    # Try the new per-ZIP directory format first
    index_path = ZCTA_CACHE_DIR / "bbox_index.json.gz"
    if index_path.exists():
        try:
            with gzip.open(index_path, "rt", encoding="utf-8") as fh:
                raw: dict[str, list[float]] = json.load(fh)
            index = {
                zip_code: ZctaBboxEntry(zip_code=zip_code, bounds=(b[0], b[1], b[2], b[3]))
                for zip_code, b in raw.items()
                if len(b) == 4
            }
            logger.info("Loaded ZCTA bbox index from %s: %d entries (~%dKB)",
                        index_path, len(index), index_path.stat().st_size // 1024)
            return index
        except Exception:
            logger.warning("Unable to read ZCTA bbox index from %s", index_path, exc_info=True)

    # Legacy: try the old single-file bbox index (from earlier version of the fix)
    legacy_index = ZCTA_CACHE_PATH.with_name("zcta_bbox_index.json.gz")
    if legacy_index.exists():
        try:
            with gzip.open(legacy_index, "rt", encoding="utf-8") as fh:
                raw = json.load(fh)
            index = {
                zip_code: ZctaBboxEntry(zip_code=zip_code, bounds=(b[0], b[1], b[2], b[3]))
                for zip_code, b in raw.items()
                if len(b) == 4
            }
            logger.info("Loaded legacy ZCTA bbox index from %s: %d entries", legacy_index, len(index))
            return index
        except Exception:
            logger.warning("Unable to read legacy ZCTA bbox index", exc_info=True)

    logger.warning("No ZCTA bbox index found. Run: python -m pipeline.cli ingest-zcta")
    return {}


def _load_features_for_zips(zip_codes: set[str]) -> dict[str, dict]:
    """Load individual per-ZIP GeoJSON files. Each is ~5-15KB gzipped.

    For 100 candidates this reads ~1MB total from disk. No giant file parsing.
    """
    if not zip_codes:
        return {}

    result: dict[str, dict] = {}
    for zip_code in zip_codes:
        zip_file = ZCTA_CACHE_DIR / f"{zip_code}.json.gz"
        if not zip_file.exists():
            continue
        try:
            with gzip.open(zip_file, "rt", encoding="utf-8") as fh:
                feature = json.load(fh)
            result[zip_code] = feature
        except Exception:
            logger.debug("Unable to read ZCTA file for %s", zip_code)
            continue

    logger.debug("Loaded %d of %d requested ZIP features from individual files", len(result), len(zip_codes))
    return result


def _clean_zip(value: Any) -> str | None:
    if value is None:
        return None
    match = re.search(r"\b(\d{5})\b", str(value).strip())
    return match.group(1) if match else None


def _approx_radius_degrees(lat: float, miles: float) -> tuple[float, float]:
    lat_delta = miles / 69.0
    lon_delta = miles / max(12.0, 69.0 * abs(__import__("math").cos(__import__("math").radians(lat))))
    return lat_delta, lon_delta


def _catchment_geometry(result: AnalysisResponse):
    if result.isochrone_polygon:
        try:
            geom = shape(result.isochrone_polygon)
            if not geom.is_empty:
                return geom, "isochrone_intersection"
        except Exception:
            logger.warning("Unable to parse isochrone polygon for dashboard ZIP selection", exc_info=True)

    lat_delta, lon_delta = _approx_radius_degrees(result.lat, result.radius_miles)
    geom = Point(result.lon, result.lat).buffer(max(lat_delta, lon_delta))
    return geom, "radius_intersection"


def _bbox_overlaps(left: tuple[float, float, float, float], right: tuple[float, float, float, float]) -> bool:
    return not (left[2] < right[0] or left[0] > right[2] or left[3] < right[1] or left[1] > right[3])


def _select_zcta_records(result: AnalysisResponse) -> DashboardSpatialContext:
    index = _load_zcta_bbox_index()
    catchment_geom, selection_method = _catchment_geometry(result)
    if not index:
        return DashboardSpatialContext(
            zip_codes=[],
            feature_collection={"type": "FeatureCollection", "features": []},
            geometry_source="cache_unavailable",
            selection_method=selection_method,
            catchment_geometry=catchment_geom,
            intersection_weights={},
        )

    catchment_bounds = tuple(catchment_geom.bounds)

    # Step 1: Cheap bbox filter (~5MB index, instant)
    bbox_candidates = [
        entry for entry in index.values()
        if _bbox_overlaps(entry.bounds, catchment_bounds)
    ]
    logger.debug("ZCTA bbox filter: %d of %d candidates", len(bbox_candidates), len(index))

    if not bbox_candidates:
        return DashboardSpatialContext(
            zip_codes=[],
            feature_collection={"type": "FeatureCollection", "features": []},
            geometry_source="census_zcta_cache",
            selection_method=selection_method,
            catchment_geometry=catchment_geom,
            intersection_weights={},
        )

    # Step 2: Load ONLY the candidate ZIP files from disk (~1MB total)
    candidate_zips = {entry.zip_code for entry in bbox_candidates}
    candidate_features = _load_features_for_zips(candidate_zips)

    # Step 3: Shapely intersection only for the ~100 candidates
    intersections: list[tuple[str, float]] = []
    for entry in bbox_candidates:
        feature = candidate_features.get(entry.zip_code)
        if not feature or not feature.get("geometry"):
            continue
        try:
            geom = shape(feature["geometry"])
        except Exception:
            continue
        if geom.is_empty or not catchment_geom.intersects(geom):
            continue
        try:
            overlap_area = catchment_geom.intersection(geom).area
        except Exception:
            overlap_area = 0.0
        if overlap_area <= 0:
            continue
        intersections.append((entry.zip_code, overlap_area))

    intersections.sort(key=lambda item: item[1], reverse=True)
    intersections = intersections[:DASHBOARD_MAX_ZIPS]

    total_overlap = sum(area for _, area in intersections) or 1.0
    weights = {zip_code: area / total_overlap for zip_code, area in intersections}

    selected_features: list[dict[str, Any]] = []
    for zip_code, _ in intersections:
        feature = json.loads(json.dumps(candidate_features[zip_code]))
        props = feature.setdefault("properties", {})
        props["zipCode"] = zip_code
        props["name"] = props.get("name") or zip_code
        selected_features.append(feature)

    return DashboardSpatialContext(
        zip_codes=[zip_code for zip_code, _ in intersections],
        feature_collection={"type": "FeatureCollection", "features": selected_features},
        geometry_source="census_zcta_cache",
        selection_method=selection_method,
        catchment_geometry=catchment_geom,
        intersection_weights=weights,
    )


# ---------------------------------------------------------------------------
# Everything below is UNCHANGED from the original.
# ---------------------------------------------------------------------------

def _projection_years() -> list[int]:
    return [DASHBOARD_DATA_YEAR + offset for offset in range(1, DASHBOARD_PROJECTION_HORIZON + 1)]


def _series_from_points(points: Iterable[Any]) -> list[DashboardTimeSeriesPoint]:
    return [
        DashboardTimeSeriesPoint(
            year=int(point.year),
            value=round(float(point.value), 2),
            projected=bool(point.projected),
            lower_bound=None if getattr(point, "lower_bound", None) is None else round(float(point.lower_bound), 2),
            upper_bound=None if getattr(point, "upper_bound", None) is None else round(float(point.upper_bound), 2),
            label="Projected" if getattr(point, "projected", False) else "Historical",
        )
        for point in points
    ]


def _build_projection_series(history: dict[int, float], current_year: int, current_value: float) -> list[DashboardTimeSeriesPoint]:
    points = [HistoricalValue(year=year, value=value) for year, value in sorted(history.items()) if value is not None and value >= 0]
    if current_year not in history and current_value >= 0:
        points.append(HistoricalValue(year=current_year, value=current_value))
    envelope = build_projection_envelope(points, _projection_years())
    return _series_from_points(envelope.points)


def _metric(label: str, current: float, projected: float, *, fmt: str = "number", invert_change: bool = False) -> DashboardDrilldownMetric:
    return DashboardDrilldownMetric(
        label=label,
        current=round(float(current), 2),
        projected=round(float(projected), 2),
        format=fmt,
        invert_change=invert_change,
    )


def _normalize_distribution(distribution: list[tuple[str, float]], current_total: float, projected_total: float) -> list[DashboardDistributionBucket]:
    base_total = sum(value for _, value in distribution) or 1.0
    projected_ratio = projected_total / max(current_total, 1.0)
    return [
        DashboardDistributionBucket(
            bucket=label,
            primary=round(value, 2),
            comparison=round(value * projected_ratio, 2),
        )
        for label, value in distribution
    ]


def _current_distribution_from_result(result: AnalysisResponse) -> list[tuple[str, float]]:
    raw = getattr(result.demographics, "income_distribution", None) or []
    if raw:
        labels = ["<$25K", "$25K-$50K", "$50K-$75K", "$75K-$100K", "$100K-$150K", "$150K-$200K", "$200K+"]
        buckets = [0.0 for _ in labels]
        for midpoint, count in raw:
            if midpoint < 25_000:
                buckets[0] += count
            elif midpoint < 50_000:
                buckets[1] += count
            elif midpoint < 75_000:
                buckets[2] += count
            elif midpoint < 100_000:
                buckets[3] += count
            elif midpoint < 150_000:
                buckets[4] += count
            elif midpoint < 200_000:
                buckets[5] += count
            else:
                buckets[6] += count
        return list(zip(labels, buckets))

    households = float(result.demographics.total_households or 0)
    return [
        ("<$25K", households * 0.12),
        ("$25K-$50K", households * 0.18),
        ("$50K-$75K", households * 0.18),
        ("$75K-$100K", households * 0.16),
        ("$100K-$150K", households * 0.18),
        ("$150K-$200K", households * 0.10),
        ("$200K+", households * 0.08),
    ]


def _init_zip_row() -> dict[str, float]:
    return {
        "total_population": 0.0,
        "total_households": 0.0,
        "school_age_population": 0.0,
        "families_with_children": 0.0,
        "median_household_income_weighted_sum": 0.0,
        "median_household_income_weight": 0.0,
        "median_family_income_weighted_sum": 0.0,
        "median_family_income_weight": 0.0,
        "high_income_households": 0.0,
        "enrolled_k_12": 0.0,
        "enrolled_private_k_12": 0.0,
        "seniors_65_plus": 0.0,
        "seniors_75_plus": 0.0,
        "seniors_living_alone": 0.0,
        "renter_households": 0.0,
        "cost_burdened_renter_households": 0.0,
        "hud_eligible_households": 0.0,
        "competitor_count": 0.0,
        "competitor_units": 0.0,
        "competitor_rating_sum": 0.0,
        "competitor_rating_count": 0.0,
    }


async def _load_db_aggregates(result: AnalysisResponse, spatial: DashboardSpatialContext) -> tuple[dict[str, dict[str, float]], dict[str, dict[int, dict[str, float]]], int | None]:
    if not spatial.zip_codes:
        return {}, {}, None

    try:
        from geoalchemy2.shape import to_shape
        from sqlalchemy import select

        from db.connection import get_session
        from db.models import CensusTractHistory
        from db.queries import get_tracts_in_catchment
    except Exception:
        logger.info("Dashboard DB aggregates unavailable; falling back to spatial weighting", exc_info=True)
        return {}, {}, None

    try:
        async with get_session() as session:
            tracts = await get_tracts_in_catchment(
                session,
                lat=result.lat,
                lon=result.lon,
                radius_miles=result.radius_miles,
                isochrone_geojson=result.isochrone_polygon,
            )
            if not tracts:
                return {}, {}, None

            prepared = {zip_code: prep(shape(feature["geometry"])) for zip_code, feature in ((feature["properties"]["zipCode"], feature) for feature in spatial.feature_collection["features"])}
            current_by_zip: dict[str, dict[str, float]] = {zip_code: _init_zip_row() for zip_code in spatial.zip_codes}
            tract_to_zip: dict[str, str] = {}
            data_year: int | None = None

            for tract in tracts:
                point = None
                if tract.centroid is not None:
                    try:
                        point = to_shape(tract.centroid)
                    except Exception:
                        point = None
                if point is None and tract.boundary is not None:
                    try:
                        point = to_shape(tract.boundary).representative_point()
                    except Exception:
                        point = None
                if point is None:
                    continue

                assigned_zip = None
                for zip_code, prepared_geom in prepared.items():
                    if prepared_geom.contains(point) or prepared_geom.intersects(point):
                        assigned_zip = zip_code
                        break
                if not assigned_zip:
                    continue

                tract_to_zip[tract.geoid] = assigned_zip
                row = current_by_zip[assigned_zip]
                pop = float(tract.total_population or 0)
                households = float(tract.total_households or 0)
                row["total_population"] += pop
                row["total_households"] += households
                row["school_age_population"] += float(tract.population_5_17 or 0)
                row["families_with_children"] += float(tract.families_with_own_children or 0)
                row["high_income_households"] += float((tract.income_bracket_100k_150k or 0) + (tract.income_bracket_150k_200k or 0) + (tract.income_bracket_200k_plus or 0))
                row["enrolled_k_12"] += float(tract.enrolled_k_12 or 0)
                row["enrolled_private_k_12"] += float(tract.enrolled_private_k_12 or 0)
                row["seniors_65_plus"] += float((tract.population_65_74 or 0) + (tract.population_75_plus or 0))
                row["seniors_75_plus"] += float(tract.population_75_plus or 0)
                row["seniors_living_alone"] += float(tract.seniors_living_alone or 0)
                row["renter_households"] += float(tract.renter_households_b25070 or tract.renter_occupied or 0)
                row["cost_burdened_renter_households"] += float(tract.cost_burdened_renter_households or 0)
                row["hud_eligible_households"] += float((tract.income_bracket_under_10k or 0) + (tract.income_bracket_10k_15k or 0) + (tract.income_bracket_15k_25k or 0) + (tract.income_bracket_25k_35k or 0) + (tract.income_bracket_35k_50k or 0))
                if tract.median_household_income:
                    row["median_household_income_weighted_sum"] += float(tract.median_household_income) * max(pop, 1.0)
                    row["median_household_income_weight"] += max(pop, 1.0)
                if tract.median_family_income:
                    row["median_family_income_weighted_sum"] += float(tract.median_family_income) * max(households, 1.0)
                    row["median_family_income_weight"] += max(households, 1.0)
                try:
                    data_year = max(data_year or 0, int(tract.acs_vintage or 0))
                except Exception:
                    pass

            for competitor in result.competitor_schools:
                zip_code = _clean_zip(competitor.zip_code)
                if not zip_code or zip_code not in current_by_zip:
                    continue
                row = current_by_zip[zip_code]
                row["competitor_count"] += 1
                row["competitor_units"] += float(competitor.total_units or competitor.enrollment or 0)
                if competitor.mds_overall_rating is not None:
                    row["competitor_rating_sum"] += float(competitor.mds_overall_rating)
                    row["competitor_rating_count"] += 1

            if not tract_to_zip:
                return {}, {}, data_year

            history_stmt = select(CensusTractHistory).where(CensusTractHistory.geoid.in_(list(tract_to_zip.keys())))
            history_rows = list((await session.execute(history_stmt)).scalars().all())
            history_by_zip: dict[str, dict[int, dict[str, float]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
            for hist in history_rows:
                zip_code = tract_to_zip.get(hist.geoid)
                if not zip_code:
                    continue
                try:
                    year = int(hist.acs_vintage)
                except Exception:
                    continue
                bucket = history_by_zip[zip_code][year]
                pop = float(hist.total_population or 0)
                households = float(hist.total_households or 0)
                bucket["total_population"] += pop
                bucket["total_households"] += households
                bucket["school_age_population"] += float(hist.population_5_17 or 0)
                bucket["families_with_children"] += float(hist.families_with_own_children or 0)
                bucket["seniors_65_plus"] += float((hist.population_65_74 or 0) + (hist.population_75_plus or 0))
                bucket["seniors_75_plus"] += float(hist.population_75_plus or 0)
                if hist.median_household_income:
                    bucket["median_household_income_weighted_sum"] += float(hist.median_household_income) * max(pop, 1.0)
                    bucket["median_household_income_weight"] += max(pop, 1.0)

            return current_by_zip, {zip_code: dict(years) for zip_code, years in history_by_zip.items()}, data_year
    except Exception:
        logger.warning("Dashboard DB aggregate load failed; falling back to spatial weighting", exc_info=True)
        return {}, {}, None


def _weighted_income(row: dict[str, float], prefix: str) -> float:
    weight = row.get(f"{prefix}_weight", 0.0)
    if weight <= 0:
        return 0.0
    return row.get(f"{prefix}_weighted_sum", 0.0) / weight


def _area_weighted_fallback(result: AnalysisResponse, spatial: DashboardSpatialContext) -> tuple[dict[str, dict[str, float]], dict[str, dict[int, dict[str, float]]], int]:
    current_by_zip: dict[str, dict[str, float]] = {zip_code: _init_zip_row() for zip_code in spatial.zip_codes}
    households = float(result.demographics.total_households or 0)
    population = float(result.demographics.total_population or 0)
    school_age = float(result.demographics.school_age_population or 0)
    families = float(result.demographics.families_with_children or 0)
    seniors65 = float(result.demographics.seniors_65_plus or 0)
    seniors75 = float(result.demographics.seniors_75_plus or 0)
    seniors_alone = float(result.demographics.seniors_living_alone or 0)
    renters = float(result.demographics.renter_households or 0)
    cost_burdened = float(result.demographics.cost_burdened_renter_households or 0)
    hud_eligible = float(result.demographics.hud_eligible_households or 0)
    median_income = float(result.demographics.median_household_income or 0)
    high_income = float(result.demographics.income_qualified_base or result.demographics.total_addressable_market or 0)

    for zip_code in spatial.zip_codes:
        weight = spatial.intersection_weights.get(zip_code, 0.0)
        row = current_by_zip[zip_code]
        row["total_population"] = population * weight
        row["total_households"] = households * weight
        row["school_age_population"] = school_age * weight
        row["families_with_children"] = families * weight
        row["seniors_65_plus"] = seniors65 * weight
        row["seniors_75_plus"] = seniors75 * weight
        row["seniors_living_alone"] = seniors_alone * weight
        row["renter_households"] = renters * weight
        row["cost_burdened_renter_households"] = cost_burdened * weight
        row["hud_eligible_households"] = hud_eligible * weight
        row["high_income_households"] = high_income * weight
        row["median_household_income_weighted_sum"] = median_income * max(population * weight, 1.0)
        row["median_household_income_weight"] = max(population * weight, 1.0)
        row["median_family_income_weighted_sum"] = median_income * max(households * weight, 1.0)
        row["median_family_income_weight"] = max(households * weight, 1.0)

    for competitor in result.competitor_schools:
        zip_code = _clean_zip(competitor.zip_code)
        if not zip_code or zip_code not in current_by_zip:
            continue
        row = current_by_zip[zip_code]
        row["competitor_count"] += 1
        row["competitor_units"] += float(competitor.total_units or competitor.enrollment or 0)
        if competitor.mds_overall_rating is not None:
            row["competitor_rating_sum"] += float(competitor.mds_overall_rating)
            row["competitor_rating_count"] += 1

    history_by_zip: dict[str, dict[int, dict[str, float]]] = defaultdict(dict)
    for zip_code in spatial.zip_codes:
        weight = spatial.intersection_weights.get(zip_code, 0.0)
        row = current_by_zip[zip_code]
        current_families = row["families_with_children"]
        current_school_age = row["school_age_population"]
        base_income = _weighted_income(row, "median_household_income")
        history_by_zip[zip_code] = {
            2019: {
                "families_with_children": current_families * 0.92,
                "school_age_population": current_school_age * 0.93,
                "seniors_65_plus": row["seniors_65_plus"] * 0.94,
                "seniors_75_plus": row["seniors_75_plus"] * 0.93,
                "total_households": row["total_households"] * 0.95,
                "median_household_income_weighted_sum": base_income * max(row["total_population"] * 0.94, 1.0),
                "median_household_income_weight": max(row["total_population"] * 0.94, 1.0),
            },
            2021: {
                "families_with_children": current_families * 0.97,
                "school_age_population": current_school_age * 0.98,
                "seniors_65_plus": row["seniors_65_plus"] * 0.98,
                "seniors_75_plus": row["seniors_75_plus"] * 0.98,
                "total_households": row["total_households"] * 0.98,
                "median_household_income_weighted_sum": base_income * max(row["total_population"] * 0.98, 1.0),
                "median_household_income_weight": max(row["total_population"] * 0.98, 1.0),
            },
        }
    return current_by_zip, history_by_zip, 2022


def _history_metric(history_by_zip: dict[str, dict[int, dict[str, float]]], zip_codes: list[str], key: str) -> dict[int, float]:
    totals: dict[int, float] = defaultdict(float)
    for zip_code in zip_codes:
        for year, metrics in history_by_zip.get(zip_code, {}).items():
            if key == "median_household_income":
                weight = metrics.get("median_household_income_weight", 0.0)
                if weight > 0:
                    totals[year] += metrics.get("median_household_income_weighted_sum", 0.0)
            else:
                totals[year] += metrics.get(key, 0.0)
    if key != "median_household_income":
        return dict(totals)

    weights: dict[int, float] = defaultdict(float)
    for zip_code in zip_codes:
        for year, metrics in history_by_zip.get(zip_code, {}).items():
            weights[year] += metrics.get("median_household_income_weight", 0.0)
    return {year: (totals[year] / weights[year]) for year in totals if weights[year] > 0}


def _zip_distribution(base_distribution: list[tuple[str, float]], weight: float, current_total: float, projected_total: float) -> list[DashboardDistributionBucket]:
    return _normalize_distribution([(label, value * weight) for label, value in base_distribution], current_total, projected_total)


def _build_schools_payload(zip_codes: list[str], current_by_zip: dict[str, dict[str, float]], history_by_zip: dict[str, dict[int, dict[str, float]]], result: AnalysisResponse, data_year: int) -> DashboardModuleData:
    metric_maps = {key: {} for key in ("schoolAgePopulation", "familiesWithChildren", "medianFamilyIncome", "competitorCount")}
    drilldowns: dict[str, DashboardZipDrilldown] = {}
    base_distribution = _current_distribution_from_result(result)

    for zip_code in zip_codes:
        row = current_by_zip[zip_code]
        median_family_income = _weighted_income(row, "median_family_income") or _weighted_income(row, "median_household_income")
        metric_maps["schoolAgePopulation"][zip_code] = round(row["school_age_population"], 2)
        metric_maps["familiesWithChildren"][zip_code] = round(row["families_with_children"], 2)
        metric_maps["medianFamilyIncome"][zip_code] = round(median_family_income, 2)
        metric_maps["competitorCount"][zip_code] = round(row["competitor_count"], 2)

        projected_families_series = _build_projection_series({year: values.get("families_with_children", 0.0) for year, values in history_by_zip.get(zip_code, {}).items()}, data_year, row["families_with_children"])
        projected_families = projected_families_series[-1].value if projected_families_series else row["families_with_children"]
        projected_income = median_family_income * 1.06
        projected_school_age = _build_projection_series({year: values.get("school_age_population", 0.0) for year, values in history_by_zip.get(zip_code, {}).items()}, data_year, row["school_age_population"])[-1].value if history_by_zip.get(zip_code) else row["school_age_population"]

        drilldowns[zip_code] = DashboardZipDrilldown(
            zip_code=zip_code,
            place_label=result.county_name,
            summary="This ZIP highlights family demand, income capacity, and competitor context within the selected school market.",
            current_year=data_year,
            projected_year=_projection_years()[-1],
            metrics=[
                _metric("School-Age Population", row["school_age_population"], projected_school_age),
                _metric("Families with Children", row["families_with_children"], projected_families),
                _metric("Median Family Income", median_family_income, projected_income, fmt="currency"),
                _metric("Nearby Competitors", row["competitor_count"], row["competitor_count"]),
            ],
            distribution=_zip_distribution(base_distribution, max(row["total_households"] / max(float(result.demographics.total_households or 1), 1.0), 0.0), row["total_households"], row["total_households"] * 1.04),
        )

    catchment_income = _history_metric(history_by_zip, zip_codes, "median_household_income")
    family_history = _history_metric(history_by_zip, zip_codes, "families_with_children")
    school_age_history = _history_metric(history_by_zip, zip_codes, "school_age_population")
    current_families = sum(row["families_with_children"] for row in current_by_zip.values())
    current_school_age = sum(row["school_age_population"] for row in current_by_zip.values())
    current_income = _weighted_income(
        {
            "median_family_income_weighted_sum": sum(row["median_family_income_weighted_sum"] for row in current_by_zip.values()),
            "median_family_income_weight": sum(row["median_family_income_weight"] for row in current_by_zip.values()),
            "median_household_income_weighted_sum": sum(row["median_household_income_weighted_sum"] for row in current_by_zip.values()),
            "median_household_income_weight": sum(row["median_household_income_weight"] for row in current_by_zip.values()),
        },
        "median_family_income",
    ) or float(result.demographics.median_household_income or 0)

    distribution = _normalize_distribution(base_distribution, float(result.demographics.total_households or 0), float(result.demographics.total_households or 0) * 1.04)

    return DashboardModuleData(
        slug="schools",
        label="Schools",
        eyebrow="Schools dashboard",
        title="School Market View",
        description="See where family growth, school-age population, and competitor pressure are strongest across the catchment.",
        primary_label="Focus",
        primary_value="Market overview",
        secondary_label="ZIPs",
        secondary_value=str(len(zip_codes)),
        sidebar_items=[
            DashboardSidebarItem(key="market_overview", title="Market Overview", description="Population, income, and addressable-market context.", badge="Core"),
            DashboardSidebarItem(key="competitors", title="Competitors", description="Nearby competitor counts and enrollment pressure by ZIP."),
        ],
        tabs=[
            DashboardTabItem(key="summary", label="Summary"),
            DashboardTabItem(key="distribution", label="Distribution"),
            DashboardTabItem(key="projections", label="Projections"),
            DashboardTabItem(key="map_view", label="Map View"),
            DashboardTabItem(key="drilldown", label="ZIP Drilldown"),
        ],
        metric_options=[
            DashboardMetricOption(key="schoolAgePopulation", label="School-Age Population", format="number"),
            DashboardMetricOption(key="familiesWithChildren", label="Families with Children", format="number"),
            DashboardMetricOption(key="medianFamilyIncome", label="Median Family Income", format="currency"),
            DashboardMetricOption(key="competitorCount", label="Nearby Competitors", format="number"),
        ],
        metric_maps=metric_maps,
        trend_title="Historical and projected school-market depth",
        trend_subtitle="Projected values carry confidence bounds and remain visually distinct from observed history.",
        trend_series=[
            DashboardSeriesDescriptor(key="familiesWithChildren", label="Families with Children", color="#2563eb", format="number"),
            DashboardSeriesDescriptor(key="schoolAgePopulation", label="School-Age Population", color="#7c3aed", format="number"),
            DashboardSeriesDescriptor(key="medianFamilyIncome", label="Median Family Income", color="#16a34a", format="currency"),
        ],
        time_series={
            "familiesWithChildren": _build_projection_series(family_history, data_year, current_families),
            "schoolAgePopulation": _build_projection_series(school_age_history, data_year, current_school_age or float(result.demographics.school_age_population or 0)),
            "medianFamilyIncome": _build_projection_series(catchment_income, data_year, current_income),
        },
        distribution_title="Catchment household income distribution",
        distribution_subtitle="Income buckets show where tuition-paying capacity is concentrated today and where it is projected to deepen next.",
        distribution=distribution,
        drilldowns=drilldowns,
        highlight_cards=[
            DashboardViewCard(label="ZIPs in catchment", value=str(len(zip_codes)), detail="Selected by actual catchment/ZCTA intersection."),
            DashboardViewCard(label="School-age population", value=f"{int(current_school_age):,}", detail="Current school-age population across the selected ZIPs."),
            DashboardViewCard(label="Families with children", value=f"{int(current_families):,}", detail="Current catchment total feeding school-market views."),
            DashboardViewCard(label="Median family income", value=f"${int(current_income):,}", detail="Weighted ZIP-level family income across the market area."),
        ],
    )


def _build_elder_payload(zip_codes: list[str], current_by_zip: dict[str, dict[str, float]], history_by_zip: dict[str, dict[int, dict[str, float]]], result: AnalysisResponse, data_year: int) -> DashboardModuleData:
    metric_maps = {key: {} for key in ("seniors65Plus", "seniors75Plus", "medianSeniorIncome", "facilityCount")}
    drilldowns: dict[str, DashboardZipDrilldown] = {}
    base_distribution = _current_distribution_from_result(result)

    for zip_code in zip_codes:
        row = current_by_zip[zip_code]
        median_income = _weighted_income(row, "median_household_income")
        avg_rating = row["competitor_rating_sum"] / row["competitor_rating_count"] if row["competitor_rating_count"] else 0.0
        metric_maps["seniors65Plus"][zip_code] = round(row["seniors_65_plus"], 2)
        metric_maps["seniors75Plus"][zip_code] = round(row["seniors_75_plus"], 2)
        metric_maps["medianSeniorIncome"][zip_code] = round(median_income, 2)
        metric_maps["facilityCount"][zip_code] = round(row["competitor_count"], 2)
        series65 = _build_projection_series({year: values.get("seniors_65_plus", 0.0) for year, values in history_by_zip.get(zip_code, {}).items()}, data_year, row["seniors_65_plus"])
        projected65 = series65[-1].value if series65 else row["seniors_65_plus"]
        series75 = _build_projection_series({year: values.get("seniors_75_plus", 0.0) for year, values in history_by_zip.get(zip_code, {}).items()}, data_year, row["seniors_75_plus"])
        projected75 = series75[-1].value if series75 else row["seniors_75_plus"]
        drilldowns[zip_code] = DashboardZipDrilldown(
            zip_code=zip_code,
            place_label=result.county_name,
            summary="This ZIP highlights senior concentration, living-alone risk, and local care-market quality context.",
            current_year=data_year,
            projected_year=_projection_years()[-1],
            metrics=[
                _metric("Seniors 65+", row["seniors_65_plus"], projected65),
                _metric("Seniors 75+", row["seniors_75_plus"], projected75),
                _metric("Seniors Living Alone", row["seniors_living_alone"], row["seniors_living_alone"] * 1.03),
                _metric("Median Senior HH Income", median_income, median_income * 1.05, fmt="currency"),
                _metric("Facilities", row["competitor_count"], row["competitor_count"]),
                _metric("Average Rating", avg_rating, avg_rating, fmt="number"),
            ],
            distribution=_zip_distribution(base_distribution, max(row["total_households"] / max(float(result.demographics.total_households or 1), 1.0), 0.0), row["total_households"], row["total_households"] * 1.03),
        )

    seniors65_history = _history_metric(history_by_zip, zip_codes, "seniors_65_plus")
    seniors75_history = _history_metric(history_by_zip, zip_codes, "seniors_75_plus")
    median_income_history = _history_metric(history_by_zip, zip_codes, "median_household_income")

    return DashboardModuleData(
        slug="elder-care",
        label="Elder Care",
        eyebrow="Elder care dashboard",
        title="Elder Care Market View",
        description="Focus on the ZIPs with the largest senior populations, the fastest cohort growth, and the clearest care-market gaps.",
        primary_label="Focus",
        primary_value="Community profile",
        secondary_label="ZIPs",
        secondary_value=str(len(zip_codes)),
        sidebar_items=[
            DashboardSidebarItem(key="community_profile", title="Community Profile", description="Senior population and income context.", badge="Core"),
            DashboardSidebarItem(key="market_landscape", title="Market Landscape", description="Facilities, ratings, and capacity context by ZIP."),
        ],
        tabs=[
            DashboardTabItem(key="summary", label="Summary"),
            DashboardTabItem(key="distribution", label="Distribution"),
            DashboardTabItem(key="projections", label="Projections"),
            DashboardTabItem(key="map_view", label="Map View"),
            DashboardTabItem(key="drilldown", label="ZIP Drilldown"),
        ],
        metric_options=[
            DashboardMetricOption(key="seniors65Plus", label="Seniors 65+", format="number"),
            DashboardMetricOption(key="seniors75Plus", label="Seniors 75+", format="number"),
            DashboardMetricOption(key="medianSeniorIncome", label="Senior Household Income", format="currency"),
            DashboardMetricOption(key="facilityCount", label="Facilities", format="number"),
        ],
        metric_maps=metric_maps,
        trend_title="Senior cohort history and projection",
        trend_subtitle="Projected values are bounded and labeled so they are never presented as observed fact.",
        trend_series=[
            DashboardSeriesDescriptor(key="seniors65Plus", label="Seniors 65+", color="#2563eb", format="number"),
            DashboardSeriesDescriptor(key="seniors75Plus", label="Seniors 75+", color="#16a34a", format="number"),
            DashboardSeriesDescriptor(key="medianSeniorIncome", label="Median Senior HH Income", color="#7c3aed", format="currency"),
        ],
        time_series={
            "seniors65Plus": _build_projection_series(seniors65_history, data_year, float(result.demographics.seniors_65_plus or 0)),
            "seniors75Plus": _build_projection_series(seniors75_history, data_year, float(result.demographics.seniors_75_plus or 0)),
            "medianSeniorIncome": _build_projection_series(median_income_history, data_year, float(result.demographics.median_household_income or 0)),
        },
        distribution_title="Senior-household income context",
        distribution_subtitle="Income buckets help distinguish mission-sensitive demand from private-pay capacity across the catchment.",
        distribution=_normalize_distribution(base_distribution, float(result.demographics.total_households or 0), float(result.demographics.total_households or 0) * 1.03),
        drilldowns=drilldowns,
        highlight_cards=[
            DashboardViewCard(label="ZIPs in catchment", value=str(len(zip_codes)), detail="Selected by actual catchment/ZCTA intersection."),
            DashboardViewCard(label="Seniors 65+", value=f"{int(sum(row['seniors_65_plus'] for row in current_by_zip.values())):,}", detail="Current catchment-wide cohort total."),
            DashboardViewCard(label="Facilities", value=f"{int(sum(row['competitor_count'] for row in current_by_zip.values()))}", detail="Facilities with ZIPs falling inside the selected dashboard layer."),
        ],
    )


def _build_housing_payload(zip_codes: list[str], current_by_zip: dict[str, dict[str, float]], history_by_zip: dict[str, dict[int, dict[str, float]]], result: AnalysisResponse, data_year: int) -> DashboardModuleData:
    metric_maps = {key: {} for key in ("totalPopulation", "costBurdenedHouseholds", "costBurdenRate", "renterHouseholds", "hudEligibleHouseholds", "medianHouseholdIncome")}
    drilldowns: dict[str, DashboardZipDrilldown] = {}
    base_distribution = _current_distribution_from_result(result)

    for zip_code in zip_codes:
        row = current_by_zip[zip_code]
        median_income = _weighted_income(row, "median_household_income")
        burden_rate = (row["cost_burdened_renter_households"] / max(row["renter_households"], 1.0)) * 100.0
        metric_maps["totalPopulation"][zip_code] = round(row["total_population"], 2)
        metric_maps["costBurdenedHouseholds"][zip_code] = round(row["cost_burdened_renter_households"], 2)
        metric_maps["costBurdenRate"][zip_code] = round(burden_rate, 2)
        metric_maps["renterHouseholds"][zip_code] = round(row["renter_households"], 2)
        metric_maps["hudEligibleHouseholds"][zip_code] = round(row["hud_eligible_households"], 2)
        metric_maps["medianHouseholdIncome"][zip_code] = round(median_income, 2)
        burden_history = {year: values.get("total_households", 0.0) * (row["cost_burdened_renter_households"] / max(row["total_households"], 1.0)) for year, values in history_by_zip.get(zip_code, {}).items()}
        projected_burden = _build_projection_series(burden_history, data_year, row["cost_burdened_renter_households"])[-1].value if burden_history else row["cost_burdened_renter_households"]
        projected_hud = row["hud_eligible_households"] * 1.03
        drilldowns[zip_code] = DashboardZipDrilldown(
            zip_code=zip_code,
            place_label=result.county_name,
            summary="This ZIP highlights renter need, cost burden, and income thresholds within the housing market.",
            current_year=data_year,
            projected_year=_projection_years()[-1],
            metrics=[
                _metric("Cost-Burdened Households", row["cost_burdened_renter_households"], projected_burden),
                _metric("Renter Households", row["renter_households"], row["renter_households"] * 1.02),
                _metric("Cost-Burden Rate", burden_rate, burden_rate, fmt="percent"),
                _metric("HUD-Eligible Households", row["hud_eligible_households"], projected_hud),
                _metric("Median Household Income", median_income, median_income * 1.05, fmt="currency"),
            ],
            distribution=_zip_distribution(base_distribution, max(row["total_households"] / max(float(result.demographics.total_households or 1), 1.0), 0.0), row["total_households"], row["total_households"] * 1.02),
        )

    households_history = _history_metric(history_by_zip, zip_codes, "total_households")
    current_burden_ratio = sum(row["cost_burdened_renter_households"] for row in current_by_zip.values()) / max(sum(row["total_households"] for row in current_by_zip.values()), 1.0)
    current_hud_ratio = sum(row["hud_eligible_households"] for row in current_by_zip.values()) / max(sum(row["total_households"] for row in current_by_zip.values()), 1.0)
    burden_history = {year: value * current_burden_ratio for year, value in households_history.items()}
    hud_history = {year: value * current_hud_ratio for year, value in households_history.items()}
    income_history = _history_metric(history_by_zip, zip_codes, "median_household_income")

    return DashboardModuleData(
        slug="housing",
        label="Housing",
        eyebrow="Housing dashboard",
        title="Housing Market View",
        description="See where renter demand, cost burden, and income-qualified need are concentrated across the catchment.",
        primary_label="Focus",
        primary_value="Need assessment",
        secondary_label="ZIPs",
        secondary_value=str(len(zip_codes)),
        sidebar_items=[
            DashboardSidebarItem(key="community_profile", title="Community Profile", description="Household income, burden, and demand context.", badge="Core"),
            DashboardSidebarItem(key="need_assessment", title="Need Assessment", description="Burdened and HUD-eligible households by ZIP."),
        ],
        tabs=[
            DashboardTabItem(key="summary", label="Summary"),
            DashboardTabItem(key="distribution", label="Distribution"),
            DashboardTabItem(key="projections", label="Projections"),
            DashboardTabItem(key="map_view", label="Map View"),
            DashboardTabItem(key="drilldown", label="ZIP Drilldown"),
        ],
        metric_options=[
            DashboardMetricOption(key="totalPopulation", label="Total Population", format="number"),
            DashboardMetricOption(key="costBurdenedHouseholds", label="Cost-Burdened Households", format="number"),
            DashboardMetricOption(key="costBurdenRate", label="Cost-Burden Rate", format="percent"),
            DashboardMetricOption(key="renterHouseholds", label="Renter Households", format="number"),
            DashboardMetricOption(key="hudEligibleHouseholds", label="HUD-Eligible Households", format="number"),
            DashboardMetricOption(key="medianHouseholdIncome", label="Median Household Income", format="currency"),
        ],
        metric_maps=metric_maps,
        trend_title="Housing need history and projection",
        trend_subtitle="Projected lines are bounded and labeled to preserve the tool's directional planning framing.",
        trend_series=[
            DashboardSeriesDescriptor(key="costBurdenedHouseholds", label="Cost-Burdened Households", color="#dc2626", format="number"),
            DashboardSeriesDescriptor(key="hudEligibleHouseholds", label="HUD-Eligible Households", color="#2563eb", format="number"),
            DashboardSeriesDescriptor(key="medianHouseholdIncome", label="Median Household Income", color="#7c3aed", format="currency"),
        ],
        time_series={
            "costBurdenedHouseholds": _build_projection_series(burden_history, data_year, float(result.demographics.cost_burdened_renter_households or 0)),
            "hudEligibleHouseholds": _build_projection_series(hud_history, data_year, float(result.demographics.hud_eligible_households or 0)),
            "medianHouseholdIncome": _build_projection_series(income_history, data_year, float(result.demographics.median_household_income or 0)),
        },
        distribution_title="Household income distribution",
        distribution_subtitle="Income buckets show how affordability pressure and eligible-household depth vary across the market.",
        distribution=_normalize_distribution(base_distribution, float(result.demographics.total_households or 0), float(result.demographics.total_households or 0) * 1.02),
        drilldowns=drilldowns,
        highlight_cards=[
            DashboardViewCard(label="ZIPs in catchment", value=str(len(zip_codes)), detail="Selected by actual catchment/ZCTA intersection."),
            DashboardViewCard(label="Total population", value=f"{int(sum(row['total_population'] for row in current_by_zip.values())):,}", detail="Current population represented by the selected ZIP layer."),
            DashboardViewCard(label="Renter households", value=f"{int(sum(row['renter_households'] for row in current_by_zip.values())):,}", detail="Current renter base across the catchment."),
            DashboardViewCard(label="Burden rate", value=f"{((sum(row['cost_burdened_renter_households'] for row in current_by_zip.values()) / max(sum(row['renter_households'] for row in current_by_zip.values()), 1.0)) * 100):.1f}%", detail="Share of renter households currently facing cost burden."),
        ],
    )


def _apply_metric_properties(spatial: DashboardSpatialContext, metric_maps: dict[str, dict[str, float]]) -> dict[str, Any]:
    feature_collection = json.loads(json.dumps(spatial.feature_collection))
    for feature in feature_collection.get("features", []):
        props = feature.setdefault("properties", {})
        zip_code = str(props.get("zipCode") or "")
        for metric_key, values in metric_maps.items():
            if zip_code in values:
                props[metric_key] = values[zip_code]
    return feature_collection


async def build_dashboard_response(*, request: AnalysisRequest, result: AnalysisResponse, location: dict[str, Any]) -> DashboardResponse:
    spatial = _select_zcta_records(result)
    current_by_zip, history_by_zip, data_year = await _load_db_aggregates(result, spatial)
    if not current_by_zip:
        current_by_zip, history_by_zip, fallback_year = _area_weighted_fallback(result, spatial)
        data_year = data_year or fallback_year

    if result.ministry_type == "elder_care":
        module_data = _build_elder_payload(spatial.zip_codes, current_by_zip, history_by_zip, result, data_year or DASHBOARD_DATA_YEAR)
    elif result.ministry_type == "housing":
        module_data = _build_housing_payload(spatial.zip_codes, current_by_zip, history_by_zip, result, data_year or DASHBOARD_DATA_YEAR)
    else:
        module_data = _build_schools_payload(spatial.zip_codes, current_by_zip, history_by_zip, result, data_year or DASHBOARD_DATA_YEAR)

    feature_collection = _apply_metric_properties(spatial, module_data.metric_maps)

    freshness = result.data_freshness.model_dump() if result.data_freshness else None
    freshness_statuses = {source.status for source in (result.data_freshness.sources if result.data_freshness else [])}
    if freshness_statuses == {"fresh"}:
        confidence_band = "high"
    elif "stale" in freshness_statuses:
        confidence_band = "low"
    else:
        confidence_band = "medium"

    metadata = DashboardMetadata(
        data_year=int(data_year or DASHBOARD_DATA_YEAR),
        projection_years=_projection_years(),
        last_updated=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        confidence_band=confidence_band,
        projection_label="Projected values are directional planning estimates with confidence bounds; they are not observed outcomes.",
        geometry_source=spatial.geometry_source,
        freshness=freshness,
    )
    metadata.freshness = {**(metadata.freshness or {}), "zipSelectionMethod": spatial.selection_method, "zipCount": len(spatial.zip_codes)}

    return DashboardResponse(
        catchment=DashboardCatchment(
            center={"lat": result.lat, "lng": result.lon, "address": location.get("matched_address") or request.address},
            drive_time_minutes=request.drive_minutes,
            zip_codes=spatial.zip_codes,
            geojson=feature_collection,
        ),
        data=module_data,
        metadata=metadata,
    )
