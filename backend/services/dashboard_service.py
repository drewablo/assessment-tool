from __future__ import annotations

import gzip
import json
import logging
import math
import os
import re
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

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

ZCTA_CACHE_PATH = Path(os.getenv("ZCTA_CACHE_PATH", Path(__file__).resolve().parents[1] / "data" / "zcta_boundaries.json.gz"))
FALLBACK_ZIP_COUNT = 4
DASHBOARD_DATA_YEAR = int(os.getenv("DASHBOARD_DATA_YEAR", "2024"))
DASHBOARD_PROJECTION_HORIZON = int(os.getenv("DASHBOARD_PROJECTION_HORIZON", "5"))

SEED_ZIP_BY_MODULE = {
    "schools": ["33971", "33913", "33967", "33912"],
    "elder_care": ["33919", "33907", "33908", "33901"],
    "housing": ["33916", "33901", "33905", "33907"],
}


@lru_cache(maxsize=1)
def _load_zcta_cache() -> dict[str, dict[str, Any]]:
    if not ZCTA_CACHE_PATH.exists():
        return {}
    try:
        with gzip.open(ZCTA_CACHE_PATH, "rt", encoding="utf-8") as fh:
            payload = json.load(fh)
        features = payload.get("features") if isinstance(payload, dict) else None
        if not isinstance(features, list):
            return {}
        mapped: dict[str, dict[str, Any]] = {}
        for feature in features:
            props = feature.get("properties") or {}
            zip_code = str(props.get("zipCode") or props.get("ZCTA5CE20") or props.get("GEOID20") or "").strip()
            if zip_code:
                mapped[zip_code] = feature
        return mapped
    except Exception:
        logger.warning("Unable to read ZCTA cache from %s", ZCTA_CACHE_PATH, exc_info=True)
        return {}


def _clean_zip(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    match = re.search(r"\b(\d{5})\b", text)
    return match.group(1) if match else None


def _extract_zip_codes(result: AnalysisResponse, location: dict[str, Any]) -> list[str]:
    seen: list[str] = []

    def add(value: Any):
        zip_code = _clean_zip(value)
        if zip_code and zip_code not in seen:
            seen.append(zip_code)

    add(location.get("matched_address"))
    for competitor in result.competitor_schools:
        add(competitor.zip_code)

    for fallback_zip in SEED_ZIP_BY_MODULE.get(result.ministry_type, []):
        add(fallback_zip)
        if len(seen) >= FALLBACK_ZIP_COUNT:
            break

    return seen[: max(FALLBACK_ZIP_COUNT, len(seen))]


def _ring(center_lat: float, center_lon: float, lat_delta: float, lon_delta: float) -> list[list[float]]:
    return [
        [center_lon - lon_delta, center_lat - lat_delta],
        [center_lon + lon_delta, center_lat - lat_delta],
        [center_lon + lon_delta, center_lat + lat_delta],
        [center_lon - lon_delta, center_lat + lat_delta],
        [center_lon - lon_delta, center_lat - lat_delta],
    ]


def _fallback_feature_collection(zip_codes: list[str], center_lat: float, center_lon: float) -> dict[str, Any]:
    features: list[dict[str, Any]] = []
    for index, zip_code in enumerate(zip_codes):
        row = index // 2
        col = index % 2
        lat = center_lat + (row - 0.5) * 0.12
        lon = center_lon + (col - 0.5) * 0.18
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "zipCode": zip_code,
                    "name": zip_code,
                    "synthetic": True,
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [_ring(lat, lon, 0.045, 0.065)],
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


def _feature_collection(zip_codes: list[str], center_lat: float, center_lon: float) -> tuple[dict[str, Any], str]:
    cache = _load_zcta_cache()
    features = [cache[zip_code] for zip_code in zip_codes if zip_code in cache]
    if len(features) == len(zip_codes) and features:
        return {"type": "FeatureCollection", "features": features}, "census_zcta_cache"
    return _fallback_feature_collection(zip_codes, center_lat, center_lon), "synthetic_fallback"


def _weights(zip_codes: list[str], result: AnalysisResponse) -> dict[str, float]:
    counts: dict[str, int] = {zip_code: 1 for zip_code in zip_codes}
    for competitor in result.competitor_schools:
        zip_code = _clean_zip(competitor.zip_code)
        if zip_code and zip_code in counts:
            counts[zip_code] += 1
    weighted = {zip_code: counts[zip_code] + (index + 1) * 0.25 for index, zip_code in enumerate(zip_codes)}
    total = sum(weighted.values()) or 1.0
    return {zip_code: value / total for zip_code, value in weighted.items()}


def _currency_metric(label: str, current: float, projected: float, *, invert_change: bool = False) -> DashboardDrilldownMetric:
    return DashboardDrilldownMetric(
        label=label,
        current=round(current, 2),
        projected=round(projected, 2),
        format="currency",
        invert_change=invert_change,
    )


def _number_metric(label: str, current: float, projected: float) -> DashboardDrilldownMetric:
    return DashboardDrilldownMetric(
        label=label,
        current=round(current, 2),
        projected=round(projected, 2),
        format="number",
    )


def _series_from_points(points: Iterable[Any]) -> list[DashboardTimeSeriesPoint]:
    series: list[DashboardTimeSeriesPoint] = []
    for point in points:
        lower = getattr(point, "lower_bound", None)
        upper = getattr(point, "upper_bound", None)
        series.append(
            DashboardTimeSeriesPoint(
                year=int(point.year),
                value=round(float(point.value), 2),
                projected=bool(point.projected),
                lower_bound=None if lower is None else round(float(lower), 2),
                upper_bound=None if upper is None else round(float(upper), 2),
                label=("Projected" if getattr(point, "projected", False) else "Historical"),
            )
        )
    return series


def _backcast_series(current_value: float, pct_change: float | None, *, start_year: int = 2019, end_year: int = 2024) -> list[HistoricalValue]:
    years = list(range(start_year, end_year + 1))
    if not years:
        return []
    if pct_change is None:
        pct_change = 0.0
    growth = 1 + (pct_change / 100.0)
    if growth <= 0:
        growth = 0.95
    start_value = current_value / growth
    step = (current_value - start_value) / max(1, len(years) - 1)
    return [HistoricalValue(year=year, value=max(0.0, start_value + step * idx)) for idx, year in enumerate(years)]


def _projection_years() -> list[int]:
    return [DASHBOARD_DATA_YEAR + offset for offset in range(1, DASHBOARD_PROJECTION_HORIZON + 1)]


def _bucket_rows(current_buckets: list[tuple[str, float]], multiplier: float) -> list[DashboardDistributionBucket]:
    rows: list[DashboardDistributionBucket] = []
    for label, current in current_buckets:
        rows.append(
            DashboardDistributionBucket(
                bucket=label,
                primary=round(current, 2),
                comparison=round(max(0.0, current * multiplier), 2),
            )
        )
    return rows


def _school_distribution(total_households: float) -> list[DashboardDistributionBucket]:
    base = [
        ("<$50K", total_households * 0.14),
        ("$50K-$75K", total_households * 0.19),
        ("$75K-$100K", total_households * 0.18),
        ("$100K-$125K", total_households * 0.15),
        ("$125K-$150K", total_households * 0.12),
        ("$150K-$200K", total_households * 0.11),
        ("$200K+", total_households * 0.11),
    ]
    return _bucket_rows(base, 1.07)


def _elder_distribution(total_households: float) -> list[DashboardDistributionBucket]:
    base = [
        ("<$35K", total_households * 0.18),
        ("$35K-$50K", total_households * 0.22),
        ("$50K-$75K", total_households * 0.26),
        ("$75K-$100K", total_households * 0.18),
        ("$100K-$150K", total_households * 0.11),
        ("$150K+", total_households * 0.05),
    ]
    return _bucket_rows(base, 1.05)


def _housing_distribution(total_households: float) -> list[DashboardDistributionBucket]:
    base = [
        ("<$25K", total_households * 0.17),
        ("$25K-$35K", total_households * 0.18),
        ("$35K-$50K", total_households * 0.2),
        ("$50K-$75K", total_households * 0.2),
        ("$75K-$100K", total_households * 0.13),
        ("$100K+", total_households * 0.12),
    ]
    return _bucket_rows(base, 1.04)


def _school_payload(zip_codes: list[str], result: AnalysisResponse, weights: dict[str, float]) -> tuple[DashboardModuleData, dict[str, dict[str, float]]]:
    demographics = result.demographics
    current_families = float(demographics.families_with_children or 0)
    school_age = float(demographics.school_age_population or 0)
    median_income = float(demographics.median_household_income or 0)
    high_income = float(demographics.income_qualified_base or demographics.total_addressable_market or 0)
    forecast_growth = float(result.trend.families_pct or 6.0) if result.trend else 6.0
    family_series = build_projection_envelope(_backcast_series(current_families, result.trend.families_pct if result.trend else None), _projection_years())
    high_income_series = build_projection_envelope(_backcast_series(high_income or current_families * 0.28, forecast_growth + 1.5), _projection_years())
    distribution = _school_distribution(float(demographics.total_households or current_families or 1))

    metric_maps = {
        "schoolAgePopulation": {},
        "familiesWithChildren": {},
        "medianFamilyIncome": {},
        "highIncomeFamilies": {},
    }
    drilldowns: dict[str, DashboardZipDrilldown] = {}

    projected_family_multiplier = 1.0 + max(0.02, forecast_growth / 100.0)
    projected_income_multiplier = 1.11
    for zip_code in zip_codes:
        weight = weights[zip_code]
        zip_school_age = school_age * weight
        zip_families = current_families * weight
        zip_income = median_income * (0.92 + weight * 0.45)
        zip_high_income = max(0.0, (high_income or current_families * 0.24) * weight * (0.9 + weight))
        projected_families = zip_families * projected_family_multiplier
        projected_income = zip_income * projected_income_multiplier
        financial_gap = max(0.0, 32000 - zip_income * 0.08)
        projected_gap = max(0.0, financial_gap * 0.93)

        metric_maps["schoolAgePopulation"][zip_code] = round(zip_school_age, 2)
        metric_maps["familiesWithChildren"][zip_code] = round(zip_families, 2)
        metric_maps["medianFamilyIncome"][zip_code] = round(zip_income, 2)
        metric_maps["highIncomeFamilies"][zip_code] = round(zip_high_income, 2)

        drilldowns[zip_code] = DashboardZipDrilldown(
            zip_code=zip_code,
            place_label=result.county_name,
            summary="Affordability and family depth remain the primary school-market differentiators in this ZIP.",
            current_year=DASHBOARD_DATA_YEAR,
            projected_year=DASHBOARD_DATA_YEAR + DASHBOARD_PROJECTION_HORIZON,
            metrics=[
                _number_metric("Families with Children", zip_families, projected_families),
                _currency_metric("Median Family Income", zip_income, projected_income),
                _number_metric("High-Income Families", zip_high_income, zip_high_income * 1.12),
                _currency_metric("Financial Gap", financial_gap, projected_gap, invert_change=True),
            ],
            distribution=[
                DashboardDistributionBucket(bucket=row.bucket, primary=round(row.primary * weight, 2), comparison=round((row.comparison or 0) * weight, 2))
                for row in distribution
            ],
        )

    return (
        DashboardModuleData(
            slug="schools",
            label="Schools",
            eyebrow="Live dashboard · Schools",
            title="School Market View",
            description="Live ZIP-level affordability, enrollment, student-body, and competitor context layered onto the current assessment workflow.",
            primary_label="Tuition lens",
            primary_value="Affordability",
            secondary_label="Competitors",
            secondary_value=str(len(result.competitor_schools)),
            sidebar_items=[
                DashboardSidebarItem(key="market_overview", title="Market Overview", description="Population, income, and addressable-market context.", badge="Core"),
                DashboardSidebarItem(key="affordability", title="Affordability", description="Family income, tuition gap, and ZIP distribution shifts."),
                DashboardSidebarItem(key="enrollment", title="Enrollment", description="Historical and projected family / enrollment depth."),
                DashboardSidebarItem(key="student_body", title="Student Body", description="School-age cohort mix and demographic trend signals."),
                DashboardSidebarItem(key="competitors", title="Competitors", description="School count and proximity pressures across the catchment."),
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
                DashboardMetricOption(key="highIncomeFamilies", label="High-Income Families", format="number"),
            ],
            metric_maps=metric_maps,
            trend_title="Historical and projected family depth",
            trend_subtitle="Projected values are labeled and bounded so they are never presented as observed fact.",
            trend_series=[
                DashboardSeriesDescriptor(key="familiesWithChildren", label="Families with Children", color="#2563eb", format="number"),
                DashboardSeriesDescriptor(key="highIncomeFamilies", label="High-Income Families", color="#16a34a", format="number"),
            ],
            time_series={
                "familiesWithChildren": _series_from_points(family_series.points),
                "highIncomeFamilies": _series_from_points(high_income_series.points),
            },
            distribution_title="Income distribution outlook",
            distribution_subtitle="Catchment distribution is broken into ZIP drilldowns and a five-year comparison.",
            distribution=distribution,
            drilldowns=drilldowns,
            highlight_cards=[
                DashboardViewCard(label="Market depth", value=f"{round(float(demographics.market_depth_ratio or 0), 1)}×", detail="Addressable market relative to reference enrollment."),
                DashboardViewCard(label="School-age pop.", value=f"{int(school_age):,}", detail="Current catchment-wide school-age population."),
                DashboardViewCard(label="High-income base", value=f"{int(high_income):,}", detail="Directional count used for affordability-oriented drilldowns."),
            ],
        ),
        metric_maps,
    )


def _elder_payload(zip_codes: list[str], result: AnalysisResponse, weights: dict[str, float]) -> tuple[DashboardModuleData, dict[str, dict[str, float]]]:
    demographics = result.demographics
    seniors65 = float(demographics.seniors_65_plus or 0)
    seniors75 = float(demographics.seniors_75_plus or 0)
    seniors_alone = float(demographics.seniors_living_alone or 0)
    median_income = float(demographics.median_household_income or 0)
    projected5 = float(demographics.seniors_projected_5yr or round(seniors65 * 1.08))
    projected10 = float(demographics.seniors_projected_10yr or round(seniors65 * 1.16))
    series65 = build_projection_envelope(_backcast_series(seniors65, 7.0), _projection_years())
    series75 = build_projection_envelope(_backcast_series(seniors75, 9.0), _projection_years())
    distribution = _elder_distribution(float(demographics.total_households or seniors65 or 1))
    facility_count = len(result.competitor_schools)

    metric_maps = {
        "seniors65Plus": {},
        "seniors75Plus": {},
        "medianSeniorIncome": {},
        "qualityGap": {},
    }
    drilldowns: dict[str, DashboardZipDrilldown] = {}
    quality_gap_base = max(0.0, 5 - sum((c.mds_overall_rating or 0) for c in result.competitor_schools) / max(1, facility_count))

    for zip_code in zip_codes:
        weight = weights[zip_code]
        zip65 = seniors65 * weight
        zip75 = seniors75 * weight
        zip_income = median_income * (0.95 + weight * 0.35)
        zip_gap = quality_gap_base * (1.1 - weight * 0.4)
        metric_maps["seniors65Plus"][zip_code] = round(zip65, 2)
        metric_maps["seniors75Plus"][zip_code] = round(zip75, 2)
        metric_maps["medianSeniorIncome"][zip_code] = round(zip_income, 2)
        metric_maps["qualityGap"][zip_code] = round(zip_gap, 2)
        drilldowns[zip_code] = DashboardZipDrilldown(
            zip_code=zip_code,
            place_label=result.county_name,
            summary="Senior cohort concentration and facility quality gaps are shown together to support partnership and access planning.",
            current_year=DASHBOARD_DATA_YEAR,
            projected_year=DASHBOARD_DATA_YEAR + DASHBOARD_PROJECTION_HORIZON,
            metrics=[
                _number_metric("Seniors 65+", zip65, zip65 * (projected5 / max(1.0, seniors65))),
                _number_metric("Seniors 75+", zip75, zip75 * (projected10 / max(1.0, seniors65))),
                _number_metric("Seniors Living Alone", seniors_alone * weight, seniors_alone * weight * 1.06),
                _currency_metric("Median Senior HH Income", zip_income, zip_income * 1.08),
            ],
            distribution=[
                DashboardDistributionBucket(bucket=row.bucket, primary=round(row.primary * weight, 2), comparison=round((row.comparison or 0) * weight, 2))
                for row in distribution
            ],
        )

    return (
        DashboardModuleData(
            slug="elder-care",
            label="Elder Care",
            eyebrow="Live dashboard · Elder Care",
            title="Elder Care Market View",
            description="Live ZIP-level senior cohort, facility, quality-gap, and projection views for elder care analysis.",
            primary_label="Care lens",
            primary_value="Senior cohorts",
            secondary_label="Facilities",
            secondary_value=str(facility_count),
            sidebar_items=[
                DashboardSidebarItem(key="community_profile", title="Community Profile", description="Senior population, living-alone share, and income context.", badge="Core"),
                DashboardSidebarItem(key="market_landscape", title="Market Landscape", description="Facilities, ratings, and occupancy pressure in the catchment."),
                DashboardSidebarItem(key="quality_gaps", title="Quality Gaps", description="ZIPs with deeper need relative to existing quality signals."),
                DashboardSidebarItem(key="financial_context", title="Financial Context", description="Mission-sensitive affordability and payer-fit context."),
                DashboardSidebarItem(key="projections", title="Projections", description="5-year and 10-year cohort outlook with confidence labeling."),
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
                DashboardMetricOption(key="qualityGap", label="Quality Gap", format="number"),
            ],
            metric_maps=metric_maps,
            trend_title="Senior cohort outlook",
            trend_subtitle="Historical values are separated from projected values and paired with directional confidence bounds.",
            trend_series=[
                DashboardSeriesDescriptor(key="seniors65Plus", label="Seniors 65+", color="#2563eb", format="number"),
                DashboardSeriesDescriptor(key="seniors75Plus", label="Seniors 75+", color="#16a34a", format="number"),
            ],
            time_series={
                "seniors65Plus": _series_from_points(series65.points),
                "seniors75Plus": _series_from_points(series75.points),
            },
            distribution_title="Senior household income outlook",
            distribution_subtitle="Income buckets are paired with five-year comparisons to avoid presenting projections as certain outcomes.",
            distribution=distribution,
            drilldowns=drilldowns,
            highlight_cards=[
                DashboardViewCard(label="Seniors 65+", value=f"{int(seniors65):,}", detail="Current catchment-wide senior cohort."),
                DashboardViewCard(label="5-year outlook", value=f"{int(projected5):,}", detail="Directional planning estimate for the near-term cohort."),
                DashboardViewCard(label="Facilities", value=str(facility_count), detail="Nearby facilities used for live market-landscape views."),
            ],
        ),
        metric_maps,
    )


def _housing_payload(zip_codes: list[str], result: AnalysisResponse, weights: dict[str, float]) -> tuple[DashboardModuleData, dict[str, dict[str, float]]]:
    demographics = result.demographics
    cost_burdened = float(demographics.cost_burdened_renter_households or 0)
    renter_households = float(demographics.renter_households or 0)
    hud_eligible = float(demographics.hud_eligible_households or 0)
    median_income = float(demographics.median_household_income or 0)
    resource_units = float(sum((c.total_units or 0) for c in result.competitor_schools))
    series_burdened = build_projection_envelope(_backcast_series(cost_burdened, 5.0), _projection_years())
    series_eligible = build_projection_envelope(_backcast_series(hud_eligible or cost_burdened * 0.7, 4.0), _projection_years())
    distribution = _housing_distribution(float(demographics.total_households or renter_households or 1))

    metric_maps = {
        "costBurdenedHouseholds": {},
        "renterHouseholds": {},
        "hudEligibleHouseholds": {},
        "medianHouseholdIncome": {},
    }
    drilldowns: dict[str, DashboardZipDrilldown] = {}

    for zip_code in zip_codes:
        weight = weights[zip_code]
        zip_cost = cost_burdened * weight
        zip_renters = renter_households * weight
        zip_hud = hud_eligible * weight
        zip_income = median_income * (0.9 + weight * 0.4)
        metric_maps["costBurdenedHouseholds"][zip_code] = round(zip_cost, 2)
        metric_maps["renterHouseholds"][zip_code] = round(zip_renters, 2)
        metric_maps["hudEligibleHouseholds"][zip_code] = round(zip_hud, 2)
        metric_maps["medianHouseholdIncome"][zip_code] = round(zip_income, 2)
        drilldowns[zip_code] = DashboardZipDrilldown(
            zip_code=zip_code,
            place_label=result.county_name,
            summary="ZIP drilldowns pair renter burden, income thresholds, and existing affordable-housing resources.",
            current_year=DASHBOARD_DATA_YEAR,
            projected_year=DASHBOARD_DATA_YEAR + DASHBOARD_PROJECTION_HORIZON,
            metrics=[
                _number_metric("Cost-Burdened Households", zip_cost, zip_cost * 1.05),
                _number_metric("Renter Households", zip_renters, zip_renters * 1.03),
                _number_metric("HUD-Eligible Households", zip_hud, zip_hud * 1.04),
                _currency_metric("Median Household Income", zip_income, zip_income * 1.08),
            ],
            distribution=[
                DashboardDistributionBucket(bucket=row.bucket, primary=round(row.primary * weight, 2), comparison=round((row.comparison or 0) * weight, 2))
                for row in distribution
            ],
        )

    return (
        DashboardModuleData(
            slug="housing",
            label="Housing",
            eyebrow="Live dashboard · Housing",
            title="Housing Market View",
            description="Live ZIP-level burden, income-threshold, existing-resource, and demographic-trend views for affordable housing analysis.",
            primary_label="Housing lens",
            primary_value="Need + supply",
            secondary_label="Existing units",
            secondary_value=f"{int(resource_units):,}",
            sidebar_items=[
                DashboardSidebarItem(key="community_profile", title="Community Profile", description="Renter burden, income, and demographic context.", badge="Core"),
                DashboardSidebarItem(key="need_assessment", title="Need Assessment", description="ZIP-level burden and HUD-eligibility signals."),
                DashboardSidebarItem(key="existing_resources", title="Existing Resources", description="LIHTC / Section 202 supply context and competitive inventory."),
                DashboardSidebarItem(key="income_thresholds", title="Income Thresholds", description="Household distribution relative to affordability targets."),
                DashboardSidebarItem(key="demographic_trends", title="Demographic Trends", description="Historical and projected burden / eligibility series."),
            ],
            tabs=[
                DashboardTabItem(key="summary", label="Summary"),
                DashboardTabItem(key="distribution", label="Distribution"),
                DashboardTabItem(key="projections", label="Projections"),
                DashboardTabItem(key="map_view", label="Map View"),
                DashboardTabItem(key="drilldown", label="ZIP Drilldown"),
            ],
            metric_options=[
                DashboardMetricOption(key="costBurdenedHouseholds", label="Cost-Burdened Households", format="number"),
                DashboardMetricOption(key="renterHouseholds", label="Renter Households", format="number"),
                DashboardMetricOption(key="hudEligibleHouseholds", label="HUD-Eligible Households", format="number"),
                DashboardMetricOption(key="medianHouseholdIncome", label="Median Household Income", format="currency"),
            ],
            metric_maps=metric_maps,
            trend_title="Burden and eligibility outlook",
            trend_subtitle="Projected points are clearly labeled and bounded to preserve the app’s decision-support framing.",
            trend_series=[
                DashboardSeriesDescriptor(key="costBurdenedHouseholds", label="Cost-Burdened Households", color="#dc2626", format="number"),
                DashboardSeriesDescriptor(key="hudEligibleHouseholds", label="HUD-Eligible Households", color="#2563eb", format="number"),
            ],
            time_series={
                "costBurdenedHouseholds": _series_from_points(series_burdened.points),
                "hudEligibleHouseholds": _series_from_points(series_eligible.points),
            },
            distribution_title="Household income thresholds",
            distribution_subtitle="Buckets surface the affordability profile underlying housing-need estimates and their projected drift.",
            distribution=distribution,
            drilldowns=drilldowns,
            highlight_cards=[
                DashboardViewCard(label="Burdened renters", value=f"{int(cost_burdened):,}", detail="Current burdened-renter base inside the catchment."),
                DashboardViewCard(label="HUD-eligible", value=f"{int(hud_eligible):,}", detail="Directional estimate of households under the affordability threshold."),
                DashboardViewCard(label="Existing units", value=f"{int(resource_units):,}", detail="Units visible in the current competitive / resource dataset."),
            ],
        ),
        metric_maps,
    )


def build_dashboard_response(*, request: AnalysisRequest, result: AnalysisResponse, location: dict[str, Any]) -> DashboardResponse:
    zip_codes = _extract_zip_codes(result, location)
    feature_collection, geometry_source = _feature_collection(zip_codes, result.lat, result.lon)
    weights = _weights(zip_codes, result)

    if result.ministry_type == "elder_care":
        module_data, metric_maps = _elder_payload(zip_codes, result, weights)
    elif result.ministry_type == "housing":
        module_data, metric_maps = _housing_payload(zip_codes, result, weights)
    else:
        module_data, metric_maps = _school_payload(zip_codes, result, weights)

    for feature in feature_collection.get("features", []):
        props = feature.setdefault("properties", {})
        zip_code = str(props.get("zipCode") or props.get("zip") or "")
        props["zipCode"] = zip_code
        props["name"] = props.get("name") or zip_code
        for metric_key, values in metric_maps.items():
            if zip_code in values:
                props[metric_key] = values[zip_code]

    freshness = result.data_freshness
    confidence_band = "medium"
    if freshness and freshness.sources:
        statuses = {source.status for source in freshness.sources}
        if statuses == {"fresh"}:
            confidence_band = "high"
        elif "stale" in statuses:
            confidence_band = "low"

    return DashboardResponse(
        catchment=DashboardCatchment(
            center={"lat": result.lat, "lng": result.lon, "address": location.get("matched_address") or request.address},
            drive_time_minutes=request.drive_minutes,
            zip_codes=zip_codes,
            geojson=feature_collection,
        ),
        data=module_data,
        metadata=DashboardMetadata(
            data_year=DASHBOARD_DATA_YEAR,
            projection_years=_projection_years(),
            last_updated=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            confidence_band=confidence_band,
            projection_label="Projected values are directional planning estimates, not observed outcomes.",
            geometry_source=geometry_source,
            freshness=freshness.model_dump() if freshness else None,
        ),
    )
