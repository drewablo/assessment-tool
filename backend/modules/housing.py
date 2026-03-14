from datetime import datetime, timezone
import logging
import os
from typing import Any, Dict, List, Literal, Optional

from competitors.hud_lihtc import get_nearby_lihtc_projects
from competitors.hud_section202 import get_nearby_section202_projects
from models.schemas import (
    AnalysisResponse,
    CompetitorSchool,
    DemographicData,
    DirectionSegment,
    FeasibilityScore,
    MetricScore,
    PopulationGravityMap,
)
from modules.frameworks import build_generic_hierarchical, compute_module_benchmarks, score_rating
from modules.base import MinistryModule
from utils import decay_weight, piecewise_linear

USE_DB = os.getenv("USE_DB", "").lower() in ("1", "true", "yes")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DEMOGRAPHIC AUDIT — Low-Income Housing Module
#
# Population center density:
#   The census.py layer applies inverse-distance decay weights when aggregating
#   tract-level data (cost_burdened_renter_households, renter_households are
#   gravity-weighted).  _score_housing uses these aggregated totals directly.
#   STATUS: PRESENT via upstream gravity weighting in census.py.
#
# Income distribution (B19001):
#   Only median_household_income is used for the income sub-score.  The full
#   B19001 bracket breakdown is available in demographics["income_distribution"]
#   but was not being used to estimate HUD-eligible households (those below
#   ~60% of area median income).
#   STATUS: IMPROVED — now estimates HUD-eligible households from B19001
#   brackets and surfaces them in the narrative.
#
# Age-appropriate household composition:
#   Uses cost_burdened_renter_households (B25070) and renter_households as the
#   primary target population.  Does NOT further filter by family size or
#   household type (e.g., families with children below poverty).  Adding this
#   would require B11003 cross-tabulated with B17010 (not currently fetched).
#   STATUS: PARTIAL — uses cost-burden proxy; no family-size breakdown.
#   NOTE: Adding B17010 (poverty by family type) flagged for future
#   consideration but NOT added in this change to stay within existing
#   ACS variable set.
#
# Narrative output:
#   Generic — only states count of cost-burdened households.
#   STATUS: IMPROVED — now surfaces income bracket detail, HUD-eligible
#   estimate, and renter burden intensity in narrative descriptions.
# ---------------------------------------------------------------------------

HOUSING_WEIGHTS = {"market_size": 0.40, "income": 0.30, "competition": 0.20, "family_density": 0.10}


def _housing_weights(target_population: Literal["senior_only", "all_ages"]) -> Dict[str, float]:
    if target_population == "senior_only":
        return {"market_size": 0.34, "income": 0.23, "competition": 0.15, "family_density": 0.28}
    return HOUSING_WEIGHTS


def _score_stage2_housing(stage2_inputs: Optional[Any]) -> Dict[str, Any]:
    """Score housing operating KPIs for the Stage 2 institutional-economics panel."""
    required = [
        "occupancy_rate",
        "operating_cost_per_unit",
        "dscr",
        "subsidy_dependency",
        "operating_reserve_months",
        "capital_reserve_per_unit",
    ]
    base = {
        "schema_version": "housing-v1",
        "formula_version": "stage2-housing-v1",
        "computed_at_utc": datetime.now(timezone.utc).isoformat(),
        "required_inputs": required,
    }

    housing = getattr(stage2_inputs, "housing_financials", None) if stage2_inputs else None
    if housing is None:
        return {
            **base,
            "available": False,
            "score": None,
            "readiness": "not_ready",
            "provided_inputs": [],
            "missing_inputs": required,
            "components": [],
            "note": "Provide housing_financials in stage2_inputs to enable Housing Stage 2 scoring.",
        }

    def get(key: str) -> Optional[float]:
        val = getattr(housing, key, None)
        return float(val) if val is not None else None

    def score(value: Optional[float], segments: list) -> Optional[int]:
        if value is None:
            return None
        return round(piecewise_linear(value, segments))

    components = [
        {
            "key": "occupancy_rate",
            "label": "Occupancy Rate",
            "weight": 25,
            "score": score(get("occupancy_rate"), [(0.70, 15), (0.80, 45), (0.88, 70), (0.93, 85), (0.97, 95)]),
        },
        {
            "key": "dscr",
            "label": "Debt Service Coverage Ratio",
            "weight": 25,
            "score": score(get("dscr"), [(1.00, 10), (1.15, 40), (1.25, 70), (1.35, 87), (1.50, 95)]),
        },
        {
            "key": "operating_cost_per_unit",
            "label": "Operating Cost per Unit",
            "weight": 20,
            # Lower cost → higher score; segments are ascending in x
            "score": score(get("operating_cost_per_unit"), [(4000, 92), (5500, 80), (7000, 65), (9000, 45), (12000, 20)]),
        },
        {
            "key": "operating_reserve_months",
            "label": "Operating Reserve Months",
            "weight": 15,
            "score": score(get("operating_reserve_months"), [(1, 15), (3, 50), (6, 80), (9, 92)]),
        },
        {
            "key": "subsidy_dependency",
            "label": "Subsidy Dependency",
            "weight": 10,
            # Lower dependency → higher score
            "score": score(get("subsidy_dependency"), [(0.30, 90), (0.50, 70), (0.75, 45), (0.90, 20)]),
        },
        {
            "key": "capital_reserve_per_unit",
            "label": "Capital Reserve per Unit",
            "weight": 5,
            "score": score(get("capital_reserve_per_unit"), [(100, 20), (300, 55), (500, 75), (750, 90)]),
        },
    ]

    scored = [c for c in components if c["score"] is not None]
    stage2_score = None
    readiness = "not_ready"
    available = False
    if len(scored) == len(components) and components:
        stage2_score = round(sum(c["score"] * c["weight"] for c in components) / sum(c["weight"] for c in components))
        readiness = "ready"
        available = True
    elif scored:
        stage2_score = round(sum(c["score"] * c["weight"] for c in scored) / sum(c["weight"] for c in scored))
        readiness = "partial"
        available = True

    provided = [c["key"] for c in components if c["score"] is not None]
    missing = [c["key"] for c in components if c["score"] is None]

    return {
        **base,
        "available": available,
        "score": stage2_score,
        "readiness": readiness,
        "provided_inputs": provided,
        "missing_inputs": missing,
        "components": components,
        "note": (
            f"Housing Stage 2 scores 6 operating KPIs weighted by financial risk. "
            f"{len(scored)}/{len(components)} components scored. Readiness: {readiness}."
        ),
    }


def _estimate_hud_eligible_households(demographics: dict) -> Optional[int]:
    """Estimate households below ~60% of area median income using B19001 brackets.

    HUD LIHTC eligibility is generally capped at 60% AMI.  We approximate this
    by summing B19001 income brackets whose midpoints fall below 60% of the
    area's median household income.  This is a rough proxy — actual HUD
    eligibility considers family size and local AMI schedules — but it gives
    a directional signal that is far better than raw cost-burden counts alone.

    Uses the income_distribution list of (midpoint, household_count) tuples
    already computed by census.py from the full B19001 bracket set.
    No new ACS variables are required.
    """
    median_income = demographics.get("median_household_income")
    income_dist = demographics.get("income_distribution")
    if not median_income or median_income <= 0 or not income_dist:
        return None
    threshold = median_income * 0.60
    eligible = 0
    for midpoint, count in income_dist:
        if midpoint <= threshold:
            eligible += count
    return int(round(eligible)) if eligible > 0 else 0


def _score_housing(
    demographics: dict,
    projects: List[dict],
    hud_context: Optional[dict] = None,
    target_population: Literal["senior_only", "all_ages"] = "all_ages",
    section_202_projects: Optional[List[dict]] = None,
) -> Dict[str, float]:
    cost_burdened = demographics.get("cost_burdened_renter_households") or 0
    seniors_65_plus = demographics.get("seniors_65_plus") or 0
    renter_households = demographics.get("renter_households") or 0

    market_size = piecewise_linear(cost_burdened, [(0, 8), (250, 28), (750, 52), (1500, 72), (3000, 88), (6000, 97)])

    ratio = demographics.get("median_household_income") or 0
    income = piecewise_linear(ratio, [(25_000, 98), (40_000, 85), (60_000, 65), (80_000, 45), (100_000, 28), (130_000, 12)])

    if target_population == "senior_only" and section_202_projects is not None:
        # Senior-specific competition: measure both property density and bed
        # density against seniors 65+ in the catchment.
        s202 = section_202_projects or []
        s202_property_count = len(s202)
        total_beds = sum((p.get("total_units") or p.get("li_units") or 30) for p in s202)
        weighted_units = sum((p.get("li_units") or 30) * decay_weight(p["distance_miles"]) for p in s202)

        # Bed saturation: total beds / seniors 65+
        bed_sat = total_beds / seniors_65_plus if seniors_65_plus > 0 else (1.0 if total_beds > 0 else 0.0)
        # Property density: properties per 10,000 seniors
        prop_density = (s202_property_count / seniors_65_plus * 10_000) if seniors_65_plus > 0 else (100.0 if s202_property_count > 0 else 0.0)

        # Combined saturation ratio (beds per senior is the primary metric)
        sat_ratio = bed_sat

        # Score both dimensions and blend: 60% bed saturation, 40% property density
        bed_score = piecewise_linear(bed_sat, [(0.0, 96), (0.005, 88), (0.02, 72), (0.05, 55), (0.1, 38), (0.2, 20)])
        prop_score = piecewise_linear(prop_density, [(0.0, 96), (0.5, 88), (2.0, 72), (5.0, 55), (10.0, 38), (20.0, 20)])
        competition = round(bed_score * 0.6 + prop_score * 0.4)
    else:
        # Standard LIHTC-based competition scoring
        weighted_units = sum((p.get("li_units") or 50) * decay_weight(p["distance_miles"]) for p in projects)
        sat_ratio = weighted_units / cost_burdened if cost_burdened > 0 else 1
        competition = piecewise_linear(sat_ratio, [(0.0, 96), (0.2, 80), (0.4, 62), (0.8, 40), (1.0, 28), (1.4, 12)])

    hud_market_boost = 0.0
    hud_competition_boost = 0.0
    if hud_context:
        tenant_households = hud_context.get("tenant_households") or 0
        qct_count = hud_context.get("qct_projects") or 0
        dda_count = hud_context.get("dda_projects") or 0
        if cost_burdened > 0:
            tenant_ratio = tenant_households / cost_burdened
            hud_market_boost = piecewise_linear(tenant_ratio, [(0.0, 0), (0.1, 1), (0.2, 2), (0.4, 4), (0.7, 6)])
        if projects:
            policy_ratio = (qct_count + dda_count) / max(1, len(projects))
            hud_competition_boost = piecewise_linear(policy_ratio, [(0.0, 0), (0.15, 2), (0.3, 4), (0.6, 6), (0.9, 8)])

    market_size = min(100, market_size + hud_market_boost)
    competition = min(100, competition + hud_competition_boost)

    family_density = piecewise_linear((cost_burdened / renter_households * 100) if renter_households > 0 else 0, [(0, 10), (10, 30), (20, 55), (30, 75), (45, 92)])

    senior_demand = piecewise_linear(seniors_65_plus, [(0, 8), (2_500, 32), (6_000, 58), (12_000, 80), (20_000, 95)])

    if target_population == "senior_only":
        market_size = min(100, market_size + piecewise_linear(seniors_65_plus, [(0, 0), (2_000, 2), (6_000, 5), (12_000, 8), (20_000, 12)]))
        family_density = senior_demand

    weights = _housing_weights(target_population)
    overall = round(
        market_size * weights["market_size"]
        + income * weights["income"]
        + competition * weights["competition"]
        + family_density * weights["family_density"]
    )
    result = {
        "overall": overall,
        "market_size": market_size,
        "income": income,
        "competition": competition,
        "family_density": family_density,
        "weighted_units": weighted_units,
        "saturation_ratio": sat_ratio,
        "hud_market_boost": hud_market_boost,
        "hud_competition_boost": hud_competition_boost,
        "target_population": target_population,
        "weights": weights,
        "seniors_65_plus": seniors_65_plus,
    }
    if target_population == "senior_only" and section_202_projects is not None:
        result["s202_property_count"] = s202_property_count
        result["s202_total_beds"] = total_beds
        result["s202_bed_saturation"] = bed_sat
        result["s202_property_density"] = prop_density
    return result


async def _get_nearby_housing_db(*, lat: float, lon: float, radius_miles: float) -> list[dict]:
    from db.connection import get_session
    from db.queries import (
        get_latest_hud_property_dataset_year,
        get_nearby_housing,
        get_nearby_hud_housing_context,
    )

    async with get_session() as session:
        dataset_year = await get_latest_hud_property_dataset_year(session)
        if dataset_year is not None:
            rows = await get_nearby_hud_housing_context(
                session,
                lat=lat,
                lon=lon,
                radius_miles=radius_miles,
                dataset_year=dataset_year,
                limit=50,
            )
            if rows:
                return rows

        logger.warning("Normalized HUD housing context unavailable; trying legacy competitors_housing table")
        legacy_rows = await get_nearby_housing(
            session,
            lat=lat,
            lon=lon,
            radius_miles=radius_miles,
            limit=50,
        )
        mapped: list[dict] = []
        for project, distance in legacy_rows:
            mapped.append(
                {
                    "name": project.project_name,
                    "lat": project.lat,
                    "lon": project.lon,
                    "distance_miles": round(float(distance), 2),
                    "city": project.city,
                    "li_units": project.low_income_units,
                    "affiliation": "HUD LIHTC",
                    "is_catholic": False,
                    "is_qct": False,
                    "is_dda": False,
                    "tenant_households": None,
                }
            )
        return mapped


async def _get_nearby_section_202_db(*, lat: float, lon: float, radius_miles: float) -> list[dict]:
    """Fetch nearby HUD Section 202 senior housing properties from the database."""
    from db.connection import get_session
    from db.queries import get_nearby_section_202
    from sqlalchemy import text

    async with get_session() as session:
        rows = await get_nearby_section_202(
            session,
            lat=lat,
            lon=lon,
            radius_miles=radius_miles,
            limit=50,
        )
        if not rows:
            # Diagnostic: check if data exists and if coordinates are valid
            diag = await session.execute(text(
                "SELECT COUNT(*), "
                "MIN(lat), MAX(lat), MIN(lon), MAX(lon) "
                "FROM hud_section_202_properties"
            ))
            row = diag.one_or_none()
            if row:
                logger.warning(
                    "HUD Section 202 diagnostic: total_rows=%s lat_range=[%.4f, %.4f] lon_range=[%.4f, %.4f] "
                    "(query was lat=%.4f lon=%.4f radius=%.1f mi)",
                    row[0], row[1] or 0, row[2] or 0, row[3] or 0, row[4] or 0,
                    lat, lon, radius_miles,
                )
    mapped: list[dict] = []
    for prop, distance in rows:
        mapped.append(
            {
                "name": prop.servicing_site_name,
                "lat": prop.lat,
                "lon": prop.lon,
                "distance_miles": round(float(distance), 2),
                "city": prop.city,
                "state": prop.state,
                "street_address": prop.street_address,
                "zip_code": prop.zip_code,
                "li_units": prop.total_assisted_units,
                "total_units": prop.total_units,
                "affiliation": "HUD Section 202",
                "is_catholic": False,
                "is_qct": False,
                "is_dda": False,
                "tenant_households": None,
                "property_name": prop.property_name,
                "client_group_name": prop.client_group_name,
                "property_category": prop.property_category,
                "primary_financing_type": prop.primary_financing_type,
                "phone_number": prop.phone_number,
                "reac_inspection_score": prop.reac_inspection_score,
                "source_type": "hud_section_202",
            }
        )
    return mapped


async def analyze_housing(
    *,
    location: dict,
    demographics: dict,
    request: Any,
    radius_miles: float,
    drive_minutes: int,
    isochrone_polygon: Optional[dict],
    catchment_type: str,
) -> AnalysisResponse:
    used_loader_fallback = False
    if USE_DB:
        projects = await _get_nearby_housing_db(
            lat=location["lat"],
            lon=location["lon"],
            radius_miles=radius_miles,
        )
        if not projects:
            used_loader_fallback = True
            logger.warning("USE_DB=true but normalized HUD housing context unavailable; using CSV fallback")
    else:
        projects = []

    if (not USE_DB) or used_loader_fallback:
        projects = get_nearby_lihtc_projects(location["lat"], location["lon"], radius_miles)

    # Include HUD Section 202 properties for Senior Housing analysis
    housing_target_population = getattr(request, "housing_target_population", "all_ages") or "all_ages"
    section_202_projects: list[dict] = []
    if housing_target_population == "senior_only":
        # Try DB first, then CSV fallback — no longer gated on USE_DB
        if USE_DB:
            try:
                section_202_projects = await _get_nearby_section_202_db(
                    lat=location["lat"],
                    lon=location["lon"],
                    radius_miles=radius_miles,
                )
                logger.info(
                    "HUD Section 202 DB query: lat=%.4f lon=%.4f radius=%.1f returned=%d",
                    location["lat"], location["lon"], radius_miles, len(section_202_projects),
                )
            except Exception as exc:
                logger.warning("HUD Section 202 DB query failed (non-blocking): %s", exc)

        if not section_202_projects:
            section_202_projects = get_nearby_section202_projects(
                location["lat"], location["lon"], radius_miles,
            )

        if section_202_projects:
            logger.info(
                "HUD Section 202: %d senior housing properties found within %.1f miles",
                len(section_202_projects), radius_miles,
            )
        else:
            logger.warning("HUD Section 202: no properties found within %.1f miles (DB + CSV checked)", radius_miles)

    # For senior_only, prefer Section 202 as the competitor set.
    # Fall back to LIHTC projects if no Section 202 data is available.
    if housing_target_population == "senior_only":
        all_projects = section_202_projects if section_202_projects else projects
    else:
        all_projects = projects

    hud_context = {
        "tenant_households": sum((p.get("tenant_households") or 0) for p in projects),
        "qct_projects": sum(1 for p in projects if p.get("is_qct")),
        "dda_projects": sum(1 for p in projects if p.get("is_dda")),
        "used_db_enrichment": USE_DB and not used_loader_fallback,
        "section_202_count": len(section_202_projects),
    }
    scores = _score_housing(
        demographics,
        projects,
        hud_context=hud_context,
        target_population=housing_target_population,
        section_202_projects=section_202_projects if housing_target_population == "senior_only" else None,
    )
    cost_burdened = demographics.get("cost_burdened_renter_households") or 0
    renter_households = demographics.get("renter_households") or 0
    hud_eligible = _estimate_hud_eligible_households(demographics)
    total_households = demographics.get("total_households") or 0
    renter_burden_pct = round(cost_burdened / renter_households * 100, 1) if renter_households > 0 else None

    calibration_note = (
        "This analysis was calibrated for senior-only affordable housing."
        if housing_target_population == "senior_only"
        else "This analysis was calibrated for affordable housing serving all age groups."
    )

    if scores["overall"] >= 75:
        rec = "Strong Affordable Housing Opportunity"
    elif scores["overall"] >= 55:
        rec = "Moderate Affordable Housing Opportunity"
    else:
        rec = "Challenging Affordable Housing Market"

    stage2 = _score_stage2_housing(request.stage2_inputs)

    rounded_market_size = round(scores["market_size"])
    rounded_income = round(scores["income"])
    rounded_competition = round(scores["competition"])
    rounded_family_density = round(scores["family_density"])

    benchmarks = await compute_module_benchmarks(
        ministry_type="housing",
        overall=scores["overall"],
        state_fips=location.get("state_fips", ""),
        lat=location["lat"],
        lon=location["lon"],
        demographics=demographics,
        market_size=rounded_market_size,
        income=rounded_income,
        competition=rounded_competition,
        family_density=rounded_family_density,
    )
    hierarchical = build_generic_hierarchical(
        market_size=rounded_market_size,
        income=rounded_income,
        competition=rounded_competition,
        family_density=rounded_family_density,
    )

    # Build population gravity map from directional housing data
    housing_dir_data = demographics.get("housing_by_direction")
    gravity_map = None
    if housing_dir_data:
        segment_map = {}
        for d, info in housing_dir_data.items():
            burden = info.get("burden_ratio")
            if burden is not None:
                if burden > 0.40:
                    signal = "Growing"  # High burden = growing need
                elif burden >= 0.25:
                    signal = "Stable"
                else:
                    signal = "Declining"
            else:
                signal = None
            segment_map[d] = DirectionSegment(
                cost_burdened_renters=info.get("cost_burdened_renters", 0),
                renter_households=info.get("renter_households", 0),
                burden_ratio=burden,
                growth_signal=signal,
            )
        # Dominant direction: highest cost-burdened renter concentration
        dominant = max(segment_map, key=lambda d: segment_map[d].cost_burdened_renters or 0)
        gravity_map = PopulationGravityMap(
            by_direction=segment_map,
            dominant_direction=dominant,
            gravity_weighted=demographics.get("gravity_weighted", False),
        )

    return AnalysisResponse(
        school_name=request.school_name,
        ministry_type="housing",
        analysis_address=location.get("matched_address", ""),
        county_name=demographics.get("county_name", location.get("county_name", "")),
        state_name=location.get("state_name", ""),
        lat=location["lat"],
        lon=location["lon"],
        radius_miles=radius_miles,
        catchment_minutes=drive_minutes,
        isochrone_polygon=isochrone_polygon,
        catchment_type=catchment_type,
        gender=request.gender,
        grade_level=request.grade_level,
        demographics=DemographicData(
            total_population=demographics.get("total_population"),
            median_household_income=demographics.get("median_household_income"),
            total_households=demographics.get("total_households"),
            data_geography=demographics.get("data_geography", "county"),
            ministry_target_population=(demographics.get("seniors_65_plus") or 0) if housing_target_population == "senior_only" else cost_burdened,
            seniors_65_plus=demographics.get("seniors_65_plus"),
            cost_burdened_renter_households=cost_burdened,
            renter_households=renter_households,
            hud_eligible_households=hud_eligible,
            hud_tenant_households=hud_context["tenant_households"] or None,
            qct_designated_projects=hud_context["qct_projects"] or None,
            dda_designated_projects=hud_context["dda_projects"] or None,
        ),
        competitor_schools=[
            CompetitorSchool(
                name=p["name"],
                lat=p["lat"],
                lon=p["lon"],
                distance_miles=p["distance_miles"],
                affiliation=(
                    p.get("affiliation", "HUD LIHTC")
                    if p.get("source_type") == "hud_section_202"
                    else (
                        "HUD LIHTC"
                        + (" | QCT" if p.get("is_qct") else "")
                        + (" | DDA" if p.get("is_dda") else "")
                    )
                ),
                is_catholic=False,
                city=p.get("city"),
                state=p.get("state"),
                street_address=p.get("street_address"),
                zip_code=p.get("zip_code"),
                enrollment=p.get("li_units"),
                gender="N/A",
                grade_level=(
                    "Section 202 Senior"
                    if p.get("source_type") == "hud_section_202"
                    else "Housing"
                ),
                total_units=p.get("total_units") if p.get("source_type") == "hud_section_202" else None,
                client_group_name=p.get("client_group_name") if p.get("source_type") == "hud_section_202" else None,
                property_category=p.get("property_category") if p.get("source_type") == "hud_section_202" else None,
                primary_financing_type=p.get("primary_financing_type") if p.get("source_type") == "hud_section_202" else None,
                phone_number=p.get("phone_number") if p.get("source_type") == "hud_section_202" else None,
                reac_inspection_score=p.get("reac_inspection_score") if p.get("source_type") == "hud_section_202" else None,
            )
            for p in all_projects[:25]
        ],
        catholic_school_count=0,
        total_private_school_count=len(all_projects),
        feasibility_score=FeasibilityScore(
            overall=scores["overall"],
            weighting_profile=request.weighting_profile,
            stage2=stage2,
            benchmarks=benchmarks,
            hierarchical=hierarchical,
            market_size=MetricScore(
                score=rounded_market_size,
                label="Cost-Burdened Households",
                description=(
                    f"{cost_burdened:,} renter households pay >30% income on rent"
                    + (f". Senior calibration gives additional demand credit for {scores['seniors_65_plus']:,} residents age 65+" if housing_target_population == "senior_only" else "")
                    + (f" out of {renter_households:,} total renter households" if renter_households else "")
                    + (f". Estimated {hud_eligible:,} households below 60% of area median income (HUD-eligible proxy)" if hud_eligible else "")
                    + (f". HUD tenant enrichment contributed +{scores['hud_market_boost']:.1f} points" if scores.get("hud_market_boost") else "")
                ),
                weight=round(scores["weights"]["market_size"] * 100),
                rating=score_rating(rounded_market_size),
            ),
            income=MetricScore(
                score=rounded_income,
                label="Income Need",
                description=(
                    f"Median household income ${demographics.get('median_household_income') or 0:,}. "
                    "Lower median income indicates higher HUD-eligible need"
                ),
                weight=round(scores["weights"]["income"] * 100),
                rating=score_rating(rounded_income),
            ),
            competition=MetricScore(
                score=rounded_competition,
                label=("Section 202 Senior Saturation" if housing_target_population == "senior_only" else "LIHTC Saturation"),
                description=(
                    (
                        f"{scores.get('s202_property_count', 0)} Section 202 properties with {scores.get('s202_total_beds', 0):,} total beds"
                        f" vs. {scores['seniors_65_plus']:,} seniors 65+"
                        f" — bed saturation: {scores.get('s202_bed_saturation', 0):.4f},"
                        f" property density: {scores.get('s202_property_density', 0):.1f} per 10k seniors"
                    )
                    if housing_target_population == "senior_only"
                    else (
                        f"Weighted LIHTC unit saturation ratio: {scores['saturation_ratio']:.2f}"
                        + (f"; QCT/DDA policy-context adjustment +{scores['hud_competition_boost']:.1f}" if scores.get("hud_competition_boost") else "")
                    )
                ),
                weight=round(scores["weights"]["competition"] * 100),
                rating=score_rating(rounded_competition),
            ),
            family_density=MetricScore(
                score=rounded_family_density,
                label=("Senior Demand Signal" if housing_target_population == "senior_only" else "Renter Burden Intensity"),
                description=(
                    f"Senior calibration uses {scores['seniors_65_plus']:,} residents age 65+ as the primary household-demand indicator."
                    if housing_target_population == "senior_only"
                    else (
                        f"{renter_burden_pct}% of renter households are cost burdened"
                        if renter_burden_pct is not None
                        else "Share of renter households that are cost burdened"
                    )
                ),
                weight=round(scores["weights"]["family_density"] * 100),
                rating=score_rating(rounded_family_density),
            ),
            scenario_conservative=max(0, scores["overall"] - 12),
            scenario_optimistic=min(100, scores["overall"] + 12),
        ),
        recommendation=rec,
        population_gravity=gravity_map,
        recommendation_detail=(
            (
                f"{scores['seniors_65_plus']:,} seniors age 65+ within the catchment"
                + f". {scores.get('s202_property_count', 0)} HUD 202 properties with {scores.get('s202_total_beds', 0):,} total beds"
                + f" (bed saturation {scores.get('s202_bed_saturation', 0):.4f}, {scores.get('s202_property_density', 0):.1f} properties per 10k seniors)"
                + f". {cost_burdened:,} cost-burdened renter households provide additional market context"
                + ". " + calibration_note
            )
            if housing_target_population == "senior_only"
            else (
                f"{cost_burdened:,} cost-burdened renter households within the catchment"
                + (f" ({renter_burden_pct}% of all renters)" if renter_burden_pct is not None else "")
                + (f"; an estimated {hud_eligible:,} households below 60% AMI" if hud_eligible else "")
                + f". LIHTC saturation ratio {scores['saturation_ratio']:.2f} across {len(all_projects)} existing projects"
                + (f", including {hud_context['qct_projects']} QCT and {hud_context['dda_projects']} DDA-designated projects" if hud_context["used_db_enrichment"] else "")
                + ". " + calibration_note
            )
        ),
        data_notes=[
            note for note in [
                "Housing module Phase 2 scaffold active.",
                calibration_note,
                "Competitor inventory sourced from local HUD LIHTC ingest cache when available.",
                (
                    f"HUD Section 202: {hud_context['section_202_count']} senior housing properties included in competitor context."
                    if hud_context.get("section_202_count")
                    else "HUD Section 202 data not available for this analysis."
                    if housing_target_population == "senior_only"
                    else None
                ),
                (
                    "HUD normalized enrichment active: LIHTC properties joined to QCT/DDA designations and tenant aggregates "
                    "using deterministic exact keys."
                    if hud_context["used_db_enrichment"]
                    else "HUD normalized enrichment unavailable for this catchment; fallback path used."
                ),
            ] if note is not None
        ] + (
            [
                f"HUD-eligible estimate: {hud_eligible:,} households have income below 60% of area median "
                f"(${demographics.get('median_household_income') or 0:,} median × 60% = "
                f"${int((demographics.get('median_household_income') or 0) * 0.6):,} threshold). "
                "This is a proxy from ACS B19001 income brackets — actual eligibility varies by family size and local AMI."
            ] if hud_eligible else []
        ) + (
            [
                f"Renter burden: {cost_burdened:,} of {renter_households:,} renter households ({renter_burden_pct}%) "
                "pay more than 30% of income on rent."
            ] if renter_burden_pct is not None else []
        ),
    )


class HousingModule(MinistryModule):
    def __init__(self):
        super().__init__(
            key="housing",
            display_name="Low-Income Housing",
            supports_mission_toggle=False,
            analyzer=analyze_housing,
        )

    def weighting_profile(self, mission_mode: bool = False) -> Dict[str, float]:
        return HOUSING_WEIGHTS
