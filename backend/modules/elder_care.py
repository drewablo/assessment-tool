import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from api.bls_workforce import fetch_county_qcew, score_workforce_availability
from competitors.cms_care_compare import get_nearby_elder_care_facilities
from models.schemas import AnalysisResponse, CompetitorSchool, DemographicData, DirectionSegment, FeasibilityScore, MetricScore, PopulationGravityMap
from modules.base import MinistryModule
from modules.frameworks import build_generic_hierarchical, compute_module_benchmarks, score_rating
from utils import decay_weight, piecewise_linear

logger = logging.getLogger(__name__)
USE_DB = os.getenv("USE_DB", "").lower() in ("1", "true", "yes")

MISSION_WEIGHTS = {"market_size": 0.35, "income": 0.15, "competition": 0.30, "family_density": 0.10, "occupancy": 0.10}
MARKET_WEIGHTS = {"market_size": 0.35, "income": 0.25, "competition": 0.25, "family_density": 0.05, "occupancy": 0.10}


def _score_stage2_elder_care(stage2_inputs: Optional[Any]) -> Dict[str, Any]:
    """Score elder care operating KPIs for the Stage 2 institutional-economics panel."""
    required = [
        "occupancy_rate",
        "operating_cost_per_bed",
        "staffing_hours_per_resident_day",
        "payer_mix_private_pay",
        "payer_mix_medicaid",
        "days_cash_on_hand",
    ]
    base = {
        "schema_version": "elder-care-v1",
        "formula_version": "stage2-elder-care-v1",
        "computed_at_utc": datetime.now(timezone.utc).isoformat(),
        "required_inputs": required,
    }

    ec = getattr(stage2_inputs, "elder_care_financials", None) if stage2_inputs else None
    if ec is None:
        return {
            **base,
            "available": False,
            "score": None,
            "readiness": "not_ready",
            "provided_inputs": [],
            "missing_inputs": required,
            "components": [],
            "note": "Provide elder_care_financials in stage2_inputs to enable Elder Care Stage 2 scoring.",
        }

    def get(key: str) -> Optional[float]:
        val = getattr(ec, key, None)
        return float(val) if val is not None else None

    def score(value: Optional[float], segments: list) -> Optional[int]:
        if value is None:
            return None
        return round(piecewise_linear(value, segments))

    components = [
        {
            "key": "occupancy_rate",
            "label": "Occupancy Rate",
            "weight": 30,
            "score": score(get("occupancy_rate"), [(0.65, 15), (0.75, 40), (0.85, 70), (0.90, 85), (0.95, 95)]),
        },
        {
            "key": "days_cash_on_hand",
            "label": "Days Cash on Hand",
            "weight": 20,
            "score": score(get("days_cash_on_hand"), [(15, 20), (30, 50), (60, 75), (90, 88), (120, 95)]),
        },
        {
            "key": "operating_cost_per_bed",
            "label": "Operating Cost per Bed",
            "weight": 20,
            # Lower cost → higher score
            "score": score(get("operating_cost_per_bed"), [(45000, 92), (55000, 80), (65000, 65), (75000, 45), (90000, 20)]),
        },
        {
            "key": "payer_mix_private_pay",
            "label": "Private Pay Mix",
            "weight": 15,
            "score": score(get("payer_mix_private_pay"), [(0.05, 15), (0.15, 40), (0.30, 65), (0.45, 80), (0.60, 95)]),
        },
        {
            "key": "staffing_hours_per_resident_day",
            "label": "Staffing Hours per Resident Day",
            "weight": 10,
            # Higher HPRD → better quality and compliance posture
            "score": score(get("staffing_hours_per_resident_day"), [(2.5, 20), (3.0, 50), (3.5, 75), (4.0, 90), (4.5, 97)]),
        },
        {
            "key": "payer_mix_medicaid",
            "label": "Medicaid Dependency",
            "weight": 5,
            # Lower Medicaid share → higher margin
            "score": score(get("payer_mix_medicaid"), [(0.20, 92), (0.35, 78), (0.50, 60), (0.65, 40), (0.80, 20)]),
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
            f"Elder Care Stage 2 scores 6 operating KPIs covering occupancy, financial reserves, "
            f"cost efficiency, and payer-mix quality. "
            f"{len(scored)}/{len(components)} components scored. Readiness: {readiness}."
        ),
    }

# ---------------------------------------------------------------------------
# DEMOGRAPHIC AUDIT — Elder Care Module
#
# Population center density:
#   The census.py layer applies inverse-distance decay weights when aggregating
#   tract-level data (seniors_65_plus, seniors_75_plus, seniors_living_alone,
#   seniors_below_200pct_poverty are all gravity-weighted).  The scoring in
#   _score_elder_care uses these gravity-weighted totals directly.
#   STATUS: PRESENT via upstream gravity weighting in census.py.
#
# Income distribution (B19001):
#   Only median_household_income is used for the income sub-score.  No bracket-
#   level analysis is performed (e.g., identifying seniors on fixed incomes or
#   households below specific income thresholds).
#   STATUS: PARTIAL — median only; no bracket-level senior income filtering.
#   NOTE: ACS does not cross-tabulate B19001 by age; adding age-specific
#   income brackets would require B19049 (not currently fetched).  Flagged
#   for future consideration but NOT added in this change.
#
# Age-appropriate household composition:
#   Uses seniors_75_plus as the primary market-demand population, seniors_65_plus
#   for overall context, seniors_living_alone for isolation signal, and
#   seniors_below_200pct_poverty (B17001) for mission-mode targeting.
#   STATUS: PRESENT — good age-appropriate filtering.
#
# Narrative output:
#   Generic — states target population count but doesn't call out concentration,
#   living-alone share, or poverty context explicitly.
#   STATUS: NEEDS IMPROVEMENT — enriched below.
# ---------------------------------------------------------------------------

SURVIVAL_RATE_65_TO_74 = 0.9798
SURVIVAL_RATE_75_PLUS = 0.9542


def _target_population(demographics: dict, mission_mode: bool) -> float:
    if mission_mode:
        alone = demographics.get("seniors_living_alone") or 0
        poverty = demographics.get("seniors_below_200pct_poverty") or 0
        return alone * 0.5 + poverty * 0.5
    return demographics.get("seniors_75_plus") or 0


def _facility_beds(facility: dict) -> int:
    """Return a normalized bed count across different source field names."""
    for key in ("certified_beds", "licensed_beds", "beds"):
        raw = facility.get(key)
        if raw is None or raw == "":
            continue
        text = str(raw).replace(",", "").strip()
        if text.lower() in ("nan", "none", "null", ""):
            continue
        try:
            return max(0, int(float(text)))
        except (TypeError, ValueError):
            continue
    return 0


def _facility_affiliation(facility: dict) -> str:
    owner_name = (facility.get("owner_name") or "").strip()
    ownership = (facility.get("ownership") or "").strip()

    if owner_name and ownership:
        normalized_owner = owner_name.lower()
        normalized_ownership = ownership.lower()
        if normalized_owner in normalized_ownership or normalized_ownership in normalized_owner:
            return owner_name
        return f"{owner_name} ({ownership})"
    if owner_name:
        return owner_name
    if ownership:
        return ownership
    return "CMS Care Compare"

async def _get_nearby_elder_care_db(*, lat: float, lon: float, radius_miles: float, care_level: str) -> list[dict]:
    from db.connection import get_session
    from db.queries import get_nearby_elder_care as get_nearby_elder_care_query

    async with get_session() as session:
        rows = await get_nearby_elder_care_query(
            session,
            lat=lat,
            lon=lon,
            radius_miles=radius_miles,
            care_level=care_level,
            limit=50,
        )

    mapped = []
    for facility, distance in rows:
        mapped.append(
            {
                "name": facility.facility_name,
                "lat": facility.lat,
                "lon": facility.lon,
                "distance_miles": round(float(distance), 2),
                "care_level": facility.care_level or "snf",
                "certified_beds": facility.certified_beds,
                "occupancy_pct": facility.occupancy_pct,
                "overall_rating": facility.overall_rating,
                "ownership": facility.ownership_type,
                "owner_name": facility.ownership_type,
                "city": facility.city,
            }
        )
    return mapped


def _score_elder_care(demographics: dict, facilities: List[dict], mission_mode: bool) -> Dict[str, float]:
    target_pop = _target_population(demographics, mission_mode)
    market_size = piecewise_linear(target_pop, [(0, 8), (250, 32), (600, 52), (1200, 74), (2500, 91), (5000, 98)])

    median_income = demographics.get("median_household_income") or 0
    if mission_mode:
        income = piecewise_linear(median_income, [(20_000, 97), (28_000, 88), (35_000, 70), (50_000, 45), (80_000, 20), (120_000, 10)])
    else:
        income = piecewise_linear(median_income, [(20_000, 10), (35_000, 24), (54_000, 52), (75_000, 74), (100_000, 90), (140_000, 97)])

    weighted_beds = sum(_facility_beds(f) * decay_weight(f["distance_miles"]) for f in facilities)
    saturation_ratio = weighted_beds / target_pop if target_pop > 0 else 1
    competition = piecewise_linear(saturation_ratio, [(0.0, 96), (0.2, 85), (0.4, 66), (0.8, 42), (1.0, 30), (1.4, 14)])

    seniors_65_plus = demographics.get("seniors_65_plus") or 0
    living_alone_pct = (demographics.get("seniors_living_alone") or 0) / seniors_65_plus * 100 if seniors_65_plus > 0 else 0
    family_density = piecewise_linear(living_alone_pct, [(0, 20), (8, 35), (15, 55), (25, 76), (35, 90), (50, 97)])

    occupancy_points: list[tuple[float, float]] = []
    for facility in facilities:
        if facility.get("care_level") == "assisted_living":
            continue
        occupancy_pct = facility.get("occupancy_pct")
        if occupancy_pct is None:
            continue
        try:
            occupancy_value = float(occupancy_pct)
            weight = decay_weight(facility["distance_miles"])
        except (TypeError, ValueError, KeyError):
            continue
        occupancy_points.append((occupancy_value, weight))

    weighted_avg_occupancy_pct = None
    if occupancy_points:
        total_occupancy_weight = sum(weight for _, weight in occupancy_points)
        if total_occupancy_weight > 0:
            weighted_avg_occupancy_pct = sum(value * weight for value, weight in occupancy_points) / total_occupancy_weight

    occupancy = 50.0
    if weighted_avg_occupancy_pct is not None:
        occupancy = piecewise_linear(weighted_avg_occupancy_pct, [
            (50, 15), (65, 35), (75, 55), (82, 70), (88, 83), (93, 92), (97, 98)
        ])

    weights = MISSION_WEIGHTS if mission_mode else MARKET_WEIGHTS
    overall = round(
        market_size * weights["market_size"]
        + income * weights["income"]
        + competition * weights["competition"]
        + family_density * weights["family_density"]
        + occupancy * weights["occupancy"]
    )
    return {
        "overall": overall,
        "market_size": market_size,
        "income": income,
        "competition": competition,
        "family_density": family_density,
        "occupancy": occupancy,
        "target_pop": target_pop,
        "weighted_beds": weighted_beds,
        "saturation_ratio": saturation_ratio,
        "weighted_avg_occupancy_pct": weighted_avg_occupancy_pct,
    }


async def analyze_elder_care(
    *,
    location: dict,
    demographics: dict,
    request: Any,
    radius_miles: float,
    drive_minutes: int,
    isochrone_polygon: Optional[dict],
    catchment_type: str,
) -> AnalysisResponse:
    seniors_65_plus = demographics.get("seniors_65_plus")
    seniors_75_plus = demographics.get("seniors_75_plus")
    seniors_projected_5yr = None
    seniors_projected_10yr = None

    if seniors_65_plus is not None and seniors_75_plus is not None:
        seniors_65_to_74 = max(0, seniors_65_plus - seniors_75_plus)
        projected_5yr = (seniors_65_to_74 * (SURVIVAL_RATE_65_TO_74**5)) + (seniors_75_plus * (SURVIVAL_RATE_75_PLUS**5))
        projected_10yr = (seniors_65_to_74 * (SURVIVAL_RATE_65_TO_74**10)) + (seniors_75_plus * (SURVIVAL_RATE_75_PLUS**10))
        seniors_projected_5yr = int(round(projected_5yr))
        seniors_projected_10yr = int(round(projected_10yr))

    used_live_fallback = False
    if USE_DB:
        facilities = await _get_nearby_elder_care_db(
            lat=location["lat"],
            lon=location["lon"],
            radius_miles=radius_miles,
            care_level=request.care_level,
        )
        if not facilities:
            used_live_fallback = True
            logger.warning(
                "USE_DB=true but no elder care competitors found in DB for catchment; falling back to live fetch."
            )
    else:
        facilities = []

    if (not USE_DB) or used_live_fallback:
        facilities = await get_nearby_elder_care_facilities(
            location["lat"],
            location["lon"],
            radius_miles,
            request.care_level,
            request.min_mds_overall_rating,
        )
    scores = _score_elder_care(demographics, facilities, request.mission_mode)

    # Workforce availability index (BLS QCEW)
    county_fips = location.get("county_fips", "")
    workforce_score = 50.0
    workforce_details = {"available": False, "note": "Workforce data not available"}
    if county_fips and len(county_fips) == 5:
        try:
            qcew_data = await fetch_county_qcew(county_fips)
            workforce_score, workforce_details = score_workforce_availability(
                qcew_data, seniors_65_plus or 0
            )
        except Exception as e:
            logger.warning("Workforce scoring failed for county %s: %s", county_fips, e)

    if scores["overall"] >= 75:
        rec = "Strong Elder Care Opportunity"
    elif scores["overall"] >= 55:
        rec = "Moderate Elder Care Opportunity"
    else:
        rec = "Challenging Elder Care Market"

    mode_label = "Mission-Aligned" if request.mission_mode else "Market Demand"
    target_desc = "vulnerable seniors" if request.mission_mode else "population age 75+"

    # Demographic composition detail for narrative enrichment
    seniors_living_alone = demographics.get("seniors_living_alone") or 0
    seniors_below_200pct_poverty = demographics.get("seniors_below_200pct_poverty") or 0
    living_alone_pct = (
        round(seniors_living_alone / seniors_65_plus * 100, 1) if seniors_65_plus and seniors_65_plus > 0 else None
    )
    poverty_pct_of_seniors = (
        round(seniors_below_200pct_poverty / seniors_65_plus * 100, 1) if seniors_65_plus and seniors_65_plus > 0 else None
    )

    rounded_market_size = round(scores["market_size"])
    rounded_income = round(scores["income"])
    rounded_competition = round(scores["competition"])
    rounded_family_density = round(scores["family_density"])
    rounded_occupancy = round(scores["occupancy"])
    rounded_workforce = round(workforce_score) if workforce_details.get("available") else None

    benchmarks = await compute_module_benchmarks(
        ministry_type="elder_care",
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
        occupancy=rounded_occupancy,
        workforce=rounded_workforce,
    )

    stage2 = _score_stage2_elder_care(request.stage2_inputs)

    # Build population gravity map from directional senior data
    seniors_dir_data = demographics.get("seniors_by_direction")
    gravity_map = None
    if seniors_dir_data:
        segment_map = {}
        for d, info in seniors_dir_data.items():
            isolation = info.get("isolation_ratio")
            if isolation is not None:
                if isolation > 0.35:
                    signal = "Growing"  # High isolation = growing need
                elif isolation >= 0.2:
                    signal = "Stable"
                else:
                    signal = "Declining"
            else:
                signal = None
            segment_map[d] = DirectionSegment(
                seniors_65_plus=info.get("seniors_65_plus", 0),
                seniors_75_plus=info.get("seniors_75_plus", 0),
                seniors_living_alone=info.get("seniors_living_alone", 0),
                seniors_below_poverty=info.get("seniors_below_poverty", 0),
                isolation_ratio=isolation,
                growth_signal=signal,
            )
        # Dominant direction: highest seniors 75+ concentration
        dominant = max(segment_map, key=lambda d: segment_map[d].seniors_75_plus or 0)
        gravity_map = PopulationGravityMap(
            by_direction=segment_map,
            dominant_direction=dominant,
            gravity_weighted=demographics.get("gravity_weighted", False),
        )

    return AnalysisResponse(
        school_name=request.school_name,
        ministry_type="elder_care",
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
            data_confidence=demographics.get("data_confidence"),
            ministry_target_population=int(round(scores["target_pop"])),
            seniors_65_plus=seniors_65_plus,
            seniors_75_plus=seniors_75_plus,
            seniors_living_alone=seniors_living_alone,
            seniors_below_200pct_poverty=seniors_below_200pct_poverty or None,
            seniors_projected_5yr=seniors_projected_5yr,
            seniors_projected_10yr=seniors_projected_10yr,
        ),
        competitor_schools=[
            CompetitorSchool(
                name=f["name"],
                lat=f["lat"],
                lon=f["lon"],
                distance_miles=f["distance_miles"],
                affiliation=_facility_affiliation(f),
                is_catholic=False,
                city=f.get("city"),
                enrollment=_facility_beds(f) or None,
                gender=f.get("certification") or "N/A",
                grade_level=f.get("care_level", "Elder Care"),
                occupancy_pct=f.get("occupancy_pct"),
                mds_overall_rating=f.get("mds_overall_rating"),
            )
            for f in facilities[:25]
        ],
        catholic_school_count=0,
        total_private_school_count=len(facilities),
        feasibility_score=FeasibilityScore(
            overall=scores["overall"],
            weighting_profile=request.weighting_profile,
            stage2=stage2,
            benchmarks=benchmarks,
            hierarchical=hierarchical,
            market_size=MetricScore(
                score=rounded_market_size,
                label=f"{mode_label} Target Population",
                description=(
                    f"Estimated {target_desc}: {int(round(scores['target_pop'])):,}"
                    + (f" within the catchment. Of {seniors_65_plus:,} seniors age 65+, "
                       f"{seniors_75_plus:,} are age 75+ ({round(seniors_75_plus / seniors_65_plus * 100)}%)"
                       if seniors_65_plus and seniors_65_plus > 0 and seniors_75_plus else "")
                    + (f". {seniors_living_alone:,} seniors live alone ({living_alone_pct}%)"
                       if seniors_living_alone and living_alone_pct else "")
                ),
                weight=35,
                rating=score_rating(rounded_market_size),
            ),
            income=MetricScore(
                score=rounded_income,
                label="Income Fit",
                description=(
                    f"Median household income ${demographics.get('median_household_income') or 0:,}. "
                    + ("Lower income signals higher mission-need alignment."
                       if request.mission_mode
                       else "Higher income supports private-pay capacity for elder care.")
                ),
                weight=15 if request.mission_mode else 25,
                rating=score_rating(rounded_income),
            ),
            competition=MetricScore(score=rounded_competition, label="Bed Saturation", description=f"Weighted certified-bed saturation ratio: {scores['saturation_ratio']:.2f}", weight=30 if request.mission_mode else 25, rating=score_rating(rounded_competition)),
            family_density=MetricScore(
                score=rounded_family_density,
                label="Isolation Signal",
                description=(
                    f"{living_alone_pct}% of seniors live alone"
                    if living_alone_pct is not None
                    else "Share of seniors living alone (proxy for support network gaps)"
                ) + (
                    f"; {poverty_pct_of_seniors}% of seniors are near poverty"
                    if poverty_pct_of_seniors is not None else ""
                ),
                weight=10 if request.mission_mode else 5,
                rating=score_rating(rounded_family_density),
            ),
            workforce=MetricScore(
                score=rounded_workforce,
                label="Workforce Availability",
                description=workforce_details.get("note", "Workforce data unavailable"),
                weight=0,  # informational — not yet included in overall score
                rating=(
                    "strong" if workforce_score >= 75 else
                    "moderate" if workforce_score >= 55 else
                    "weak" if workforce_score >= 35 else "poor"
                ),
            ) if workforce_details.get("available") else None,
            occupancy=MetricScore(
                score=rounded_occupancy,
                label="Market Occupancy",
                description=(
                    f"Weighted avg occupancy across nearby facilities: {scores['weighted_avg_occupancy_pct']:.1f}%"
                    if scores["weighted_avg_occupancy_pct"] is not None
                    else "Insufficient occupancy data for this market"
                ),
                weight=10,
                rating=score_rating(rounded_occupancy),
            ),
            scenario_conservative=max(0, scores["overall"] - 12),
            scenario_optimistic=min(100, scores["overall"] + 12),
        ),
        recommendation=rec,
        population_gravity=gravity_map,
        recommendation_detail=(
            f"{mode_label} mode: {int(round(scores['target_pop'])):,} {target_desc} within the catchment"
            + (f", of whom {seniors_living_alone:,} live alone" if seniors_living_alone else "")
            + (f" and {seniors_below_200pct_poverty:,} are near poverty" if seniors_below_200pct_poverty and request.mission_mode else "")
            + f". Bed saturation ratio {scores['saturation_ratio']:.2f} against {len(facilities)} competing facilities."
        ),
        data_notes=[
            "Elder care module Phase 3 scaffold active.",
            "Competitor inventory sourced from local CMS Care Compare ingest cache when available.",
            "QRP quality-measures join is deferred until dataset ID confirmation.",
            "Occupancy signal uses PBJ-derived average daily census relative to certified beds when available.",
        ] + (
            [
                f"Workforce index: {workforce_details['note']}"
                + (f" Location quotient: {workforce_details['location_quotient']}x national average."
                   if workforce_details.get('location_quotient') else "")
                + (f" Avg weekly wage: ${workforce_details['avg_weekly_wage']:,}."
                   if workforce_details.get('avg_weekly_wage') else "")
            ] if workforce_details.get("available") else
            ["Workforce data: BLS QCEW data not available for this county."]
        ),
    )


class ElderCareModule(MinistryModule):
    def __init__(self):
        super().__init__(
            key="elder_care",
            display_name="Elder Care",
            supports_mission_toggle=True,
            analyzer=analyze_elder_care,
        )

    def weighting_profile(self, mission_mode: bool = False) -> Dict[str, float]:
        return MISSION_WEIGHTS if mission_mode else MARKET_WEIGHTS
