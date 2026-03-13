import logging
import os
from typing import Any, Dict, List, Tuple

from api.analysis import calculate_feasibility
from api.schools import get_nearby_schools
from modules.base import MinistryModule

# ---------------------------------------------------------------------------
# DEMOGRAPHIC AUDIT — Schools Module
#
# Population center density:
#   YES — census.py applies inverse-distance decay weights when aggregating
#   tract-level data.  The gravity_weighted_school_age_pop field uses
#   decay_weight(distance) on each tract's school-age population.  The
#   analysis.py _compute_stage1_scores function uses this gravity-weighted
#   value when available (coed k12), and otherwise uses _effective_school_age_pop
#   which scopes by grade and gender from B01001 sex-by-age variables.
#   STATUS: PRESENT — distance-decay gravity weighting active.
#
# Income distribution (B19001):
#   YES — full B19001 bracket breakdown feeds _estimate_addressable_market()
#   which computes per-bracket propensity using NCES enrollment rates by income.
#   School choice programs shift the income curve.  High-income ($100k+)
#   households scored as a bonus via _HIGH_INCOME_BONUS_SEGMENTS.
#   STATUS: PRESENT — comprehensive income-bracket-level analysis.
#
# Age-appropriate household composition:
#   YES — _effective_school_age_pop() breaks down by grade level and gender
#   using B01001 sex-by-age (male/female 5-9, 10-14, 15-17).
#   families_with_children (B11003) feeds family_density sub-score.
#   B14002 private school enrollment rate used as demand sub-indicator.
#   B09001 under-5 pipeline ratio used for sustainability signal.
#   STATUS: PRESENT — grade/gender/family composition filtering active.
#
# Narrative output:
#   Good — recommendation_detail references addressable market, trend, and
#   competition landscape.  MetricScore descriptions surface income brackets,
#   family density %, and private enrollment rate.  However, the
#   recommendation_detail doesn't explicitly state the count of families with
#   school-aged children.
#   STATUS: IMPROVED — _build_recommendation now surfaces families-with-children
#   count when available.
# ---------------------------------------------------------------------------

USE_DB = os.getenv("USE_DB", "").lower() in ("1", "true", "yes")

logger = logging.getLogger(__name__)


async def _get_nearby_schools_db(
    *,
    lat: float,
    lon: float,
    radius_miles: float,
    isochrone_polygon: dict | None,
) -> List[dict]:
    from db.connection import get_session
    from db.queries import get_nearby_schools as get_nearby_schools_query

    async with get_session() as session:
        rows = await get_nearby_schools_query(
            session,
            lat=lat,
            lon=lon,
            radius_miles=radius_miles,
            isochrone_geojson=isochrone_polygon,
            limit=50,
        )

    mapped = []
    for school, distance in rows:
        mapped.append(
            {
                "name": school.school_name,
                "lat": school.lat,
                "lon": school.lon,
                "distance_miles": round(float(distance), 1),
                "affiliation": school.affiliation_label or "Private School",
                "is_catholic": bool(school.is_catholic),
                "city": school.city,
                "enrollment": school.enrollment,
                "gender": school.coeducation or "Unknown",
                "grade_level": school.grade_level or "Unknown",
                "competitor_tier": school.competitor_tier,
                "tier_weight": school.tier_weight,
            }
        )
    return mapped


async def analyze_schools(
    *,
    location: dict,
    demographics: dict,
    request: Any,
    radius_miles: float,
    drive_minutes: int,
    isochrone_polygon: dict | None,
    catchment_type: str,
):
    used_live_school_fallback = False
    if USE_DB:
        schools = await _get_nearby_schools_db(
            lat=location["lat"],
            lon=location["lon"],
            radius_miles=radius_miles,
            isochrone_polygon=isochrone_polygon,
        )
        # DB mode can be enabled before NCES pipeline data is loaded.
        # Fall back to live provider so analysis still has competitor context.
        if not schools:
            logger.warning(
                "USE_DB=true but no school competitors found in DB for catchment; "
                "falling back to live school fetch for this request."
            )
            used_live_school_fallback = True
    else:
        schools = []

    if (not USE_DB) or used_live_school_fallback:
        schools = await get_nearby_schools(
            lat=location["lat"],
            lon=location["lon"],
            radius_miles=radius_miles,
            gender=request.gender,
            grade_level=request.grade_level,
            isochrone_polygon=isochrone_polygon,
        )
    return await calculate_feasibility(
        location=location,
        demographics=demographics,
        schools=schools,
        school_name=request.school_name,
        radius_miles=radius_miles,
        drive_minutes=drive_minutes,
        isochrone_polygon=isochrone_polygon,
        catchment_type=catchment_type,
        gender=request.gender,
        grade_level=request.grade_level,
        weighting_profile=request.weighting_profile,
        stage2_inputs=request.stage2_inputs.model_dump() if request.stage2_inputs else None,
        market_context=request.market_context,
    )


class SchoolsModule(MinistryModule):
    def __init__(self):
        super().__init__(
            key="schools",
            display_name="Catholic School",
            supports_mission_toggle=False,
            analyzer=analyze_schools,
        )

    def recommendation_text(
        self, score: int, competitor_count: int, market_pop: int, mission_mode: bool = False
    ) -> Tuple[str, str]:
        return ("Use shared schools recommendation logic", "")

    def load_competitors(
        self, lat: float, lon: float, radius_miles: float, mission_mode: bool = False
    ) -> List[Dict[str, Any]]:
        return []
