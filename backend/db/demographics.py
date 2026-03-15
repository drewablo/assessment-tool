"""Aggregate tract-level demographics into catchment-level summary.

Replaces the live Census API call in api/census.py with a DB-backed version.
"""

import logging
from typing import Optional

from geoalchemy2.shape import to_shape
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import CensusTract, CensusTractHistory
from db.queries import get_historical_tracts, get_tracts_by_county, get_tracts_by_state, get_tracts_in_catchment
from utils import bearing, direction_from_bearing

logger = logging.getLogger("db.demographics")

# CARA state-level Catholic percentage (same as v1)
CARA_CATHOLIC_PCT = {
    "CT": 0.43, "RI": 0.42, "MA": 0.38, "NJ": 0.37, "NY": 0.36, "PA": 0.33,
    "NH": 0.30, "IL": 0.29, "WI": 0.29, "NM": 0.28, "LA": 0.27, "CA": 0.26,
    "MN": 0.25, "OH": 0.22, "AZ": 0.22, "TX": 0.22, "HI": 0.21, "NE": 0.21,
    "MI": 0.20, "MT": 0.20, "IN": 0.18, "ND": 0.18, "MO": 0.18, "CO": 0.17,
    "KS": 0.17, "MD": 0.17, "FL": 0.17, "ME": 0.16, "NV": 0.16, "IA": 0.16,
    "KY": 0.15, "DE": 0.15, "VT": 0.14, "SD": 0.14, "VA": 0.13, "WY": 0.13,
    "OR": 0.12, "WA": 0.12, "ID": 0.11, "OK": 0.11, "GA": 0.10, "DC": 0.10,
    "TN": 0.10, "AK": 0.10, "AL": 0.10, "AR": 0.10, "NC": 0.10, "SC": 0.10,
    "WV": 0.10, "MS": 0.09, "UT": 0.09,
}

STATE_FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO",
    "09": "CT", "10": "DE", "11": "DC", "12": "FL", "13": "GA", "15": "HI",
    "16": "ID", "17": "IL", "18": "IN", "19": "IA", "20": "KS", "21": "KY",
    "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN",
    "28": "MS", "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND", "39": "OH",
    "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC", "46": "SD",
    "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA",
    "54": "WV", "55": "WI", "56": "WY",
}


async def aggregate_demographics(
    session: AsyncSession,
    lat: float,
    lon: float,
    radius_miles: float,
    state_fips: str,
    county_fips: str | None = None,
    isochrone_geojson: Optional[dict] = None,
) -> dict:
    """Aggregate tract-level demographics into a catchment-level summary.

    Returns a dict compatible with the existing AnalysisResponse.demographics fields.
    """
    tracts = await get_tracts_in_catchment(session, lat, lon, radius_miles, isochrone_geojson)

    used_county_fallback = False
    used_state_fallback = False
    if not tracts and county_fips:
        tracts = await get_tracts_by_county(session, county_fips, state_fips=state_fips)
        used_county_fallback = bool(tracts)
        logger.info(
            "Census catchment county fallback county_fips=%s state_fips=%s tract_hits=%s",
            county_fips,
            state_fips,
            len(tracts),
        )

    if not tracts and state_fips:
        tracts = await get_tracts_by_state(session, state_fips, limit=300)
        used_state_fallback = bool(tracts)
        logger.info(
            "Census catchment state fallback state_fips=%s tract_hits=%s",
            state_fips,
            len(tracts),
        )

    if not tracts:
        state_abbr = STATE_FIPS_TO_ABBR.get(state_fips, f"FIPS {state_fips}")
        logger.warning(
            "Census catchment lookup returned zero tracts after all fallbacks "
            "(lat=%.6f lon=%.6f radius_miles=%.2f state_fips=%s county_fips=%s). "
            "%s has not been ingested. "
            "Fix: run census pipeline with states=['%s'].",
            lat, lon, radius_miles, state_fips, county_fips,
            state_abbr, state_fips,
        )
        return _empty_demographics(state_fips)

    tract_geoids = [t.geoid for t in tracts if t.geoid]
    historical_rows = await get_historical_tracts(session, tract_geoids, vintage="2017") if tract_geoids else []

    # Simple summation for count fields, population-weighted for rates
    total_pop = 0
    total_under_18 = 0
    total_5_17 = 0
    total_under_5 = 0
    total_65_74 = 0
    total_75_plus = 0
    total_households = 0
    total_families = 0
    families_with_children = 0
    owner_occupied = 0
    renter_occupied = 0
    below_poverty = 0
    seniors_below_poverty = 0
    seniors_living_alone = 0

    # School enrollment (B14002) — for private school rate scoring
    total_enrolled_k_12 = 0
    total_enrolled_private_k_12 = 0

    # Income brackets (for addressable market calculation)
    income_brackets = {
        "under_10k": 0, "10k_15k": 0, "15k_25k": 0, "25k_35k": 0,
        "35k_50k": 0, "50k_75k": 0, "75k_100k": 0, "100k_150k": 0,
        "150k_200k": 0, "200k_plus": 0,
    }

    # Weighted median income (population-weighted average of tract medians)
    weighted_income_sum = 0
    weighted_income_pop = 0
    income_cvs = []

    for t in tracts:
        pop = t.total_population or 0
        total_pop += pop
        total_under_18 += t.population_under_18 or 0
        total_5_17 += t.population_5_17 or 0
        total_under_5 += t.population_under_5 or 0
        total_65_74 += t.population_65_74 or 0
        total_75_plus += t.population_75_plus or 0
        total_households += t.total_households or 0
        total_families += t.family_households or 0
        families_with_children += t.families_with_own_children or 0
        owner_occupied += t.owner_occupied or 0
        renter_occupied += t.renter_occupied or 0
        below_poverty += t.population_below_poverty or 0
        seniors_below_poverty += t.seniors_below_poverty or 0
        seniors_living_alone += t.seniors_living_alone or 0

        # School enrollment aggregation (B14002)
        total_enrolled_k_12 += t.enrolled_k_12 or 0
        total_enrolled_private_k_12 += t.enrolled_private_k_12 or 0

        # Income brackets
        income_brackets["under_10k"] += t.income_bracket_under_10k or 0
        income_brackets["10k_15k"] += t.income_bracket_10k_15k or 0
        income_brackets["15k_25k"] += t.income_bracket_15k_25k or 0
        income_brackets["25k_35k"] += t.income_bracket_25k_35k or 0
        income_brackets["35k_50k"] += t.income_bracket_35k_50k or 0
        income_brackets["50k_75k"] += t.income_bracket_50k_75k or 0
        income_brackets["75k_100k"] += t.income_bracket_75k_100k or 0
        income_brackets["100k_150k"] += t.income_bracket_100k_150k or 0
        income_brackets["150k_200k"] += t.income_bracket_150k_200k or 0
        income_brackets["200k_plus"] += t.income_bracket_200k_plus or 0

        if t.median_household_income and pop > 0:
            weighted_income_sum += t.median_household_income * pop
            weighted_income_pop += pop

        if t.income_cv is not None:
            income_cvs.append(t.income_cv)

    median_hh_income = None
    if weighted_income_pop > 0:
        median_hh_income = int(weighted_income_sum / weighted_income_pop)

    owner_pct = None
    occupied = owner_occupied + renter_occupied
    if occupied > 0:
        owner_pct = round(owner_occupied / occupied * 100.0, 1)

    historical_2017 = {}
    if historical_rows:
        hist_total_pop = sum(h.total_population or 0 for h in historical_rows)
        hist_school_age = sum(h.population_5_17 or 0 for h in historical_rows)
        hist_families = sum(h.families_with_own_children or 0 for h in historical_rows)
        hist_households = sum(h.total_households or 0 for h in historical_rows)

        weighted_hist_income = 0
        weighted_hist_pop = 0
        for h in historical_rows:
            pop = h.total_population or 0
            income = h.median_household_income or 0
            if pop > 0 and income > 0:
                weighted_hist_income += income * pop
                weighted_hist_pop += pop

        historical_2017 = {
            "total_population": hist_total_pop,
            "school_age_population": hist_school_age,
            "families_with_children": hist_families,
            "total_households": hist_households,
            "median_household_income": int(weighted_hist_income / weighted_hist_pop) if weighted_hist_pop > 0 else None,
        }

    # Estimate Catholic percentage
    state_abbr = STATE_FIPS_TO_ABBR.get(state_fips, "")
    estimated_catholic_pct = CARA_CATHOLIC_PCT.get(state_abbr, 0.12)

    # Data confidence based on tract count and income CV
    avg_cv = sum(income_cvs) / len(income_cvs) if income_cvs else 30.0
    if len(tracts) >= 15 and avg_cv < 12:
        data_confidence = "high"
    elif len(tracts) >= 5 and avg_cv < 28:
        data_confidence = "medium"
    else:
        data_confidence = "low"

    # The DB stores combined population_5_17.  The analysis code needs the
    # 5-to-11 / 12-to-17 split (7 year-groups vs 6 year-groups).
    pop_5_to_11 = int(round(total_5_17 * 7 / 13)) if total_5_17 else 0
    pop_12_to_17 = total_5_17 - pop_5_to_11

    seniors_65_plus_total = total_65_74 + total_75_plus
    if seniors_65_plus_total > 0 and seniors_living_alone == 0 and seniors_below_poverty == 0:
        logger.warning(
            "DB demographics contain seniors_65_plus but zero seniors_living_alone and seniors_below_poverty. "
            "This usually indicates legacy census ingest missing B11010/B17001 senior fields. "
            "state_fips=%s county_fips=%s tract_count=%d",
            state_fips,
            county_fips,
            len(tracts),
        )

    # High-income households ($100k+) — mirrors _HIGH_INCOME_VARS in census.py
    high_income_households = (
        income_brackets.get("100k_150k", 0)
        + income_brackets.get("150k_200k", 0)
        + income_brackets.get("200k_plus", 0)
    )

    # Build seniors_by_direction using tract centroids and bearing from analysis center
    seniors_by_direction: dict[str, dict] = {}
    dir_buckets: dict[str, dict[str, float]] = {
        d: {"seniors_65_plus": 0.0, "seniors_75_plus": 0.0,
            "seniors_living_alone": 0.0, "seniors_below_poverty": 0.0}
        for d in ("N", "NE", "E", "SE", "S", "SW", "W", "NW")
    }
    for t in tracts:
        if t.centroid is None:
            continue
        try:
            pt = to_shape(t.centroid)
            tract_lat, tract_lon = pt.y, pt.x
        except Exception:
            continue
        b = bearing(lat, lon, tract_lat, tract_lon)
        d = direction_from_bearing(b)
        s65 = (t.population_65_74 or 0) + (t.population_75_plus or 0)
        dir_buckets[d]["seniors_65_plus"] += s65
        dir_buckets[d]["seniors_75_plus"] += t.population_75_plus or 0
        dir_buckets[d]["seniors_living_alone"] += t.seniors_living_alone or 0
        dir_buckets[d]["seniors_below_poverty"] += t.seniors_below_poverty or 0

    for d, vals in dir_buckets.items():
        s65 = round(vals["seniors_65_plus"])
        alone = round(vals["seniors_living_alone"])
        seniors_by_direction[d] = {
            "seniors_65_plus": s65,
            "seniors_75_plus": round(vals["seniors_75_plus"]),
            "seniors_living_alone": alone,
            "seniors_below_poverty": round(vals["seniors_below_poverty"]),
            "isolation_ratio": round(alone / s65, 3) if s65 > 0 else None,
        }

    # Derive county_name from tracts (use first available county_fips → state lookup)
    county_name = None
    if tracts:
        first_county = tracts[0].county_fips
        first_state = tracts[0].state_fips
        state_abbr_local = STATE_FIPS_TO_ABBR.get(first_state, "")
        if first_county and state_abbr_local:
            county_name = f"FIPS {first_county}, {state_abbr_local}"

    # Income distribution in (midpoint, count) tuple format expected by analysis.py
    _BRACKET_MIDPOINTS = [
        ("under_10k", 5_000), ("10k_15k", 12_500), ("15k_25k", 20_000),
        ("25k_35k", 30_000), ("35k_50k", 42_500), ("50k_75k", 62_500),
        ("75k_100k", 87_500), ("100k_150k", 125_000), ("150k_200k", 175_000),
        ("200k_plus", 250_000),
    ]
    income_distribution = [
        (midpoint, income_brackets.get(key, 0))
        for key, midpoint in _BRACKET_MIDPOINTS
    ]

    return {
        "total_population": total_pop,
        "population_under_18": total_under_18,
        "school_age_population": total_5_17,
        "population_5_to_11": pop_5_to_11,
        "population_12_to_17": pop_12_to_17,
        "gravity_weighted_school_age_pop": total_5_17,
        "population_under_5": total_under_5,
        "seniors_65_plus": seniors_65_plus_total,
        "seniors_65_74": total_65_74,
        "seniors_75_plus": total_75_plus,
        "median_household_income": median_hh_income,
        "total_households": total_households,
        "family_households": total_families,
        "families_with_children": families_with_children,
        "owner_occupied_pct": owner_pct,
        "owner_occupied_units": owner_occupied,
        "total_housing_units": owner_occupied + renter_occupied if (owner_occupied + renter_occupied) > 0 else None,
        "high_income_households": high_income_households,
        "estimated_catholic_pct": estimated_catholic_pct,
        "data_geography": "state_fallback" if used_state_fallback else ("county_fallback" if used_county_fallback else "tract"),
        "data_confidence": data_confidence,
        "tract_count": len(tracts),
        "income_brackets": income_brackets,
        "income_distribution": income_distribution,
        "population_below_poverty": below_poverty,
        "seniors_below_200pct_poverty": seniors_below_poverty,
        "seniors_living_alone": seniors_living_alone,
        "seniors_by_direction": seniors_by_direction,
        "county_name": county_name,
        "owner_occupied": owner_occupied,
        "renter_occupied": renter_occupied,
        "private_school_enrolled": total_enrolled_private_k_12,
        "total_school_enrolled": total_enrolled_k_12,
        "historical_2017": historical_2017,
    }


async def get_trend_data(
    session: AsyncSession,
    geoids: list[str],
    current_vintage: str = "2022",
    historical_vintage: str = "2017",
) -> dict:
    """Compute demographic trends between two ACS vintages."""
    historical = await get_historical_tracts(session, geoids, historical_vintage)

    if not historical:
        return {"trend_label": "Unknown", "period": f"ACS {historical_vintage} → {current_vintage}"}

    hist_pop_5_17 = sum(h.population_5_17 or 0 for h in historical)
    hist_income = sum(
        (h.median_household_income or 0) * (h.total_population or 1)
        for h in historical
    )
    hist_pop_total = sum(h.total_population or 1 for h in historical)
    hist_median_income = hist_income / hist_pop_total if hist_pop_total else 0
    hist_families = sum(h.families_with_own_children or 0 for h in historical)

    return {
        "historical_school_age": hist_pop_5_17,
        "historical_median_income": int(hist_median_income),
        "historical_families": hist_families,
        "historical_vintage": historical_vintage,
    }


def _empty_demographics(state_fips: str) -> dict:
    """Return empty demographics dict when no tracts are found."""
    state_abbr = STATE_FIPS_TO_ABBR.get(state_fips, "")
    return {
        "total_population": 0,
        "population_under_18": 0,
        "school_age_population": 0,
        "population_5_to_11": 0,
        "population_12_to_17": 0,
        "gravity_weighted_school_age_pop": 0,
        "population_under_5": 0,
        "seniors_65_plus": 0,
        "seniors_65_74": 0,
        "seniors_75_plus": 0,
        "seniors_living_alone": 0,
        "seniors_below_200pct_poverty": 0,
        "seniors_by_direction": {},
        "median_household_income": None,
        "total_households": 0,
        "families_with_children": 0,
        "owner_occupied_pct": None,
        "owner_occupied_units": None,
        "total_housing_units": None,
        "high_income_households": 0,
        "estimated_catholic_pct": CARA_CATHOLIC_PCT.get(state_abbr, 0.12),
        "data_geography": "tract",
        "data_confidence": "low",
        "tract_count": 0,
        "income_brackets": {},
        "income_distribution": [],
        "county_name": None,
    }
