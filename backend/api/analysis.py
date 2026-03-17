"""
Market feasibility scoring engine.

Combines Census demographic data with NCES competitor school data
to produce a weighted feasibility score and recommendation.

Scoring methodology:
  - Market Size (35%): Income-first addressable market — base enrollment
                        propensity from household income distribution (NCES),
                        plus Catholic affiliation boost (CARA/NCEA)
  - Income Level (25%): Median household income & high-income household share,
                        with a bonus for states with strong school choice programs
  - Competition (25%): Two-dimensional — market validation (proven demand) and
                       saturation pressure (distance-decayed capacity vs. target pop)
  - Family Density (15%): Share of households with school-age children

Calibration source: NCEA 2024-2025 Annual Statistical Report on Schools,
Enrollment and Staffing (ISBN 978-1-55833-795-4). Key empirical anchors:
  - Modal Catholic school enrollment: 150–299 students (38.6% of all schools)
  - 39.3% of Catholic schools nationally have waiting lists — existing Catholic
    school presence is primarily a demand-validation signal, not saturation
  - 18% of students use parental choice programs; 50%+ in FL, OH, IN, OK, IA, AZ
  - Primary closure drivers: economic downturns, demographic decline, tuition increases

Scoring uses continuous piecewise-linear transforms instead of hard buckets,
eliminating cliff effects at threshold boundaries.

Competition uses inverse-distance weighting (weight = 1/max(0.5, dist)^1.5) so
that nearby schools count more than distant ones. It splits into two sub-scores:
  - Validation (60%): distance-decayed presence of Catholic schools (demand signal)
  - Saturation (40%): distance-decayed capacity relative to target population (pressure)
  Validation outweighs saturation because NCEA data shows 39.3% of Catholic schools
  have waiting lists — established Catholic school presence more often signals
  unmet demand than a fully saturated market.

Catholic population estimates use CARA (Georgetown) state-level data as a
baseline multiplier, then adjusted upward or downward based on the area's
actual private school enrollment rate (from Census ACS C14002). Areas with
high private school propensity are more likely to support Catholic school
enrollment; areas with low propensity are adjusted down. This is clearly
disclosed to users as an estimate.

Scenario bands (conservative/base/optimistic) reflect data confidence:
  - High confidence:   ±6 points
  - Medium confidence: ±12 points
  - Low confidence:    ±18 points
"""

import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from api.census import compute_trend
from models.schemas import (
    AnalysisResponse,
    BenchmarkPercentiles,
    DemographicData,
    DemographicTrend,
    EnrollmentForecast,
    ForecastPoint,
    DirectionSegment,
    CompetitorSchool,
    FeasibilityScore,
    HierarchicalScore,
    MetricScore,
    PopulationGravityMap,
)
from utils import bearing, decay_weight, direction_from_bearing, piecewise_linear

logger = logging.getLogger(__name__)

# CARA-based Catholic population percentage by state (% of total population)
# Source: CARA State of the Church Data; Georgetown University
CATHOLIC_PCT_BY_STATE = {
    "CT": 0.43, "RI": 0.42, "MA": 0.40, "NJ": 0.40, "NY": 0.37,
    "PA": 0.32, "IL": 0.32, "CA": 0.30, "WI": 0.29, "MN": 0.29,
    "MD": 0.28, "OH": 0.27, "MI": 0.27, "NH": 0.27, "ME": 0.27,
    "NM": 0.34, "TX": 0.32, "AZ": 0.25, "CO": 0.25, "WA": 0.23,
    "OR": 0.22, "FL": 0.26, "VA": 0.22, "GA": 0.15, "NC": 0.14,
    "SC": 0.12, "AL": 0.10, "MS": 0.12, "TN": 0.11, "KY": 0.12,
    "AR": 0.10, "LA": 0.32, "IN": 0.19, "IA": 0.24, "MO": 0.22,
    "KS": 0.19, "NE": 0.25, "SD": 0.25, "ND": 0.27, "MT": 0.22,
    "WY": 0.20, "ID": 0.14, "NV": 0.24, "UT": 0.09, "AK": 0.20,
    "HI": 0.27, "DE": 0.32, "VT": 0.27, "WV": 0.10, "OK": 0.11,
    "DC": 0.25,
}
NATIONAL_CATHOLIC_PCT = 0.21

# ~10.5% of K-12 students nationally attend private school (NCES Digest 2022)
NATIONAL_PRIVATE_SCHOOL_RATE = 0.105

# Scenario band half-width by confidence level
_CONFIDENCE_BAND = {"high": 6, "medium": 12, "low": 18}

_WEIGHTING_PROFILES = {
    "standard_baseline": {"market_size": 0.35, "income": 0.25, "competition": 0.25, "family_density": 0.15},
    "affordability_sensitive": {"market_size": 0.30, "income": 0.30, "competition": 0.20, "family_density": 0.20},
    "demand_primacy": {"market_size": 0.40, "income": 0.20, "competition": 0.25, "family_density": 0.15},
}


# ---------------------------------------------------------------------------
# Market-size scoring — ratio-based model.
#
# The score is based on *market depth ratio*:
#   market_depth_ratio = total_addressable_market / reference_enrollment
#
# where reference_enrollment is the NCEA modal enrollment for the school type
# being analyzed (NCEA 2024-2025 Annual Statistical Report).  This ensures
# the same geographic market scores comparably regardless of gender/grade
# filter — a girls' high school in an affluent suburb is NOT a poor market
# just because the absolute addressable population is smaller.
#
# Reference enrollments (coed, gendered):
#   k5:          (200, 150)    — Elementary: modal 150-250
#   k8:          (250, 175)    — K-8: modal 200-300
#   high_school: (350, 225)    — High school: modal 300-450
#   k12:         (400, 275)    — Combined K-12: modal 350-500
#
# Calibration anchors (ratio-based, 15-mile suburban catchment):
#
#   Wealthy Chicago suburb (coed K-12):
#     addressable ~4,024 / 400 = ratio 10.1 → score ~91 → +8 trend = 99 ✓
#
#   Affluent Fairfield County CT (coed K-12):
#     addressable ~3,930 / 400 = 9.8 → score ~90 → -10 declining = 80 ✓
#
#   Working-class South Boston (coed K-12, urban):
#     addressable ~1,362 / 400 = 3.4 → score ~74 → -4 urban adj = ~70 ✓
#
#   Rural Alabama (coed K-12):
#     addressable ~382 / 400 = 0.96 → score ~35 → -10 declining = 25 ✓
#     (+4 rural adj = ~29)
#
#   Affluent Philly suburb (girls HS):
#     addressable ~629 / 225 = 2.8 → score ~68 ✓
# ---------------------------------------------------------------------------
_REFERENCE_ENROLLMENT = {
    # (coed, gendered) — NCEA 2024-2025 modal enrollments
    # Gendered variants for high_school/k12 are intentionally near the lower
    # edge of NCEA IQR (not median) because this model targets minimum
    # sustainable enrollment floors used for viability screening, not central tendency of
    # long-established institutions with larger legacy footprints.
    "k5": (200, 150),
    "k8": (250, 175),
    "high_school": (350, 225),
    "k12": (400, 275),
}

_MARKET_DEPTH_RATIO_SEGMENTS = [
    # market_depth_ratio → base score (0-100)
    # ratio < 0.5: can't fill even half a school → poor
    # ratio ~1.0: barely enough for one school → weak
    # ratio 2-3: healthy depth, can sustain enrollment + waitlist → moderate-strong
    # ratio 4+: deep market, strong demand signal → strong to very strong
    (0.0, 5),
    (0.3, 12),
    (0.7, 22),
    (1.0, 35),
    (1.5, 48),
    (2.0, 58),
    (3.0, 72),
    (5.0, 84),
    (8.0, 93),
    (15.0, 98),
]

# After scoring the ratio, apply a small context adjustment:
# urban markets have more alternatives (slightly harder),
# rural markets have fewer alternatives (slightly easier).
_MARKET_CONTEXT_ADJUSTMENT = {
    "urban": -8,
    "suburban": 0,
    "rural": +4,
}

_CARA_DECLINE_MARKET_SIZE_PENALTY = -24
_CARA_DECLINE_NEGATIVE_TREND_EXTRA_PENALTY = -8

_INCOME_SEGMENTS = [
    (20_000, 8), (35_000, 22), (50_000, 40), (65_000, 55),
    (80_000, 68), (100_000, 80), (130_000, 90), (175_000, 97),
]

_HIGH_INCOME_BONUS_SEGMENTS = [
    # Threshold raised to $100k+ (see census.py); fewer households reach this
    # level than the old $75k+ threshold, so breakpoints are shifted down.
    (0.0, 0), (0.10, 0), (0.20, 5), (0.35, 12), (0.55, 18),
]

_FAMILY_DENSITY_SEGMENTS = [
    (0, 8), (8, 18), (15, 32), (25, 55), (35, 73), (45, 88), (55, 97),
]

# ---------------------------------------------------------------------------
# Private school enrollment rate scoring (Census B14002)
# ---------------------------------------------------------------------------
# The local private school enrollment rate is a direct measure of the area's
# propensity to choose private education.  National average is ~10.5% (NCES).
# Areas with higher rates demonstrate stronger private-school demand.
_PRIVATE_ENROLLMENT_RATE_SEGMENTS = [
    # (private_enrollment_pct, score)
    (0.0, 5),
    (3.0, 15),
    (6.0, 30),
    (10.5, 50),   # National average → neutral
    (15.0, 68),
    (20.0, 82),
    (30.0, 95),
]

# ---------------------------------------------------------------------------
# Under-5 pipeline scoring (kindergarten pipeline indicator)
# ---------------------------------------------------------------------------
# Ratio of under-5 population to school-age (5-17) population.  A high ratio
# means the kindergarten pipeline is strong relative to current school-age
# cohort — positive sustainability signal.  National average ~0.33 (roughly
# 5 birth years vs 13 school-age years).
_PIPELINE_RATIO_SEGMENTS = [
    # (under_5 / pop_5_17 ratio, score)
    (0.0, 5),
    (0.15, 18),
    (0.22, 32),
    (0.30, 50),   # ~national average → neutral
    (0.38, 68),
    (0.48, 82),
    (0.60, 95),
]

# ---------------------------------------------------------------------------
# School choice program state tiers — NCEA 2024-2025, p. 36
# In choice states, vouchers/scholarships reduce the effective tuition burden
# on families, meaning income thresholds matter less for enrollment viability.
# ---------------------------------------------------------------------------
# States with accelerated Catholic population decline since 2015 (CARA data).
# The CATHOLIC_PCT_BY_STATE values above are CARA ~2020 estimates; these states
# have seen continued post-2020 decline and the % may now be optimistic.
# A data_note is appended to warn users when analyzing these markets.
_CARA_HIGH_DECLINE_STATES = {
    # New England — fastest-declining Catholic share in the country
    "CT", "MA", "ME", "NH", "RI", "VT",
    # Pacific Northwest — historically low density, accelerating decline post-2015
    "OR", "WA",
    # Northeastern/Rust Belt Catholic markets with notable secularization and
    # diocesan contraction pressures in the last decade
    "PA",
}

_CARA_DECLINE_CATHOLIC_PCT_FACTOR = 0.50

# ---------------------------------------------------------------------------
# States where >50% of Catholic school students use choice programs (NCEA 2024-2025)
_STRONG_CHOICE_STATES = {"FL", "OH", "IN", "OK", "IA", "AZ"}

# States with established programs (32 states + DC per NCEA 2024-2025)
_ESTABLISHED_CHOICE_STATES = {
    "AR", "CO", "GA", "ID", "IL", "KS", "KY", "LA", "MD", "ME", "MN",
    "MS", "MT", "NC", "NH", "NJ", "NV", "PA", "SC", "TN", "UT", "VA",
    "VT", "WI", "WV",
}


def _school_choice_bonus(state_abbr: str) -> float:
    """
    Income score bonus for states with parental school choice programs.
    These programs (vouchers, scholarships, tax-credit scholarships) reduce
    the effective tuition cost to families, lowering the income barrier.
    Source: NCEA 2024-2025 Annual Statistical Report.
    """
    if state_abbr in _STRONG_CHOICE_STATES:
        return 12.0   # >50% student utilization per NCEA 2024-2025
    if state_abbr in _ESTABLISHED_CHOICE_STATES:
        return 5.0    # established program, moderate affordability boost
    return 0.0


def _adjusted_catholic_pct(
    state_catholic_pct: float,
    private_school_enrolled: int,
    total_school_enrolled: int,
    state_abbr: str = "",
) -> tuple:
    """
    Adjust the state-level Catholic % by the local private school propensity.

    If the local area sends 20% of its students to private school vs. the
    national 10.5%, the Catholic population estimate is proportionally higher
    (capped at √2.5x). Areas with low private school enrollment are adjusted
    down (floor at √0.3x). The square-root dampens extreme swings.

    Returns (adjusted_pct, local_private_rate_or_None).
    """
    decline_factor = _CARA_DECLINE_CATHOLIC_PCT_FACTOR if state_abbr in _CARA_HIGH_DECLINE_STATES else 1.0
    adjusted_state_pct = state_catholic_pct * decline_factor

    if not total_school_enrolled or total_school_enrolled <= 0:
        return adjusted_state_pct, None

    local_rate = private_school_enrolled / total_school_enrolled
    ratio = local_rate / NATIONAL_PRIVATE_SCHOOL_RATE
    # Dampen with square root and cap at reasonable bounds
    adjustment = max(math.sqrt(0.3), min(math.sqrt(2.5), math.sqrt(ratio)))
    return min(0.90, adjusted_state_pct * adjustment), local_rate


# ---------------------------------------------------------------------------
# Income-first addressable market model
# ---------------------------------------------------------------------------
# Base enrollment propensity by household income bracket.
# Calibrated from CPS October School Enrollment Supplement (Census Bureau,
# Table 8) and NCES Digest of Education Statistics Table 206.20.
#
# Key empirical anchors:
#   - Families $75k+: ~11% private school enrollment (CAPE/Census CPS 2018)
#   - National overall: ~10.5% private school rate (NCES Digest 2022)
#   - Private school poverty rate 11% vs 17% public (NCES 2021)
#   - Higher income → monotonically higher private school enrollment
#
# These are ACTUAL enrollment rates, not consideration/intent.
_INCOME_PROPENSITY_SEGMENTS = [
    # (household_income, private_school_enrollment_pct)
    # piecewise_linear returns the % value; divide by 100 before use.
    (0, 3.0),
    (20_000, 4.0),
    (35_000, 6.0),
    (50_000, 9.0),
    (75_000, 13.0),
    (100_000, 18.0),
    (150_000, 21.0),
    (200_000, 31.0),
]

# Catholic affiliation boost: additive percentage points on top of income-based
# propensity. A Catholic family at $100k has propensity 18% + 7% = 25%.
# Calibrated so that high-income Catholic families reach ~35-40% combined
# propensity (consistent with NCEA enrollment-to-population ratios in strong
# Catholic markets) while low-income Catholic families see a modest but
# insufficient-to-overcome-affordability lift (4% + 7% = 11%).
_CATHOLIC_BOOST = 0.07

# Non-Catholic draw rate: ~12% of Catholic school students nationally are non-Catholic
# (NCES Private School Universe Survey). These families are part of the addressable
# market but are weighted lower than Catholic families in competition scoring.
_NON_CATHOLIC_DRAW_RATE = 0.12

# School choice program income shifts — in choice states, vouchers/scholarships
# reduce effective tuition cost, so families at lower incomes behave like
# higher-income families for enrollment propensity purposes.
_CHOICE_INCOME_SHIFT = {
    "strong": 22_500,   # $50k family behaves like $72.5k family
    "established": 10_000,  # $50k family behaves like $60k family
    "none": 0,
}

_REGIONAL_WAITLIST_PREVALENCE = {
    "northeast": 54.0,
    "midwest": 41.0,
    "south": 24.0,
    "west": 33.0,
}
_NATIONAL_WAITLIST_BASELINE = 39.3
_REGION_WAITLIST_NOTE_DELTA_PCT = 10.0

_STATE_TO_REGION = {
    "CT": "northeast", "ME": "northeast", "MA": "northeast", "NH": "northeast", "RI": "northeast", "VT": "northeast",
    "NJ": "northeast", "NY": "northeast", "PA": "northeast",
    "IL": "midwest", "IN": "midwest", "IA": "midwest", "KS": "midwest", "MI": "midwest", "MN": "midwest", "MO": "midwest",
    "NE": "midwest", "ND": "midwest", "OH": "midwest", "SD": "midwest", "WI": "midwest",
    "DE": "south", "DC": "south", "FL": "south", "GA": "south", "MD": "south", "NC": "south", "SC": "south", "VA": "south", "WV": "south",
    "AL": "south", "KY": "south", "MS": "south", "TN": "south", "AR": "south", "LA": "south", "OK": "south", "TX": "south",
    "AZ": "west", "CO": "west", "ID": "west", "MT": "west", "NV": "west", "NM": "west", "UT": "west", "WY": "west",
    "AK": "west", "CA": "west", "HI": "west", "OR": "west", "WA": "west",
}


def _choice_state_tier(state_abbr: str) -> str:
    """Return the school choice program tier for a state."""
    if state_abbr in _STRONG_CHOICE_STATES:
        return "strong"
    if state_abbr in _ESTABLISHED_CHOICE_STATES:
        return "established"
    return "none"


def _estimate_addressable_market(
    effective_pop: int,
    catholic_pct: float,
    income_distribution: list,
    state_abbr: str = "",
) -> dict:
    """
    Income-first addressable market estimation.

    Base enrollment probability comes from household income distribution
    (NCES private school enrollment rates by income bracket). Catholic
    affiliation provides an additive boost. School choice programs shift
    the income curve leftward (lowering the effective affordability barrier).

    Args:
        effective_pop: School-age population scoped to grade/gender.
        catholic_pct: Adjusted Catholic % for this area.
        income_distribution: List of (midpoint_income, household_count) tuples
            from ACS B19001. May be empty for fallback.
        state_abbr: Two-letter state abbreviation for school choice lookup.

    Returns dict with market breakdown and total addressable market.
    """
    choice_tier = _choice_state_tier(state_abbr)
    income_shift = _CHOICE_INCOME_SHIFT.get(choice_tier, 0)

    total_hh = sum(count for _, count in income_distribution) if income_distribution else 0

    if total_hh > 0 and income_distribution:
        # Per-bracket propensity calculation
        income_qualified_base = 0.0
        for midpoint, hh_count in income_distribution:
            # Apply school choice income shift — raises effective income for
            # propensity lookup (a $50k family in a strong choice state looks
            # up propensity at $72.5k because vouchers cover the gap).
            shifted_income = midpoint + income_shift
            propensity_pct = piecewise_linear(shifted_income, _INCOME_PROPENSITY_SEGMENTS)
            propensity = propensity_pct / 100.0
            bracket_share = hh_count / total_hh
            income_qualified_base += bracket_share * effective_pop * propensity
        income_qualified_base = int(income_qualified_base)
    else:
        # Fallback: no income distribution data — use a flat national rate
        income_qualified_base = int(effective_pop * NATIONAL_PRIVATE_SCHOOL_RATE)

    # Catholic affiliation boost: additive enrollment probability for Catholic
    # families beyond what income alone would predict.
    catholic_boost_contribution = int(effective_pop * catholic_pct * _CATHOLIC_BOOST)

    total_addressable = income_qualified_base + catholic_boost_contribution

    return {
        "income_qualified_base": income_qualified_base,
        "catholic_boost_contribution": catholic_boost_contribution,
        "catholic_boost_rate": _CATHOLIC_BOOST,
        "choice_tier": choice_tier,
        "choice_income_shift": income_shift,
        "total_addressable_market": total_addressable,
    }


STATE_FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY",
}




def _normalize_school_name(name: str) -> str:
    """Normalize school names for conservative self-school exclusion matching."""
    normalized = "".join(ch.lower() if ch.isalnum() else " " for ch in (name or ""))
    return " ".join(normalized.split())


def _rating(score: float) -> str:
    if score >= 75:
        return "strong"
    if score >= 55:
        return "moderate"
    if score >= 35:
        return "weak"
    return "poor"


def _trend_score_adjustment(trend_label: str) -> float:
    """
    Apply a ±point adjustment to the market size score based on the 5-year
    demographic trend (ACS 2017 → 2022). Trajectory matters: a market in
    demographic decline today will be meaningfully smaller by the time a
    school reaches a stable enrollment trajectory over a multi-year horizon.

    Growing  → +8 pts  (school-age pop rising, supports enrollment ramp-up)
    Stable   →  0 pts  (no adjustment)
    Mixed    → -4 pts  (conflicting signals — population/income diverging)
    Declining→ -10 pts (shrinking market; increased enrollment risk)
    Unknown  →  0 pts  (no trend data available)
    """
    return {
        "Growing": 8.0,
        "Stable": 0.0,
        "Mixed": -4.0,
        "Declining": -10.0,
        "Unknown": 0.0,
    }.get(trend_label, 0.0)


def _score_market_size(
    market_depth_ratio: float,
    market_context: str = "suburban",
) -> float:
    """Score market size from the market depth ratio (addressable_market / reference_enrollment).

    A small context adjustment is applied after scoring:
      urban -8, suburban 0, rural +4.
    """
    base = piecewise_linear(market_depth_ratio, _MARKET_DEPTH_RATIO_SEGMENTS)
    adjustment = _MARKET_CONTEXT_ADJUSTMENT.get(market_context, 0)
    return min(100.0, max(0.0, base + adjustment))


def _score_income(median_income: Optional[int], high_income_pct: float) -> float:
    """
    Continuous income score combining median household income and the share of
    high-income households (≥$75k). Catholic school tuition typically runs $8k–$18k/yr.
    """
    if not median_income:
        return 50.0  # Unknown — neutral
    base = piecewise_linear(median_income, _INCOME_SEGMENTS)
    bonus = piecewise_linear(high_income_pct, _HIGH_INCOME_BONUS_SEGMENTS)
    return min(100.0, base + bonus)


# Breakpoints for competition sub-scores
_VALIDATION_WEIGHT_SEGMENTS = [
    (0.0, 0), (0.5, 28), (1.5, 48), (3.0, 63), (6.0, 78), (12.0, 90),
]
_UNSERVED_MARKET_SEGMENTS = [
    (0, 18), (500, 33), (1_000, 46), (2_000, 58), (5_000, 70),
]
_SATURATION_RATIO_SEGMENTS = [
    (0.0, 95), (0.05, 88), (0.15, 75), (0.30, 58), (0.50, 40),
    (0.75, 25), (1.0, 15),
]

# Private market maturity floor: in markets with high private school
# enrollment, the demand for private education is already proven even if
# Catholic-specific presence is thin.  This floors the validation score so
# that elite-private markets (Main Line, Fairfield County, etc.) aren't
# mis-read as "unvalidated".
_PRIVATE_MATURITY_FLOOR_SEGMENTS = [
    (0.00, 0), (0.08, 0), (0.12, 45), (0.18, 60), (0.25, 72), (0.35, 80),
]

# ---------------------------------------------------------------------------
# Stage 2 KPI scoring breakpoints (piecewise-linear, same philosophy as Stage 1)
# Each segment list maps a raw metric value → 0-100 score.
# Non-monotone curves (sweet-spot metrics) are handled naturally by piecewise
# interpolation — no separate "higher vs lower is better" flag needed.
# ---------------------------------------------------------------------------
_S2_TUITION_RATIO_SEGMENTS = [
    # Higher is better; healthy ≥ 75%. Parish-subsidised models may score
    # adequately at 60–70% but signal structural subsidy dependency below that.
    (0.00, 5), (0.35, 18), (0.50, 38), (0.60, 55),
    (0.70, 70), (0.75, 80), (0.85, 90), (1.00, 95),
]
_S2_OPEX_PER_STUDENT_SEGMENTS = [
    # Lower is better; healthy ≤ $12k (NCEA avg ≈ $10k).
    (0, 100), (6_000, 95), (10_000, 85), (12_000, 75),
    (14_000, 55), (16_000, 35), (20_000, 18), (30_000, 5),
]
_S2_OPERATION_GAP_SEGMENTS = [
    # opex_per_student − tuition_per_student.  Sweet spot: −$500 to +$1 000
    # (slight surplus to modest subsidy need). Very negative = unsustainably
    # low operating spend; very positive = heavily subsidy-dependent.
    (-8_000, 45), (-2_000, 65), (-500, 85), (500, 90),
    (1_000, 82), (2_000, 65), (3_500, 40), (6_000, 15),
]
_S2_FUNDRAISING_RATIO_SEGMENTS = [
    # fundraising / total_income.  Sweet spot: 3–12%.
    # Too low = no diversification; too high = over-reliant on campaigns.
    (0.00, 22), (0.02, 48), (0.03, 68), (0.07, 88),
    (0.12, 82), (0.20, 55), (0.30, 28), (0.50, 10),
]
_S2_PAYROLL_PCT_SEGMENTS = [
    # payroll / total_expenses.  Sweet spot: 55–72%.
    # Below 55% may signal understaffing; above 72% leaves little margin
    # for facilities, curriculum, and contingency.
    (0.30, 18), (0.48, 50), (0.55, 80), (0.63, 90),
    (0.72, 82), (0.80, 58), (0.90, 28),
]
_S2_SURPLUS_DEFICIT_SEGMENTS = [
    # Average annual surplus (positive) or deficit (negative).
    # Continuous curve replaces the hard 35/65/90 buckets.
    (-500_000, 5), (-200_000, 22), (-100_000, 38), (-50_000, 55),
    (0, 72), (50_000, 84), (150_000, 92), (500_000, 97),
]
_S2_INVESTMENT_TRANSFERS_SEGMENTS = [
    # Annual endowment/reserve draws or deposits.  Sweet spot: −$50k to +$150k.
    # Large negative = significant endowment draw (sustainability risk);
    # very large positive = unusual capital movement worth investigation.
    (-500_000, 8), (-200_000, 28), (-50_000, 72), (75_000, 90),
    (150_000, 82), (300_000, 52), (600_000, 22),
]
_S2_ONE_TIME_INCOME_SEGMENTS = [
    # One-time/non-recurring income.  Lower recurrence = healthier base.
    # Penalises schools that routinely rely on windfalls to stay solvent.
    (0, 90), (25_000, 86), (50_000, 78), (100_000, 60),
    (200_000, 38), (400_000, 18), (800_000, 5),
]

# ---------------------------------------------------------------------------
# Grade-span overrides for payroll % and tuition ratio.
# Elementary schools are more generalist (lower payroll %), while high schools
# employ specialists, counselors, and coaches (higher payroll % is normal).
# Tuition coverage ratios also differ by grade span.
# ---------------------------------------------------------------------------
_S2_PAYROLL_PCT_SEGMENTS_K5 = [
    # Elementary: lean staffing, generalist model — payroll% peaks slightly lower
    (0.28, 12), (0.46, 42), (0.54, 75), (0.64, 90),
    (0.74, 80), (0.82, 52), (0.90, 22),
]
_S2_PAYROLL_PCT_SEGMENTS_HS = [
    # High school: specialists, coaches, counselors — higher payroll% is healthy
    (0.38, 18), (0.52, 50), (0.60, 80), (0.70, 90),
    (0.80, 80), (0.88, 54), (0.96, 24),
]
# K-8 / K-12 uses the default _S2_PAYROLL_PCT_SEGMENTS

_S2_TUITION_RATIO_SEGMENTS_K5 = [
    # Elementary: lower absolute cost base; tuition coverage typically higher
    (0.00, 5), (0.30, 15), (0.48, 38), (0.60, 58),
    (0.72, 74), (0.78, 84), (0.88, 93), (1.00, 97),
]
_S2_TUITION_RATIO_SEGMENTS_HS = [
    # High school: higher facility and program costs; somewhat lower coverage is expected
    (0.00, 8), (0.35, 22), (0.48, 40), (0.58, 57),
    (0.66, 70), (0.72, 79), (0.80, 87), (1.00, 94),
]
# K-8 / K-12 uses the default _S2_TUITION_RATIO_SEGMENTS

_S2_PAYROLL_SEGS_BY_GRADE = {
    "k5": _S2_PAYROLL_PCT_SEGMENTS_K5,
    "k8": _S2_PAYROLL_PCT_SEGMENTS,    # baseline
    "high_school": _S2_PAYROLL_PCT_SEGMENTS_HS,
    "k12": _S2_PAYROLL_PCT_SEGMENTS,   # baseline
}
_S2_TUITION_SEGS_BY_GRADE = {
    "k5": _S2_TUITION_RATIO_SEGMENTS_K5,
    "k8": _S2_TUITION_RATIO_SEGMENTS,  # baseline
    "high_school": _S2_TUITION_RATIO_SEGMENTS_HS,
    "k12": _S2_TUITION_RATIO_SEGMENTS, # baseline
}



def _score_competition(
    schools: List[dict],
    est_catholic_school_age: int,
    school_lat: Optional[float] = None,
    school_lon: Optional[float] = None,
    population_by_direction: Optional[Dict[str, int]] = None,
    local_private_rate: Optional[float] = None,
) -> Tuple[float, float, float]:
    """
    Two-dimensional competition score with distance-decay weighting.

    Returns (combined_score, validation_score, saturation_score).

    Market validation (60%): distance-decayed presence of Catholic schools,
      floored by private-market maturity when the local private enrollment
      rate is high (e.g. Main Line, Fairfield County).  In elite-private
      markets the demand for private education is already proven even if
      Catholic-specific presence is thin.

    Saturation pressure (40%): distance-decayed capacity (enrollment) relative
      to target Catholic school-age population.  In markets with high private
      enrollment, non-Catholic tier weights are boosted to reflect that elite
      secular schools compete directly for the same affluent families.

    Each competitor's weight = 1 / max(0.5, distance_miles)^1.5
    """
    private_rate = local_private_rate or 0.0
    catholic_schools = [s for s in schools if s.get("is_catholic")]

    if not catholic_schools:
        # Unserved market — validation is low but saturation is nil
        val_score = piecewise_linear(est_catholic_school_age, _UNSERVED_MARKET_SEGMENTS)
        # Even with no Catholic schools, a mature private market proves demand
        if private_rate > 0.08:
            maturity_floor = piecewise_linear(private_rate, _PRIVATE_MATURITY_FLOOR_SEGMENTS)
            val_score = max(val_score, maturity_floor)
        sat_score = 90.0  # No saturation whatsoever
        combined = val_score * 0.6 + sat_score * 0.4
        return combined, val_score, sat_score

    direction_total_pop = sum((population_by_direction or {}).values()) if population_by_direction else 0

    # In markets where private enrollment is high, non-Catholic schools
    # compete more directly for the same families.  Scale their tier weights
    # up so saturation reflects real competitive pressure.
    # Below 10% private rate → no boost (normal market).
    # At 20%+ private rate → non-Catholic weights approach full (1.0).
    if private_rate > 0.10:
        # Linear ramp: at 10% → multiplier 1.0, at 25% → multiplier 2.25
        # Effective tier weight = min(1.0, base_tier * multiplier)
        _intensity_multiplier = min(2.5, 1.0 + (private_rate - 0.10) / 0.12)
    else:
        _intensity_multiplier = 1.0

    def _competitor_weight(competitor: dict) -> float:
        weight = decay_weight(competitor["distance_miles"])
        if (
            population_by_direction
            and school_lat is not None
            and school_lon is not None
            and direction_total_pop > 0
        ):
            b = bearing(school_lat, school_lon, competitor["lat"], competitor["lon"])
            direction = direction_from_bearing(b)
            direction_pop = population_by_direction.get(direction, 0)
            direction_share = direction_pop / direction_total_pop if direction_total_pop > 0 else 0.0
            multiplier = min(1.5, 1.0 + direction_share)
            weight *= multiplier
        # Apply competitor tier weight — Catholic schools get full weight,
        # secular/religious schools get reduced weight, niche schools get minimal.
        # In high-private-enrollment markets, non-Catholic weights are boosted.
        tier_weight = competitor.get("tier_weight", 0.4)
        if not competitor.get("is_catholic") and _intensity_multiplier > 1.0:
            tier_weight = min(1.0, tier_weight * _intensity_multiplier)
        weight *= tier_weight
        return weight

    # Sum of distance-decayed weights (presence signal)
    total_weight = sum(_competitor_weight(s) for s in catholic_schools)

    # Validation: how strong is the distance-decayed Catholic school presence?
    val_score = piecewise_linear(total_weight, _VALIDATION_WEIGHT_SEGMENTS)

    # In high-private-enrollment markets, floor validation to reflect that
    # the demand for private education is already proven.
    if private_rate > 0.08:
        maturity_floor = piecewise_linear(private_rate, _PRIVATE_MATURITY_FLOOR_SEGMENTS)
        val_score = max(val_score, maturity_floor)

    # Saturation: distance-decayed capacity vs. target population
    # Fall back to 250 students/school if enrollment data is missing
    # All schools contribute to saturation pressure, weighted by tier
    weighted_capacity = sum(
        (s.get("enrollment") or 250) * _competitor_weight(s)
        for s in schools
    )
    if est_catholic_school_age > 0:
        saturation_ratio = weighted_capacity / est_catholic_school_age
    else:
        saturation_ratio = 1.0

    sat_score = piecewise_linear(saturation_ratio, _SATURATION_RATIO_SEGMENTS)

    # Validation outweighs saturation: NCEA 2024-2025 shows 39.3% of schools
    # have waiting lists, meaning existing Catholic school presence is more
    # often a demand-validation signal than evidence of market saturation.
    combined = val_score * 0.6 + sat_score * 0.4
    return combined, val_score, sat_score


def _score_private_enrollment_rate(
    private_school_enrolled: int,
    total_school_enrolled: int,
) -> Tuple[float, Optional[float]]:
    """
    Score the local private school enrollment rate from Census B14002.

    Returns (score, local_rate_pct_or_None).
    A higher private school enrollment rate indicates stronger local demand
    for private education — a direct demand signal beyond income/Catholic proxies.
    """
    if total_school_enrolled <= 0:
        return 50.0, None  # No data → neutral
    local_rate_pct = (private_school_enrolled / total_school_enrolled) * 100
    score = piecewise_linear(local_rate_pct, _PRIVATE_ENROLLMENT_RATE_SEGMENTS)
    return min(100.0, max(0.0, score)), local_rate_pct


def _score_pipeline_ratio(
    population_under_5: int,
    school_age_pop_5_17: int,
) -> Tuple[float, Optional[float]]:
    """
    Score the kindergarten pipeline strength: under-5 population relative to
    current school-age (5-17) population.

    A high ratio means strong incoming cohorts relative to the current
    school-age base — a positive sustainability signal for schools.

    Returns (score, pipeline_ratio_or_None).
    """
    if school_age_pop_5_17 <= 0:
        return 50.0, None  # No data → neutral
    ratio = population_under_5 / school_age_pop_5_17
    score = piecewise_linear(ratio, _PIPELINE_RATIO_SEGMENTS)
    return min(100.0, max(0.0, score)), ratio


def _score_family_density(
    families_with_children: Optional[int],
    total_households: Optional[int],
) -> float:
    """Continuous piecewise-linear score from % of households with children."""
    if families_with_children is None or total_households is None or total_households == 0:
        return 50.0  # Unknown — neutral
    pct = families_with_children / total_households * 100
    return piecewise_linear(pct, _FAMILY_DENSITY_SEGMENTS)


def _data_confidence(demographics: dict) -> str:
    """
    Estimate data confidence based on ACS income margin of error and data geography.

    For tract-level data: uses the population-weighted coefficient of variation
    of median household income (MOE / (1.645 * estimate)). Falls back to
    tract count as a proxy when MOE data is absent.

    For county-level fallback data: always "medium" (less geographic precision).

    Returns "high", "medium", or "low".
    """
    if demographics.get("data_geography") == "county":
        return "medium"

    income_cv = demographics.get("income_moe_pct")

    if income_cv is None:
        # Proxy: tract count
        tract_count = demographics.get("tract_count", 0)
        if tract_count >= 15:
            return "high"
        if tract_count >= 5:
            return "medium"
        return "low"

    # CV thresholds (income MOE relative to the estimate)
    if income_cv < 0.12:
        return "high"
    if income_cv < 0.28:
        return "medium"
    return "low"


def _scenario_scores(overall: int, confidence: str) -> Tuple[int, int]:
    """
    Compute (conservative, optimistic) scenario scores based on data confidence.
    The band represents a ±N-point range around the base score.
    """
    band = _CONFIDENCE_BAND.get(confidence, 12)
    return max(0, overall - band), min(100, overall + band)


# Human-readable labels used in descriptions
_GRADE_LEVEL_AGES = {
    "k5": "ages 5–11",
    "k8": "ages 5–14",
    "high_school": "ages 14–17",
    "k12": "ages 5–17",
}
_GENDER_LABELS = {
    "boys": "boys",
    "girls": "girls",
    "coed": "children",
}


def _effective_school_age_pop(demographics: dict, gender: str, grade_level: str) -> int:
    """
    Return the population count that is actually relevant to the school's
    grade level and gender, using Census B01001 sex-by-age variables.

    Grade-level → age-range mapping (using ACS 5-year buckets):
      k5          → 5–11  : male/female 5-9 + 2/5 of 10-14
      k8          → 5–14  : male/female 5-9 + 10-14
      high_school → 14–17 : male/female 1/5 of 10-14 + 15-17
      k12         → 5–17  : male/female 5-9 + 10-14 + 15-17

    Falls back to B09001 total-population buckets when gender data is absent.
    """
    m59  = demographics.get("male_5_9") or 0
    m1014 = demographics.get("male_10_14") or 0
    m1517 = demographics.get("male_15_17") or 0
    f59  = demographics.get("female_5_9") or 0
    f1014 = demographics.get("female_10_14") or 0
    f1517 = demographics.get("female_15_17") or 0

    has_gender_data = any([m59, m1014, m1517, f59, f1014, f1517])

    if has_gender_data:
        if grade_level == "k5":
            male   = m59  + round(2 / 5 * m1014)
            female = f59  + round(2 / 5 * f1014)
        elif grade_level == "k8":
            male   = m59  + m1014
            female = f59  + f1014
        elif grade_level == "high_school":
            male   = round(1 / 5 * m1014) + m1517
            female = round(1 / 5 * f1014) + f1517
        else:  # k12
            male   = m59  + m1014 + m1517
            female = f59  + f1014 + f1517

        if gender == "boys":
            return male
        if gender == "girls":
            return female
        return male + female

    # Fallback: use total school-age buckets with grade-level fraction
    pop_5_11  = demographics.get("population_5_to_11") or 0
    pop_12_17 = demographics.get("population_12_to_17") or 0
    if grade_level == "k5":
        total = pop_5_11
    elif grade_level == "k8":
        total = pop_5_11 + round(0.5 * pop_12_17)
    elif grade_level == "high_school":
        total = round(2 / 3 * pop_12_17)
    else:
        total = pop_5_11 + pop_12_17

    if gender in ("boys", "girls"):
        return round(total / 2)
    return total


def _s2_is_num(x: Any) -> bool:
    return isinstance(x, (int, float))


def _s2_safe_div(num: Any, den: Any) -> Optional[float]:
    if not _s2_is_num(num) or not _s2_is_num(den) or den == 0:
        return None
    return float(num) / float(den)


def _s2_score(value: Optional[float], segments: list) -> Optional[int]:
    """Continuous piecewise-linear Stage 2 KPI score (0–100), or None if no data."""
    if value is None:
        return None
    return round(piecewise_linear(value, segments))


def _score_stage2_component(stage2_inputs: Dict[str, Any], grade_level: str = "k12") -> Dict[str, Any]:
    required = ["school_audit_financials", "historical_financials"]

    audit_records = stage2_inputs.get("school_audit_financials") or []
    audit_records = [r for r in audit_records if isinstance(r, dict)]
    historical_records = stage2_inputs.get("historical_financials") or []
    historical_records = [r for r in historical_records if isinstance(r, dict)]

    confirmed = bool(stage2_inputs.get("school_stage2_confirmed"))
    use_audit_records = bool(audit_records) and confirmed

    def _norm_year(y: Any) -> Optional[int]:
        return int(y) if _s2_is_num(y) else None

    audit_by_year: Dict[int, Dict[str, Any]] = {}
    if use_audit_records:
        for r in audit_records:
            fy = _norm_year(r.get("fiscal_year"))
            if fy is None:
                continue
            audit_by_year[fy] = r

    historical_by_year: Dict[int, Dict[str, Any]] = {}
    for r in historical_records:
        fy = _norm_year(r.get("year"))
        if fy is None:
            continue
        historical_by_year[fy] = {
            "fiscal_year": fy,
            "tuition_revenue": r.get("tuition_revenue"),
            "total_expenses": r.get("total_expenses"),
            "enrollment": r.get("student_count"),
            "manual_total_revenue": r.get("total_revenue"),
        }

    merged_rows: List[Dict[str, Any]] = []
    for fy in sorted(set(audit_by_year.keys()) | set(historical_by_year.keys()))[-3:]:
        audit_row = audit_by_year.get(fy, {})
        hist_row = historical_by_year.get(fy, {})
        merged = {"fiscal_year": fy}
        for field in [
            "tuition_revenue",
            "tuition_aid",
            "other_revenue",
            "total_expenses",
            "non_operating_revenue",
            "total_assets",
            "enrollment",
        ]:
            merged[field] = audit_row.get(field) if _s2_is_num(audit_row.get(field)) else hist_row.get(field)
        merged["manual_total_revenue"] = (
            None if _s2_is_num(audit_row.get("tuition_revenue")) and _s2_is_num(audit_row.get("other_revenue")) else hist_row.get("manual_total_revenue")
        )
        merged_rows.append(merged)

    if not merged_rows:
        missing_inputs = ["school_audit_financials", "historical_financials"]
        if audit_records and not confirmed:
            missing_inputs = ["school_stage2_confirmed", "historical_financials"]
        return {
            "available": False,
            "score": None,
            "schema_version": "v3",
            "formula_version": "stage2-school-audit-v1",
            "computed_at_utc": datetime.now(timezone.utc).isoformat(),
            "readiness": "not_ready",
            "required_inputs": required,
            "provided_inputs": ["school_audit_financials"] if audit_records else [],
            "missing_inputs": missing_inputs,
            "components": [],
            "note": "No usable Stage 2 school financial years were provided.",
        }

    def _total_revenue(r: Dict[str, Any]) -> Optional[float]:
        if _s2_is_num(r.get("manual_total_revenue")):
            return float(r.get("manual_total_revenue"))
        tuition = r.get("tuition_revenue")
        other = r.get("other_revenue")
        non_op = r.get("non_operating_revenue")
        if not _s2_is_num(tuition) and not _s2_is_num(other):
            return None
        return float((tuition or 0) + (other or 0) + (non_op or 0))

    net_operating = []
    tuition_dependency = []
    effective_tuition = []
    revenue_per_student = []
    expense_per_student = []
    non_operating_share = []

    year_rows = sorted(merged_rows, key=lambda r: r.get("fiscal_year"))

    for r in year_rows:
        tuition = r.get("tuition_revenue")
        other = r.get("other_revenue")
        expenses = r.get("total_expenses")
        non_op = r.get("non_operating_revenue")
        aid = r.get("tuition_aid")
        enrollment = r.get("enrollment")
        total_rev = _total_revenue(r)

        if _s2_is_num(tuition) and _s2_is_num(other) and _s2_is_num(expenses):
            net_operating.append(float(tuition + other - expenses))
        if _s2_is_num(tuition) and _s2_is_num(total_rev) and total_rev != 0:
            tuition_dependency.append(float(tuition) / float(total_rev))
        if _s2_is_num(tuition) and _s2_is_num(aid) and _s2_is_num(enrollment) and enrollment != 0:
            effective_tuition.append(float(tuition - aid) / float(enrollment))
        if _s2_is_num(total_rev) and _s2_is_num(enrollment) and enrollment != 0:
            revenue_per_student.append(float(total_rev) / float(enrollment))
        if _s2_is_num(expenses) and _s2_is_num(enrollment) and enrollment != 0:
            expense_per_student.append(float(expenses) / float(enrollment))
        if _s2_is_num(non_op) and _s2_is_num(total_rev) and total_rev != 0:
            non_operating_share.append(float(non_op) / float(total_rev))

    def _avg(vals: List[float]) -> Optional[float]:
        return sum(vals) / len(vals) if vals else None

    def _trend(rows: List[Dict[str, Any]], key: str) -> Optional[float]:
        vals = [(r.get("fiscal_year"), r.get(key)) for r in rows if _s2_is_num(r.get(key))]
        if len(vals) < 2:
            return None
        first = float(vals[0][1])
        last = float(vals[-1][1])
        if first == 0:
            return None
        return (last - first) / abs(first)

    enrollment_trend = _trend(year_rows, "enrollment")
    revenue_trend = _trend([{**r, "total_revenue_calc": _total_revenue(r)} for r in year_rows], "total_revenue_calc")
    expense_trend = _trend(year_rows, "total_expenses")

    components = [
        {"key": "net_operating_position", "label": "Net Operating Position", "weight": 20, "score": _s2_score(_avg(net_operating), [(-500000, 10), (0, 60), (250000, 90)])},
        {"key": "tuition_dependency_ratio", "label": "Tuition Dependency Ratio", "weight": 15, "score": _s2_score(_avg(tuition_dependency), [(0.2, 20), (0.5, 55), (0.75, 85), (0.95, 70)])},
        {"key": "effective_tuition_rate", "label": "Effective Tuition Rate", "weight": 12, "score": _s2_score(_avg(effective_tuition), [(1000, 20), (6000, 60), (12000, 90)])},
        {"key": "revenue_per_student", "label": "Revenue per Student", "weight": 12, "score": _s2_score(_avg(revenue_per_student), [(4000, 20), (10000, 65), (18000, 90)])},
        {"key": "expense_per_student", "label": "Expense per Student", "weight": 12, "score": _s2_score(_avg(expense_per_student), [(4000, 90), (11000, 70), (18000, 40), (25000, 20)])},
        {"key": "enrollment_trend", "label": "Enrollment Trend", "weight": 9, "score": _s2_score(enrollment_trend, [(-0.2, 20), (-0.05, 45), (0.0, 60), (0.08, 80), (0.2, 95)])},
        {"key": "revenue_trend", "label": "Revenue Trend", "weight": 10, "score": _s2_score(revenue_trend, [(-0.2, 20), (-0.05, 45), (0.0, 60), (0.08, 80), (0.2, 95)])},
        {"key": "expense_trend", "label": "Expense Trend", "weight": 10, "score": _s2_score(expense_trend, [(-0.2, 95), (-0.05, 80), (0.05, 60), (0.15, 35), (0.25, 20)])},
    ]

    if _avg(non_operating_share) is not None:
        components.append({
            "key": "non_operating_revenue_share",
            "label": "Non-Operating Revenue Share",
            "weight": 10,
            "score": _s2_score(_avg(non_operating_share), [(0.0, 95), (0.05, 80), (0.15, 50), (0.3, 20)]),
        })

    provided = []
    if use_audit_records:
        provided.extend(["school_audit_financials", "school_stage2_confirmed"])
    elif audit_records:
        provided.append("school_audit_financials")
    if historical_by_year:
        provided.append("historical_financials")
    observed_fields = {k for r in year_rows for k, v in r.items() if v is not None}
    component_dependencies = {
        "net_operating_position": ["tuition_revenue", "other_revenue", "total_expenses"],
        "tuition_dependency_ratio": ["tuition_revenue", "other_revenue"],
        "effective_tuition_rate": ["tuition_revenue", "tuition_aid", "enrollment"],
        "revenue_per_student": ["manual_total_revenue", "enrollment"],
        "expense_per_student": ["total_expenses", "enrollment"],
        "enrollment_trend": ["enrollment"],
        "revenue_trend": ["manual_total_revenue"],
        "expense_trend": ["total_expenses"],
        "non_operating_revenue_share": ["non_operating_revenue"],
    }
    missing = [f for f in required if f not in provided]
    for component in components:
        if component["score"] is None:
            for dep in component_dependencies.get(component["key"], []):
                if dep not in observed_fields:
                    missing.append(dep)
    if audit_records and not confirmed:
        missing.append("school_stage2_confirmed")

    scored = [c for c in components if c["score"] is not None]
    readiness = "not_ready"
    available = False
    stage2_score = None
    if len(scored) == len(components) and components:
        stage2_score = round(sum(c["score"] * c["weight"] for c in components) / sum(c["weight"] for c in components))
        readiness = "ready"
        available = True
    elif scored:
        stage2_score = round(sum(c["score"] * c["weight"] for c in scored) / sum(c["weight"] for c in scored))
        readiness = "partial"
        available = True

    years = [r.get("fiscal_year") for r in year_rows if r.get("fiscal_year") is not None]
    trend_parts = []
    for label, value in [("enrollment", enrollment_trend), ("revenue", revenue_trend), ("expenses", expense_trend)]:
        if value is None:
            continue
        direction = "up" if value > 0 else "down" if value < 0 else "flat"
        trend_parts.append(f"{label} {direction} {abs(value) * 100:.1f}%")

    return {
        "available": available,
        "score": stage2_score,
        "schema_version": "v3",
        "formula_version": "stage2-school-audit-v1",
        "computed_at_utc": datetime.now(timezone.utc).isoformat(),
        "readiness": readiness,
        "required_inputs": required,
        "provided_inputs": sorted(set(provided)),
        "missing_inputs": sorted(set(missing)),
        "components": components,
        "note": (
            "School Stage 2 uses confirmed audit PDF extraction with manual enrollment alignment. "
            "When both audit and historical rows exist for a year, confirmed audit values take precedence and historical data fills gaps. "
            "Operating metrics exclude non-operating revenue from net operating position. "
            f"Years: {', '.join(str(y) for y in years) if years else 'none'}. "
            + ("Unconfirmed audit rows were ignored; historical/manual rows were used when available. " if audit_records and not confirmed else "")
            + ("Trends: " + "; ".join(trend_parts) + "." if trend_parts else "Trend data unavailable.")
        ),
    }


def _build_recommendation(
    overall: int,
    ms: float,
    inc: float,
    comp: float,
    fam: float,
    est_catholic_school_age: int,
    n_catholic_schools: int,
    catchment_desc: str,
    total_addressable_market: int = 0,
    market_depth_ratio: float = 0.0,
    reference_enrollment: int = 400,
    families_with_children: int = 0,
    effective_school_age_pop: int = 0,
) -> Tuple[str, str]:
    """
    Derive a human-readable recommendation title and detail sentence from scores.

    Returns (recommendation_title, recommendation_detail).
    """
    families_clause = ""
    if families_with_children > 0 and effective_school_age_pop > 0:
        families_clause = (
            f" Among {families_with_children:,} households with school-aged children "
            f"({effective_school_age_pop:,} children in the target age range),"
        )
    market_label = (
        f"An estimated addressable market of {total_addressable_market:,} families "
        f"({market_depth_ratio}x the reference enrollment of {reference_enrollment})"
        if total_addressable_market > 0
        else f"An estimated {est_catholic_school_age:,} Catholic school-age children"
    )

    if overall >= 75:
        unserved = n_catholic_schools == 0
        presence = (
            "There are currently no Catholic schools in your catchment — this represents an unserved market."
            if unserved
            else f"Existing Catholic school presence ({n_catholic_schools} school(s)) demonstrates proven community demand."
        )
        return (
            "Strong Sustainability Conditions",
            (
                f"The {catchment_desc} around this address shows strong indicators for sustaining Catholic K-12 education."
                f"{families_clause} "
                f"{market_label} reside within this area, "
                f"and income levels suggest solid ability to support tuition. {presence}"
            ),
        )

    if overall >= 55:
        strengths = []
        if ms >= 60:
            strengths.append("sizable addressable market")
        if inc >= 60:
            strengths.append("favorable household income levels")
        if comp >= 60:
            strengths.append("limited Catholic school competition")
        if fam >= 60:
            strengths.append("high density of families with children")
        strength_clause = (", with strengths in " + " and ".join(strengths)) if strengths else ""
        return (
            "Moderate Sustainability Conditions",
            (
                f"This area has moderate sustainability potential{strength_clause}. "
                "Differentiated programming, strong community ties, or a unique academic identity "
                "can strengthen enrollment stability and long-term viability."
            ),
        )

    if overall >= 35:
        competition_clause = (
            f"High competition from {n_catholic_schools} existing Catholic school(s)"
            if n_catholic_schools > 0
            else "A limited addressable market"
        )
        return (
            "Challenging Market Conditions",
            (
                f"Market conditions present real challenges. {competition_clause} "
                "may constrain enrollment potential. A focused niche strategy, regional draw, or strong "
                "alumni network and strong community relationships will be essential."
            ),
        )

    return (
        "Difficult Market Conditions",
        (
            "Multiple unfavorable indicators — including limited addressable market, income constraints, "
            "or existing school saturation — suggest elevated risk to long-term operational viability in this area. "
            "Deep local research and community engagement are strongly recommended before major strategic commitments."
        ),
    )


def _compute_stage1_scores(
    location: dict,
    demographics: dict,
    schools: List[dict],
    gender: str,
    grade_level: str,
    weighting_profile: str,
    market_context: str,
) -> dict:
    """
    Compute all Stage 1 feasibility scores and supporting metadata.
    Returns a flat dict consumed by calculate_feasibility and _build_data_notes.
    """
    state_fips = location.get("state_fips", "")
    state_abbr = STATE_FIPS_TO_ABBR.get(state_fips[:2] if len(state_fips) >= 2 else "", "")
    state_catholic_pct = CATHOLIC_PCT_BY_STATE.get(state_abbr, NATIONAL_CATHOLIC_PCT)

    catholic_pct, local_private_rate = _adjusted_catholic_pct(
        state_catholic_pct,
        demographics.get("private_school_enrolled") or 0,
        demographics.get("total_school_enrolled") or 0,
        state_abbr=state_abbr,
    )

    # Effective population scoped to grade level and gender.
    gravity_school_age = demographics.get("gravity_weighted_school_age_pop")
    effective_pop = _effective_school_age_pop(demographics, gender, grade_level)
    if gravity_school_age and gravity_school_age > 0 and gender == "coed" and grade_level == "k12":
        effective_pop = int(gravity_school_age)
    est_catholic_school_age = int(effective_pop * catholic_pct)

    # Weighted addressable population for competition scoring: Catholic families
    # count fully; non-Catholic families count at the national non-Catholic draw
    # rate (~12% per NCES PSS). This ensures all school-age families inform the
    # market depth while Catholic families carry proportionally higher weight.
    non_catholic_school_age = max(0, effective_pop - est_catholic_school_age)
    addressable_school_age = est_catholic_school_age + int(non_catholic_school_age * _NON_CATHOLIC_DRAW_RATE)

    # Income-first addressable market estimation — school choice programs
    # are modeled as income curve shifts here (not in the income score).
    addressable = _estimate_addressable_market(
        effective_pop=effective_pop,
        catholic_pct=catholic_pct,
        income_distribution=demographics.get("income_distribution") or [],
        state_abbr=state_abbr,
    )
    total_addressable_market = addressable["total_addressable_market"]

    total_households = demographics.get("total_households") or 1
    high_income_households = demographics.get("high_income_households") or 0
    high_income_pct = high_income_households / total_households if total_households > 0 else 0

    catholic_schools = [s for s in schools if s["is_catholic"]]

    historical_2017 = demographics.get("historical_2017") or {}
    trend_dict = compute_trend(demographics, historical_2017)
    trend_label = trend_dict.get("trend_label", "Unknown")
    trend_adjustment = _trend_score_adjustment(trend_label)

    # Reference enrollment for market depth ratio — NCEA modal enrollment
    ref_pair = _REFERENCE_ENROLLMENT.get(grade_level, _REFERENCE_ENROLLMENT["k12"])
    reference_enrollment = ref_pair[1] if gender in ("boys", "girls") else ref_pair[0]
    market_depth_ratio = total_addressable_market / reference_enrollment if reference_enrollment > 0 else 0.0

    secularization_adjustment = _CARA_DECLINE_MARKET_SIZE_PENALTY if state_abbr in _CARA_HIGH_DECLINE_STATES else 0
    if (
        state_abbr in _CARA_HIGH_DECLINE_STATES
        and (trend_dict.get("school_age_pop_pct") or 0) < 0
        and trend_label in {"Stable", "Mixed", "Unknown"}
    ):
        secularization_adjustment += _CARA_DECLINE_NEGATIVE_TREND_EXTRA_PENALTY

    ms = min(100.0, max(0.0, _score_market_size(market_depth_ratio, market_context) + trend_adjustment + secularization_adjustment))

    # Income score — pure affordability measure. School choice programs are
    # already captured in the market size estimation (income curve shift),
    # so they are NOT added here to avoid double-counting.
    income_for_scoring = demographics.get("median_family_income") or demographics.get("median_household_income")
    income_type = "family (with children)" if demographics.get("median_family_income") else "household"
    inc = min(100.0, _score_income(income_for_scoring, high_income_pct))

    comp, comp_validation, comp_saturation = _score_competition(
        schools,
        addressable_school_age,
        school_lat=location["lat"],
        school_lon=location["lon"],
        population_by_direction=demographics.get("population_by_direction"),
        local_private_rate=local_private_rate,
    )
    fam = _score_family_density(
        demographics.get("families_with_children"),
        demographics.get("total_households"),
    )

    # Private school enrollment rate sub-indicator (Census B14002)
    priv_enroll_score, priv_enroll_rate_pct = _score_private_enrollment_rate(
        demographics.get("private_school_enrolled") or 0,
        demographics.get("total_school_enrolled") or 0,
    )

    # Under-5 kindergarten pipeline sub-indicator
    pop_under_5 = demographics.get("population_under_5") or 0
    pop_5_17 = demographics.get("school_age_population") or demographics.get("population_5_to_11", 0) + demographics.get("population_12_to_17", 0)
    pipeline_score, pipeline_ratio = _score_pipeline_ratio(pop_under_5, pop_5_17)

    profile_weights = _WEIGHTING_PROFILES.get(weighting_profile, _WEIGHTING_PROFILES["standard_baseline"])
    overall = round(
        ms * profile_weights["market_size"]
        + inc * profile_weights["income"]
        + comp * profile_weights["competition"]
        + fam * profile_weights["family_density"]
    )

    confidence = _data_confidence(demographics)
    scenario_conservative, scenario_optimistic = _scenario_scores(overall, confidence)

    return {
        "state_abbr": state_abbr,
        "state_catholic_pct": state_catholic_pct,
        "catholic_pct": catholic_pct,
        "local_private_rate": local_private_rate,
        "effective_pop": effective_pop,
        "est_catholic_school_age": est_catholic_school_age,
        "addressable": addressable,
        "total_addressable_market": total_addressable_market,
        "reference_enrollment": reference_enrollment,
        "market_depth_ratio": round(market_depth_ratio, 2),
        "high_income_pct": high_income_pct,
        "income_for_scoring": income_for_scoring,
        "income_type": income_type,
        "catholic_schools": catholic_schools,
        "trend_dict": trend_dict,
        "trend_adjustment": trend_adjustment,
        "ms": ms,
        "inc": inc,
        "comp": comp,
        "comp_validation": comp_validation,
        "comp_saturation": comp_saturation,
        "fam": fam,
        "profile_weights": profile_weights,
        "overall": overall,
        "confidence": confidence,
        "scenario_conservative": scenario_conservative,
        "scenario_optimistic": scenario_optimistic,
        "priv_enroll_score": priv_enroll_score,
        "priv_enroll_rate_pct": priv_enroll_rate_pct,
        "pipeline_score": pipeline_score,
        "pipeline_ratio": pipeline_ratio,
        "population_under_5": pop_under_5,
    }


def _build_data_notes(
    location: dict,
    demographics: dict,
    schools: List[dict],
    catchment_desc: str,
    stage2_payload: dict,
    s: dict,
) -> Tuple[List[str], Optional["DemographicTrend"], Optional["PopulationGravityMap"], Optional[float], Optional[float], str]:
    """
    Assemble all data notes, derive DemographicTrend and PopulationGravityMap objects,
    and compute owner_occupied_pct / families_pct / comp_desc for use in the response.

    Returns (data_notes, trend, gravity_map, owner_occupied_pct, families_pct, comp_desc).
    """
    state_abbr = s["state_abbr"]
    state_catholic_pct = s["state_catholic_pct"]
    catholic_pct = s["catholic_pct"]
    local_private_rate = s["local_private_rate"]
    confidence = s["confidence"]
    scenario_conservative = s["scenario_conservative"]
    scenario_optimistic = s["scenario_optimistic"]
    catholic_schools = s["catholic_schools"]
    trend_dict = s["trend_dict"]
    comp_validation = s["comp_validation"]
    comp_saturation = s["comp_saturation"]

    data_geography = demographics.get("data_geography", "county")
    tract_count = demographics.get("tract_count")
    area_label = demographics.get("county_name", location.get("county_name", "Unknown"))

    if data_geography == "radius" and tract_count:
        geography_note = (
            f"Demographics are ACS 5-year estimates (2022) aggregated from "
            f"{tract_count} census tract(s) within the {catchment_desc} ({area_label})."
        )
    else:
        geography_note = (
            f"Demographics are county-level ACS 5-year estimates (2022). County: {area_label}."
        )

    catholic_note = (
        f"Catholic population baseline: {round(state_catholic_pct * 100, 1)}% "
        f"(CARA state-level estimate for {state_abbr or 'this state'})"
    )
    if local_private_rate is not None:
        catholic_note += (
            f", adjusted to {round(catholic_pct * 100, 1)}% based on local private school "
            f"enrollment rate ({round(local_private_rate * 100, 1)}% vs. "
            f"{round(NATIONAL_PRIVATE_SCHOOL_RATE * 100, 1)}% national average, from ACS C14002)."
        )
    else:
        catholic_note += "."

    choice_tier = s.get("addressable", {}).get("choice_tier", "none")
    choice_note = ""
    if choice_tier != "none":
        choice_note = (
            f" School choice programs in {state_abbr} adjust the effective income "
            f"threshold downward ({choice_tier} program tier)."
        )
    addressable_note = (
        "Market size uses an income-first propensity model. Base enrollment probability "
        "is estimated from household income distribution (NCES private school enrollment "
        "rates by income bracket, CPS October School Enrollment data). Catholic affiliation "
        f"provides an additional propensity boost using CARA diocese data.{choice_note}"
    )

    data_notes: List[str] = [geography_note, catholic_note, addressable_note]
    if data_geography == "radius":
        data_notes.append(
            "Radius-based catchments in dense urban areas can pull in suburban demographics and schools; "
            "isochrone travel-time analysis is recommended for finer urban-market precision."
        )

    if demographics.get("gravity_weighted") is True:
        data_notes.append(
            "Market size score uses distance-decay population weighting (nearer tracts weighted "
            "more heavily than distant ones). Reported school-age population reflects gravity-adjusted estimate."
        )

    if not schools:
        data_notes.append(
            "Competitor school data is unavailable (NCES PSS data could not be loaded). "
            "Competition score reflects this uncertainty."
        )
    else:
        data_notes.append(
            f"Competitor school data from NCES Private School Survey 2021–22 "
            f"({len(schools)} private school(s) found within the {catchment_desc}). "
            f"Competition score uses distance-decay weighting (nearer schools weighted more heavily)."
        )

    band = _CONFIDENCE_BAND.get(confidence, 12)
    income_cv = demographics.get("income_moe_pct")
    if income_cv is not None:
        data_notes.append(
            f"Data confidence: {confidence} (income estimate CV {round(income_cv * 100, 1)}%). "
            f"Scenario range: {scenario_conservative}–{scenario_optimistic} (±{band} pts)."
        )
    else:
        data_notes.append(
            f"Data confidence: {confidence}. "
            f"Scenario range: {scenario_conservative}–{scenario_optimistic} (±{band} pts)."
        )

    owner_occupied_pct = None
    if demographics.get("owner_occupied_units") is not None and demographics.get("total_housing_units"):
        total_units = demographics["total_housing_units"]
        if total_units > 0:
            owner_occupied_pct = round(demographics["owner_occupied_units"] / total_units * 100, 1)

    families_with_children = demographics.get("families_with_children")
    families_pct = None
    if families_with_children is not None and demographics.get("total_households"):
        hh = demographics["total_households"]
        if hh > 0:
            families_pct = round(families_with_children / hh * 100, 1)

    # Summarize competitor landscape by tier
    n_direct = sum(1 for s in schools if s.get("competitor_tier") == "direct")
    n_strong = sum(1 for s in schools if s.get("competitor_tier") == "strong")
    n_moderate = sum(1 for s in schools if s.get("competitor_tier") == "moderate")
    n_weak = sum(1 for s in schools if s.get("competitor_tier") == "weak")

    tier_parts = []
    if n_direct:
        tier_parts.append(f"{n_direct} direct Catholic competitor{'s' if n_direct != 1 else ''}")
    if n_strong:
        tier_parts.append(f"{n_strong} other religious school{'s' if n_strong != 1 else ''}")
    if n_moderate:
        tier_parts.append(f"{n_moderate} secular private school{'s' if n_moderate != 1 else ''}")
    if n_weak:
        tier_parts.append(f"{n_weak} special emphasis school{'s' if n_weak != 1 else ''}")

    comp_desc = (
        (", ".join(tier_parts) if tier_parts else "No private schools")
        + f" within {catchment_desc}"
    )
    if catholic_schools:
        comp_desc += (
            f" · Demand validation {round(comp_validation)}/100, "
            f"saturation pressure {round(comp_saturation)}/100"
        )

    trend = None
    if trend_dict:
        trend = DemographicTrend(
            school_age_pop_pct=trend_dict.get("school_age_pop_pct"),
            income_real_pct=trend_dict.get("income_real_pct"),
            families_pct=trend_dict.get("families_pct"),
            trend_label=trend_dict.get("trend_label", "Unknown"),
            period=trend_dict.get("period", "ACS 2017 → 2022 (county-level)"),
        )
        if trend.school_age_pop_pct is not None:
            direction = "↑" if trend.school_age_pop_pct > 0 else "↓"
            data_notes.append(
                f"Demographic trend ({trend.period}): school-age pop {direction}{abs(trend.school_age_pop_pct):.1f}%, "
                f"real income {'↑' if (trend.income_real_pct or 0) > 0 else '↓'}{abs(trend.income_real_pct or 0):.1f}%, "
                f"families with children {'↑' if (trend.families_pct or 0) > 0 else '↓'}{abs(trend.families_pct or 0):.1f}%. "
                f"Overall: {trend.trend_label}."
            )

    if state_abbr in _CARA_HIGH_DECLINE_STATES:
        data_notes.append(
            f"CARA data note: {state_abbr} is in a region (New England / Pacific Northwest) with "
            "documented post-2015 Catholic population decline. The state-level Catholic% used here "
            "is from CARA ~2020 estimates and likely overstates today's addressable market. "
            "Consider a ±15–20% sensitivity range around the estimated Catholic school-age count."
        )
        data_notes.append(
            "Market-size scoring includes a conservative secularization adjustment for high-decline "
            "CARA states to avoid overstating demand in historically Catholic but fast-secularizing regions."
        )

    region = _STATE_TO_REGION.get(state_abbr)
    if region:
        regional_waitlist = _REGIONAL_WAITLIST_PREVALENCE.get(region)
        if regional_waitlist is not None and abs(regional_waitlist - _NATIONAL_WAITLIST_BASELINE) > _REGION_WAITLIST_NOTE_DELTA_PCT:
            data_notes.append(
                "Note: The competition model uses a national waitlist baseline (39.3%). "
                f"In the {region.title()} region, waitlist prevalence is {regional_waitlist:.1f}%, "
                "which may affect the relative weight of market validation vs saturation pressure."
            )

    by_direction = demographics.get("population_by_direction")
    direction_details = demographics.get("direction_details")
    gravity_map = None
    if by_direction:
        # Build rich DirectionSegment map from direction_details if available,
        # otherwise fall back to raw school-age pop only
        if direction_details:
            segment_map = {
                d: DirectionSegment(
                    school_age_pop=info.get("school_age_pop", 0),
                    income_qualified_pop=info.get("income_qualified_pop", 0),
                    catholic_qualified_pop=int(info.get("income_qualified_pop", 0) * catholic_pct),
                    pipeline_ratio=info.get("pipeline_ratio"),
                    growth_signal=info.get("growth_signal"),
                )
                for d, info in direction_details.items()
            }
        else:
            segment_map = {
                d: DirectionSegment(school_age_pop=v)
                for d, v in by_direction.items()
            }
        dominant = max(segment_map, key=lambda d: segment_map[d].school_age_pop)
        gravity_map = PopulationGravityMap(
            by_direction=segment_map,
            dominant_direction=dominant,
            gravity_weighted=demographics.get("gravity_weighted", False),
        )

        # Directional data note summarising strongest market and any declining corridors
        if direction_details:
            best_dir = max(direction_details, key=lambda d: direction_details[d].get("income_qualified_pop", 0))
            best = direction_details[best_dir]
            note = (
                f"Strongest market concentration is to the {best_dir} "
                f"({best['income_qualified_pop']} income-qualified school-age"
                f"{', ' + best['growth_signal'] if best.get('growth_signal') else ''})."
            )
            declining = [
                d for d, info in direction_details.items()
                if info.get("growth_signal") == "Declining" and info.get("school_age_pop", 0) > 0
            ]
            if declining:
                note += f" {', '.join(declining)} corridor{'s' if len(declining) > 1 else ''} show{'s' if len(declining) == 1 else ''} pipeline decline."
            data_notes.append(note)

    # Private school enrollment rate note
    priv_rate = s.get("priv_enroll_rate_pct")
    if priv_rate is not None:
        data_notes.append(
            f"Private school enrollment rate: {priv_rate:.1f}% of K-12 students in the catchment "
            f"attend private school (national avg: {NATIONAL_PRIVATE_SCHOOL_RATE * 100:.1f}%). "
            f"Sub-indicator score: {round(s['priv_enroll_score'])}/100."
        )

    # Kindergarten pipeline note
    pipeline_ratio = s.get("pipeline_ratio")
    if pipeline_ratio is not None:
        data_notes.append(
            f"Kindergarten pipeline: under-5 population is {pipeline_ratio:.2f}x the current school-age (5-17) population. "
            f"{'Strong incoming cohorts support future enrollment.' if pipeline_ratio >= 0.33 else 'Below-average pipeline may signal enrollment decline risk.'} "
            f"Sub-indicator score: {round(s['pipeline_score'])}/100."
        )

    data_notes.append(
        f"Stage 2 audit: schema {stage2_payload.get('schema_version')} · formula {stage2_payload.get('formula_version')} · "
        f"readiness {stage2_payload.get('readiness')} · provided {len(stage2_payload.get('provided_inputs', []))}/"
        f"{len(stage2_payload.get('required_inputs', []))} required inputs."
    )

    return data_notes, trend, gravity_map, owner_occupied_pct, families_pct, comp_desc


def _build_enrollment_forecast(
    school_age_population: int,
    pipeline_ratio: Optional[float],
    trend_dict: dict,
    minimum_viable_enrollment: int = 100,
) -> EnrollmentForecast:
    """Project enrollment/census trend to support 3/5/10-year decline planning."""
    base_pop = max(0, school_age_population or 0)
    school_age_pct = trend_dict.get("school_age_pop_pct")
    annual_demographic_change = (school_age_pct / 100.0) / 5.0 if school_age_pct is not None else 0.0

    # pipeline_ratio > 0.33 implies stronger incoming cohorts; below implies decline pressure
    pipeline_adjustment = 0.0
    if pipeline_ratio is not None:
        pipeline_adjustment = max(-0.02, min(0.02, (pipeline_ratio - 0.33) * 0.08))

    baseline_annual = annual_demographic_change + pipeline_adjustment
    optimistic_annual = baseline_annual + 0.01
    conservative_annual = baseline_annual - 0.01

    def _scenario(rate: float) -> list[ForecastPoint]:
        points = []
        for years_out in [0, 3, 5, 10]:
            projected = round(base_pop * ((1 + rate) ** years_out))
            points.append(ForecastPoint(years_out=years_out, projected_enrollment=max(0, projected)))
        return points

    baseline = _scenario(baseline_annual)
    optimistic = _scenario(optimistic_annual)
    conservative = _scenario(conservative_annual)

    cliff_year = None
    for p in baseline:
        if p.years_out > 0 and p.projected_enrollment < minimum_viable_enrollment:
            cliff_year = datetime.now(timezone.utc).year + p.years_out
            break

    if cliff_year is not None:
        risk = "high"
    elif baseline[-1].projected_enrollment < max(minimum_viable_enrollment * 1.2, 120):
        risk = "moderate"
    else:
        risk = "low"

    return EnrollmentForecast(
        baseline=baseline,
        optimistic=optimistic,
        conservative=conservative,
        minimum_viable_enrollment=minimum_viable_enrollment,
        estimated_cliff_year=cliff_year,
        decline_risk=risk,
    )


async def calculate_feasibility(
    location: dict,
    demographics: dict,
    schools: List[dict],
    school_name: str,
    radius_miles: float,
    drive_minutes: int = 20,
    isochrone_polygon: Optional[dict] = None,
    catchment_type: str = "radius",
    gender: str = "coed",
    grade_level: str = "k12",
    weighting_profile: str = "standard_baseline",
    stage2_inputs: Optional[Dict[str, Any]] = None,
    market_context: str = "suburban",
) -> AnalysisResponse:
    subject_name_norm = _normalize_school_name(school_name)
    if subject_name_norm:
        schools_for_analysis = [
            sch
            for sch in schools
            if _normalize_school_name(sch.get("name", "")) != subject_name_norm
        ]
    else:
        schools_for_analysis = schools

    s = _compute_stage1_scores(
        location,
        demographics,
        schools_for_analysis,
        gender,
        grade_level,
        weighting_profile,
        market_context,
    )

    catchment_desc = (
        f"{drive_minutes}-minute drive catchment" if catchment_type == "isochrone"
        else f"{radius_miles:.0f}-mile radius"
    )

    recommendation, recommendation_detail = _build_recommendation(
        overall=s["overall"],
        ms=s["ms"],
        inc=s["inc"],
        comp=s["comp"],
        fam=s["fam"],
        est_catholic_school_age=s["est_catholic_school_age"],
        n_catholic_schools=len(s["catholic_schools"]),
        catchment_desc=catchment_desc,
        total_addressable_market=s["total_addressable_market"],
        market_depth_ratio=s["market_depth_ratio"],
        reference_enrollment=s["reference_enrollment"],
        families_with_children=demographics.get("families_with_children") or 0,
        effective_school_age_pop=s["effective_pop"],
    )

    stage2_payload = _score_stage2_component(stage2_inputs or {}, grade_level=grade_level)

    data_notes, trend, gravity_map, owner_occupied_pct, families_pct, comp_desc = _build_data_notes(
        location, demographics, schools_for_analysis, catchment_desc, stage2_payload, s
    )

    effective_pop = s["effective_pop"]
    est_catholic_school_age = s["est_catholic_school_age"]
    addressable = s["addressable"]
    total_addressable_market = s["total_addressable_market"]
    reference_enrollment = s["reference_enrollment"]
    market_depth_ratio = s["market_depth_ratio"]
    catholic_pct = s["catholic_pct"]
    catholic_schools = s["catholic_schools"]
    ms, inc, comp, fam = s["ms"], s["inc"], s["comp"], s["fam"]
    overall = s["overall"]
    scenario_conservative, scenario_optimistic = s["scenario_conservative"], s["scenario_optimistic"]
    profile_weights = s["profile_weights"]
    trend_dict = s["trend_dict"]
    trend_adjustment = s["trend_adjustment"]
    income_for_scoring = s["income_for_scoring"]
    income_type = s["income_type"]
    high_income_pct = s["high_income_pct"]
    confidence = s["confidence"]
    state_abbr = s["state_abbr"]
    data_geography = demographics.get("data_geography", "county")

    # Benchmark percentile rankings (Section 4.3 of blueprint)
    from api.benchmarks import compute_benchmarks
    try:
        benchmarks_data = await compute_benchmarks(
            overall_score=overall,
            state_fips=location.get("state_fips", ""),
            ministry_type="schools",
            lat=location["lat"],
            lon=location["lon"],
            demographics=demographics,
            market_size_score=round(ms),
            income_score=round(inc),
            competition_score=round(comp),
            family_density_score=round(fam),
        )
        benchmarks = BenchmarkPercentiles(**benchmarks_data)
        state_name_for_benchmarks = location.get("state_name", "")
        if state_name_for_benchmarks:
            benchmarks.state_name = state_name_for_benchmarks
    except Exception:
        benchmarks = None

    if benchmarks and benchmarks.percentile_national is not None:
        pct_note = (
            f"Benchmark: this location scores in the "
            f"{benchmarks.percentile_national:.0f}th percentile nationally"
        )
        if benchmarks.percentile_state is not None and benchmarks.state_name:
            pct_note += f" and the {benchmarks.percentile_state:.0f}th percentile in {benchmarks.state_name}"
        pct_note += "."
        data_notes.append(pct_note)

    # Hierarchical composite scoring
    from api.hierarchical_scoring import compute_hierarchical_score
    try:
        hierarchical = compute_hierarchical_score(
            market_depth_ratio=market_depth_ratio,
            trend_label=trend_dict.get("trend_label", "Unknown"),
            trend_adjustment=trend_adjustment,
            median_income=income_for_scoring,
            high_income_pct=high_income_pct,
            choice_tier=addressable.get("choice_tier", "none"),
            comp_validation=s["comp_validation"],
            comp_saturation=s["comp_saturation"],
            families_pct=families_pct,
            owner_occupied_pct=owner_occupied_pct,
            catholic_pct=catholic_pct,
            market_context=market_context,
            ms_score=ms,
            inc_score=inc,
            comp_score=comp,
            fam_score=fam,
            private_enrollment_score=s.get("priv_enroll_score"),
            private_enrollment_rate_pct=s.get("priv_enroll_rate_pct"),
            pipeline_score=s.get("pipeline_score"),
            pipeline_ratio=s.get("pipeline_ratio"),
        )
    except Exception:
        hierarchical = None

    enrollment_forecast = _build_enrollment_forecast(
        school_age_population=effective_pop,
        pipeline_ratio=s.get("pipeline_ratio"),
        trend_dict=trend_dict,
    )

    if enrollment_forecast.estimated_cliff_year is not None:
        data_notes.append(
            f"Enrollment forecast (baseline): projected to cross the minimum viable enrollment threshold "
            f"({enrollment_forecast.minimum_viable_enrollment}) by {enrollment_forecast.estimated_cliff_year}."
        )

    return AnalysisResponse(
        school_name=school_name,
        analysis_address=location.get("matched_address", ""),
        county_name=demographics.get("county_name", location.get("county_name", "")),
        state_name=location.get("state_name", ""),
        lat=location["lat"],
        lon=location["lon"],
        radius_miles=radius_miles,
        catchment_minutes=drive_minutes,
        isochrone_polygon=isochrone_polygon,
        catchment_type=catchment_type,
        gender=gender,
        grade_level=grade_level,
        demographics=DemographicData(
            total_population=demographics.get("total_population"),
            population_under_18=demographics.get("population_under_18"),
            school_age_population=effective_pop if effective_pop > 0 else None,
            estimated_catholic_school_age=est_catholic_school_age,
            median_household_income=demographics.get("median_household_income"),
            total_households=demographics.get("total_households"),
            families_with_children=demographics.get("families_with_children"),
            owner_occupied_pct=owner_occupied_pct,
            estimated_catholic_pct=round(catholic_pct * 100, 1),
            data_geography=data_geography,
            data_confidence=confidence,
            total_addressable_market=total_addressable_market,
            reference_enrollment=reference_enrollment,
            market_depth_ratio=market_depth_ratio,
            income_qualified_base=addressable["income_qualified_base"],
            catholic_boost_contribution=addressable["catholic_boost_contribution"],
            population_under_5=s.get("population_under_5"),
            pipeline_ratio=round(s["pipeline_ratio"], 3) if s.get("pipeline_ratio") is not None else None,
            pipeline_score=round(s["pipeline_score"]) if s.get("pipeline_score") is not None else None,
            private_enrollment_rate_pct=round(s["priv_enroll_rate_pct"], 1) if s.get("priv_enroll_rate_pct") is not None else None,
            private_enrollment_score=round(s["priv_enroll_score"]) if s.get("priv_enroll_score") is not None else None,
        ),
        competitor_schools=[
            CompetitorSchool(
                name=sch["name"],
                lat=sch["lat"],
                lon=sch["lon"],
                distance_miles=sch["distance_miles"],
                affiliation=sch["affiliation"],
                is_catholic=sch["is_catholic"],
                city=sch.get("city"),
                enrollment=sch.get("enrollment"),
                gender=sch.get("gender", "Unknown"),
                grade_level=sch.get("grade_level", "Unknown"),
                competitor_tier=sch.get("competitor_tier", "moderate"),
                tier_weight=sch.get("tier_weight", 0.4),
            )
            for sch in schools_for_analysis[:25]
        ],
        catholic_school_count=len(catholic_schools),
        total_private_school_count=len(schools_for_analysis),
        feasibility_score=FeasibilityScore(
            overall=overall,
            scenario_conservative=scenario_conservative,
            scenario_optimistic=scenario_optimistic,
            weighting_profile=weighting_profile,
            stage2=stage2_payload,
            benchmarks=benchmarks,
            hierarchical=hierarchical,
            market_size=MetricScore(
                score=round(ms),
                label="Market Size",
                description=(
                    f"Addressable market: ~{total_addressable_market:,} families "
                    f"/ {reference_enrollment} reference enrollment "
                    f"= {market_depth_ratio}x market depth"
                    f" (income-qualified base: {addressable['income_qualified_base']:,}"
                    f", Catholic affiliation boost: +{addressable['catholic_boost_contribution']:,})"
                    f" within {catchment_desc}"
                    f" · {market_context} market"
                    + (
                        f" · {state_abbr} {addressable['choice_tier']} school choice program "
                        f"(+${addressable['choice_income_shift']:,} income shift)"
                        if addressable["choice_income_shift"] > 0 else ""
                    )
                    + (
                        f" · School-age pop trend: "
                        f"{'↑' if (trend_dict.get('school_age_pop_pct') or 0) >= 0 else '↓'}"
                        f"{abs(trend_dict.get('school_age_pop_pct') or 0):.1f}% (2017→2022)"
                        + (
                            f" · score {'boosted' if trend_adjustment > 0 else 'reduced'} "
                            f"{abs(trend_adjustment):.0f} pts ({trend_dict.get('trend_label', '')} trend)"
                            if trend_adjustment != 0 else ""
                        )
                        if trend_dict and trend_dict.get("school_age_pop_pct") is not None
                        else ""
                    )
                ),
                weight=round(profile_weights["market_size"] * 100),
                rating=_rating(ms),
            ),
            income=MetricScore(
                score=round(inc),
                label="Income Level",
                description=(
                    f"Median {income_type} income ${(income_for_scoring or 0):,} · "
                    f"{round(high_income_pct * 100, 1)}% of households earn $100k+"
                ),
                weight=round(profile_weights["income"] * 100),
                rating=_rating(inc),
            ),
            competition=MetricScore(
                score=round(comp),
                label="Competition",
                description=comp_desc,
                weight=round(profile_weights["competition"] * 100),
                rating=_rating(comp),
            ),
            family_density=MetricScore(
                score=round(fam),
                label="Family Density",
                description=(
                    f"{families_pct}% of households have children under 18"
                    if families_pct is not None
                    else "Family household data unavailable"
                ),
                weight=round(profile_weights["family_density"] * 100),
                rating=_rating(fam),
            ),
        ),
        recommendation=recommendation,
        recommendation_detail=recommendation_detail,
        data_notes=data_notes,
        trend=trend,
        population_gravity=gravity_map,
        enrollment_forecast=enrollment_forecast,
    )
