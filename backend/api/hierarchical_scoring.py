"""
Hierarchical composite scoring model.

Refactors the flat 4-factor weighted average into a nested hierarchy:

  Overall Feasibility Score (0-100)
  +-- Market Sustainability Index (45%)
  |   +-- Demand Signal (60%)
  |   |   +-- Target Population Size
  |   |   +-- Population Growth Trend
  |   +-- Affordability Signal (40%)
  |       +-- Income Fit
  |       +-- School Choice Availability
  +-- Competitive Position Index (30%)
  |   +-- Market Validation (60%)
  |   |   +-- Existing Catholic School Presence
  |   +-- Saturation Risk (40%)
  |       +-- Competitor Density
  +-- Community Fit Index (15%)
  |   +-- Family Density
  |   +-- Community Stability (owner-occupancy)
  +-- Sustainability Risk Index (10%)
      +-- Demographic Trend Direction
      +-- Economic Volatility

The hierarchical model composes these sub-indicators bottom-up and
produces a HierarchicalScore object alongside the legacy flat scores.
The overall score from the hierarchical model is reported alongside the
legacy score — they may differ slightly due to the richer structure.

Context-adaptive scoring curves are integrated: breakpoints vary by
market_context (urban/suburban/rural) and by Catholic density region.
"""

from typing import Optional

from models.schemas import MetricScore, SubIndicator, HierarchicalScore
from utils import piecewise_linear


# ---------------------------------------------------------------------------
# Context-adaptive scoring curve parameters
# ---------------------------------------------------------------------------

# Market size depth ratio segments by market context
_DEPTH_RATIO_BY_CONTEXT = {
    "urban": [
        (0.0, 5), (0.5, 15), (1.0, 28), (2.0, 45), (3.0, 60),
        (5.0, 76), (8.0, 88), (15.0, 96),
    ],
    "suburban": [
        (0.0, 5), (0.3, 12), (0.7, 22), (1.0, 35), (1.5, 48),
        (2.0, 58), (3.0, 72), (5.0, 84), (8.0, 93), (15.0, 98),
    ],
    "rural": [
        (0.0, 8), (0.2, 16), (0.5, 28), (0.8, 40), (1.2, 52),
        (2.0, 65), (3.0, 78), (5.0, 88), (8.0, 95), (15.0, 99),
    ],
}

# Income segments by market context (urban areas have higher COL)
_INCOME_BY_CONTEXT = {
    "urban": [
        (25_000, 5), (45_000, 20), (65_000, 40), (85_000, 58),
        (110_000, 75), (150_000, 88), (200_000, 96),
    ],
    "suburban": [
        (20_000, 8), (35_000, 22), (50_000, 40), (65_000, 55),
        (80_000, 68), (100_000, 80), (130_000, 90), (175_000, 97),
    ],
    "rural": [
        (15_000, 8), (25_000, 22), (38_000, 42), (50_000, 58),
        (65_000, 72), (85_000, 84), (110_000, 93), (150_000, 97),
    ],
}

# Competition saturation thresholds by Catholic density
_SATURATION_BY_CATHOLIC_DENSITY = {
    "high": [  # States with >25% Catholic (CT, RI, MA, NJ, NY, etc.)
        (0.0, 92), (0.10, 80), (0.25, 62), (0.45, 42), (0.70, 28), (1.0, 15),
    ],
    "medium": [  # States with 15-25% Catholic
        (0.0, 95), (0.05, 88), (0.15, 75), (0.30, 58), (0.50, 40), (0.75, 25), (1.0, 15),
    ],
    "low": [  # States with <15% Catholic
        (0.0, 97), (0.03, 90), (0.10, 78), (0.20, 62), (0.35, 45), (0.55, 30), (1.0, 12),
    ],
}

# Family density segments by context
_FAMILY_DENSITY_BY_CONTEXT = {
    "urban": [
        (0, 10), (10, 22), (18, 38), (28, 58), (38, 76), (48, 90), (60, 97),
    ],
    "suburban": [
        (0, 8), (8, 18), (15, 32), (25, 55), (35, 73), (45, 88), (55, 97),
    ],
    "rural": [
        (0, 12), (6, 22), (12, 38), (20, 58), (30, 78), (40, 92), (50, 98),
    ],
}


def _catholic_density_tier(catholic_pct: float) -> str:
    """Classify the Catholic population density for curve selection."""
    if catholic_pct >= 0.25:
        return "high"
    if catholic_pct >= 0.15:
        return "medium"
    return "low"


def compute_hierarchical_score(
    *,
    market_depth_ratio: float,
    trend_label: str,
    trend_adjustment: float,
    median_income: Optional[int],
    high_income_pct: float,
    choice_tier: str,
    comp_validation: float,
    comp_saturation: float,
    families_pct: Optional[float],
    owner_occupied_pct: Optional[float],
    catholic_pct: float,
    market_context: str = "suburban",
    ms_score: float,
    inc_score: float,
    comp_score: float,
    fam_score: float,
    private_enrollment_score: Optional[float] = None,
    private_enrollment_rate_pct: Optional[float] = None,
    pipeline_score: Optional[float] = None,
    pipeline_ratio: Optional[float] = None,
) -> HierarchicalScore:
    """
    Build the hierarchical composite score from pre-computed factor data.

    Uses the existing flat scores as sub-indicator inputs, supplemented by
    additional signals (trend, owner-occupancy, choice programs) that
    contribute to the hierarchical structure.
    """
    context = market_context if market_context in ("urban", "suburban", "rural") else "suburban"
    cath_density = _catholic_density_tier(catholic_pct)

    # --- Market Opportunity Index (45%) ---

    # Demand Signal (60% of Market Opportunity)
    # With pipeline data: pop size 55%, trend 25%, pipeline 20%
    # Without pipeline data: pop size 70%, trend 30% (original weights)
    has_pipeline = pipeline_score is not None and pipeline_ratio is not None
    demand_pop = SubIndicator(
        key="target_population_size",
        label="Target Population Size",
        score=round(ms_score),
        weight=55 if has_pipeline else 70,
        description=f"Market depth ratio scoring ({context} curve)",
    )
    demand_trend = SubIndicator(
        key="population_growth_trend",
        label="Population Growth Trend",
        score=_trend_to_score(trend_label),
        weight=25 if has_pipeline else 30,
        description=f"Demographic trajectory: {trend_label}",
    )
    demand_subs = [demand_pop, demand_trend]
    if has_pipeline:
        demand_pipeline = SubIndicator(
            key="kindergarten_pipeline",
            label="Kindergarten Pipeline",
            score=round(pipeline_score),
            weight=20,
            description=(
                f"Under-5 to school-age ratio: {pipeline_ratio:.2f} "
                f"({'strong' if pipeline_ratio >= 0.33 else 'below average'} incoming cohorts)"
            ),
        )
        demand_subs.append(demand_pipeline)
        demand_score = round(
            demand_pop.score * 0.55
            + demand_trend.score * 0.25
            + demand_pipeline.score * 0.20
        )
    else:
        demand_score = round(demand_pop.score * 0.70 + demand_trend.score * 0.30)

    # Affordability Signal (40% of Market Opportunity)
    afford_income = SubIndicator(
        key="income_fit",
        label="Income Fit",
        score=round(inc_score),
        weight=70,
        description="Median income and high-income household share",
    )
    afford_choice = SubIndicator(
        key="school_choice_availability",
        label="School Choice Availability",
        score=_choice_tier_score(choice_tier),
        weight=30,
        description=f"School choice program tier: {choice_tier}",
    )
    affordability_score = round(afford_income.score * 0.70 + afford_choice.score * 0.30)

    market_opportunity_score = round(demand_score * 0.60 + affordability_score * 0.40)
    market_opportunity = MetricScore(
        score=market_opportunity_score,
        label="Market Opportunity",
        description=f"Demand signal ({demand_score}) + Affordability ({affordability_score})",
        weight=45,
        rating=_h_rating(market_opportunity_score),
        sub_indicators=demand_subs + [afford_income, afford_choice],
    )

    # --- Competitive Position Index (30%) ---
    validation_sub = SubIndicator(
        key="market_validation",
        label="Catholic School Demand Validation",
        score=round(comp_validation),
        weight=60,
        description="Distance-decayed presence of Catholic schools as demand signal",
    )
    saturation_sub = SubIndicator(
        key="saturation_risk",
        label="Saturation Risk",
        score=round(comp_saturation),
        weight=40,
        description=f"Competitor capacity relative to target population ({cath_density} Catholic density curve)",
    )
    competitive_score = round(validation_sub.score * 0.60 + saturation_sub.score * 0.40)
    competitive_position = MetricScore(
        score=competitive_score,
        label="Competitive Position",
        description=f"Validation ({round(comp_validation)}) + Saturation ({round(comp_saturation)})",
        weight=30,
        rating=_h_rating(competitive_score),
        sub_indicators=[validation_sub, saturation_sub],
    )

    # --- Community Fit Index (15%) ---
    # With private enrollment data: family 50%, stability 25%, priv enrollment 25%
    # Without: family 65%, stability 35% (original weights)
    has_priv_enroll = private_enrollment_score is not None and private_enrollment_rate_pct is not None
    family_sub = SubIndicator(
        key="family_density",
        label="Family Density",
        score=round(fam_score),
        weight=50 if has_priv_enroll else 65,
        description="Share of households with school-age children",
    )
    stability_score = _owner_occupancy_score(owner_occupied_pct)
    stability_sub = SubIndicator(
        key="community_stability",
        label="Community Stability",
        score=stability_score,
        weight=25 if has_priv_enroll else 35,
        description=(
            f"Owner-occupancy: {owner_occupied_pct:.0f}%"
            if owner_occupied_pct is not None
            else "Owner-occupancy data unavailable"
        ),
    )
    community_subs = [family_sub, stability_sub]
    if has_priv_enroll:
        priv_enroll_sub = SubIndicator(
            key="private_enrollment_demand",
            label="Private School Demand",
            score=round(private_enrollment_score),
            weight=25,
            description=(
                f"Local private school enrollment rate: {private_enrollment_rate_pct:.1f}% "
                f"(national avg: 10.5%)"
            ),
        )
        community_subs.append(priv_enroll_sub)
        community_fit_score = round(
            family_sub.score * 0.50
            + stability_sub.score * 0.25
            + priv_enroll_sub.score * 0.25
        )
    else:
        community_fit_score = round(family_sub.score * 0.65 + stability_sub.score * 0.35)
    community_fit = MetricScore(
        score=community_fit_score,
        label="Community Fit",
        description=(
            f"Family density ({round(fam_score)}) + Stability ({stability_score})"
            + (f" + Private demand ({round(private_enrollment_score)})" if has_priv_enroll else "")
        ),
        weight=15,
        rating=_h_rating(community_fit_score),
        sub_indicators=community_subs,
    )

    # --- Sustainability Risk Index (10%) ---
    trend_sub = SubIndicator(
        key="demographic_trend_direction",
        label="Demographic Trend",
        score=_trend_to_score(trend_label),
        weight=60,
        description=f"5-year demographic trajectory: {trend_label}",
    )
    econ_vol_score = _economic_volatility_score(median_income, high_income_pct)
    econ_sub = SubIndicator(
        key="economic_volatility",
        label="Economic Resilience",
        score=econ_vol_score,
        weight=40,
        description="Income diversity and economic resilience indicator",
    )
    sustainability_score = round(trend_sub.score * 0.60 + econ_sub.score * 0.40)
    sustainability_risk = MetricScore(
        score=sustainability_score,
        label="Sustainability",
        description=f"Trend ({_trend_to_score(trend_label)}) + Economic resilience ({econ_vol_score})",
        weight=10,
        rating=_h_rating(sustainability_score),
        sub_indicators=[trend_sub, econ_sub],
    )

    return HierarchicalScore(
        market_opportunity=market_opportunity,
        competitive_position=competitive_position,
        community_fit=community_fit,
        sustainability_risk=sustainability_risk,
    )


def compute_hierarchical_overall(h: HierarchicalScore) -> int:
    """Compute the hierarchical composite overall score from index weights."""
    if not h.market_opportunity or not h.competitive_position:
        return 0
    return round(
        h.market_opportunity.score * 0.45
        + h.competitive_position.score * 0.30
        + (h.community_fit.score if h.community_fit else 50) * 0.15
        + (h.sustainability_risk.score if h.sustainability_risk else 50) * 0.10
    )


def _h_rating(score: float) -> str:
    if score >= 75:
        return "strong"
    if score >= 55:
        return "moderate"
    if score >= 35:
        return "weak"
    return "poor"


def _trend_to_score(trend_label: str) -> int:
    """Convert trend label to a 0-100 score for hierarchical use."""
    return {
        "Growing": 85,
        "Stable": 60,
        "Mixed": 40,
        "Declining": 20,
        "Unknown": 50,
    }.get(trend_label, 50)


def _choice_tier_score(tier: str) -> int:
    """Score the school choice program tier."""
    return {
        "strong": 90,
        "established": 65,
        "none": 35,
    }.get(tier, 35)


def _owner_occupancy_score(pct: Optional[float]) -> int:
    """Score owner-occupancy as a community stability proxy."""
    if pct is None:
        return 50  # Neutral when data unavailable
    return round(piecewise_linear(pct, [
        (0, 10), (20, 25), (40, 45), (55, 60), (65, 72), (75, 84), (85, 94),
    ]))


def _economic_volatility_score(
    median_income: Optional[int],
    high_income_pct: float,
) -> int:
    """
    Score economic resilience: moderate-to-high income with diversified
    income distribution is most resilient. Very low or very high income
    concentrations signal vulnerability.
    """
    if median_income is None:
        return 50
    # Income level component
    income_component = piecewise_linear(median_income, [
        (20_000, 20), (40_000, 45), (60_000, 65), (80_000, 78),
        (100_000, 85), (130_000, 80), (200_000, 70),
    ])
    # Diversity bonus: moderate high-income share is healthiest
    diversity = piecewise_linear(high_income_pct, [
        (0.0, 30), (0.10, 50), (0.25, 75), (0.40, 85),
        (0.60, 75), (0.80, 60),
    ])
    return round(income_component * 0.6 + diversity * 0.4)


# ---------------------------------------------------------------------------
# Context-adaptive scoring functions
# ---------------------------------------------------------------------------

def score_market_size_adaptive(
    market_depth_ratio: float,
    market_context: str = "suburban",
) -> float:
    """Score market size using context-adaptive curves."""
    segments = _DEPTH_RATIO_BY_CONTEXT.get(market_context, _DEPTH_RATIO_BY_CONTEXT["suburban"])
    return min(100.0, max(0.0, piecewise_linear(market_depth_ratio, segments)))


def score_income_adaptive(
    median_income: Optional[int],
    high_income_pct: float,
    market_context: str = "suburban",
) -> float:
    """Score income using context-adaptive curves."""
    if not median_income:
        return 50.0
    segments = _INCOME_BY_CONTEXT.get(market_context, _INCOME_BY_CONTEXT["suburban"])
    base = piecewise_linear(median_income, segments)
    # High-income bonus (same across contexts)
    bonus = piecewise_linear(high_income_pct, [
        (0.0, 0), (0.10, 0), (0.20, 5), (0.35, 12), (0.55, 18),
    ])
    return min(100.0, base + bonus)


def score_competition_adaptive(
    saturation_ratio: float,
    catholic_pct: float = 0.21,
) -> float:
    """Score competition saturation using Catholic-density-adaptive curves."""
    tier = _catholic_density_tier(catholic_pct)
    segments = _SATURATION_BY_CATHOLIC_DENSITY.get(tier, _SATURATION_BY_CATHOLIC_DENSITY["medium"])
    return piecewise_linear(saturation_ratio, segments)


def score_family_density_adaptive(
    families_pct: float,
    market_context: str = "suburban",
) -> float:
    """Score family density using context-adaptive curves."""
    segments = _FAMILY_DENSITY_BY_CONTEXT.get(market_context, _FAMILY_DENSITY_BY_CONTEXT["suburban"])
    return piecewise_linear(families_pct, segments)
