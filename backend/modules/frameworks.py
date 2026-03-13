from __future__ import annotations

from typing import Optional

from api.benchmarks import compute_benchmarks
from models.schemas import HierarchicalScore, MetricScore, SubIndicator


def score_rating(score: int) -> str:
    if score >= 75:
        return "strong"
    if score >= 55:
        return "moderate"
    if score >= 35:
        return "weak"
    return "poor"


def _rating(score: int) -> str:
    """Backward-compatible alias for existing imports/tests."""
    return score_rating(score)


def build_generic_hierarchical(
    *,
    market_size: int,
    income: int,
    competition: int,
    family_density: int,
    occupancy: Optional[int] = None,
    workforce: Optional[int] = None,
) -> HierarchicalScore:
    """Module-agnostic hierarchical breakdown reusable across non-school tracks."""
    market_opportunity_score = round(market_size * 0.6 + income * 0.4)
    market_opportunity = MetricScore(
        score=market_opportunity_score,
        label="Market Opportunity",
        description="Demand depth and affordability/need signal",
        weight=45,
        rating=score_rating(market_opportunity_score),
        sub_indicators=[
            SubIndicator(key="demand_depth", label="Demand Depth", score=market_size, weight=60, description="Target population demand signal"),
            SubIndicator(key="affordability_need", label="Affordability / Need", score=income, weight=40, description="Income alignment for mission model"),
        ],
    )

    competitive_subs = [
        SubIndicator(key="capacity_pressure", label="Capacity Pressure", score=competition, weight=70 if occupancy is not None else 100, description="Competitor saturation and pressure")
    ]
    if occupancy is not None:
        competitive_subs.append(
            SubIndicator(key="occupancy_tightness", label="Occupancy Tightness", score=occupancy, weight=30, description="Nearby capacity utilization signal")
        )
    competitive_position_score = round(sum(s.score * (s.weight / 100) for s in competitive_subs))
    competitive_position = MetricScore(
        score=competitive_position_score,
        label="Competitive Position",
        description="Supply pressure and utilization context",
        weight=30,
        rating=score_rating(competitive_position_score),
        sub_indicators=competitive_subs,
    )

    community_subs = [
        SubIndicator(key="local_fit", label="Local Fit", score=family_density, weight=70 if workforce is not None else 100, description="Household/senior composition fit")
    ]
    if workforce is not None:
        community_subs.append(
            SubIndicator(key="workforce_readiness", label="Workforce Readiness", score=workforce, weight=30, description="Labor-market staffing viability")
        )
    community_fit_score = round(sum(s.score * (s.weight / 100) for s in community_subs))
    community_fit = MetricScore(
        score=community_fit_score,
        label="Community Fit",
        description="Population fit and local execution readiness",
        weight=15,
        rating=score_rating(community_fit_score),
        sub_indicators=community_subs,
    )

    sustainability_score = round(max(0, min(100, 100 - abs(market_size - competition))))
    sustainability_risk = MetricScore(
        score=sustainability_score,
        label="Sustainability Risk",
        description="Balance between local demand and external supply pressure",
        weight=10,
        rating=score_rating(sustainability_score),
        sub_indicators=[
            SubIndicator(
                key="demand_supply_balance",
                label="Demand-Supply Balance",
                score=sustainability_score,
                weight=100,
                description="Lower mismatch between demand and competition improves sustainability",
            )
        ],
    )

    return HierarchicalScore(
        market_opportunity=market_opportunity,
        competitive_position=competitive_position,
        community_fit=community_fit,
        sustainability_risk=sustainability_risk,
    )


async def compute_module_benchmarks(
    *,
    ministry_type: str,
    overall: int,
    state_fips: str,
    lat: float,
    lon: float,
    demographics: dict,
    market_size: int,
    income: int,
    competition: int,
    family_density: int,
) -> dict:
    return await compute_benchmarks(
        overall_score=overall,
        state_fips=state_fips,
        ministry_type=ministry_type,
        lat=lat,
        lon=lon,
        demographics=demographics,
        market_size_score=market_size,
        income_score=income,
        competition_score=competition,
        family_density_score=family_density,
    )
