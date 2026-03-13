"""
Benchmark percentile ranking engine.

Computes percentile ranks for a feasibility score against:
  - All analyses in the same state (percentile_state)
  - All analyses nationally (percentile_national)
  - All analyses in the same MSA/metro area (percentile_msa)

Also identifies comparable markets: tracts with similar demographic
profiles but potentially different scores, for contextual comparison.

When USE_DB is enabled and tract_feasibility_scores are populated,
percentiles are computed from precomputed tract scores. Otherwise,
percentiles are computed from the analysis_history table (past analyses).
"""

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

USE_DB = os.getenv("USE_DB", "").lower() in ("1", "true", "yes")


async def compute_benchmarks(
    overall_score: int,
    state_fips: str,
    ministry_type: str,
    lat: float,
    lon: float,
    demographics: dict,
    market_size_score: int = 0,
    income_score: int = 0,
    competition_score: int = 0,
    family_density_score: int = 0,
) -> dict:
    """
    Compute benchmark percentile rankings for a feasibility score.

    Returns a dict matching the BenchmarkPercentiles schema.
    """
    if USE_DB:
        result = await _compute_from_db(
            overall_score, state_fips, ministry_type, lat, lon,
            demographics, market_size_score, income_score,
            competition_score, family_density_score,
        )
        # If DB returned valid percentiles, use them; otherwise fall back
        # to the distribution estimate (covers empty DB or query errors).
        if result.get("percentile_national") is not None:
            return result
        fallback = _compute_from_distribution(overall_score, ministry_type)
        # Preserve any comparable markets or metadata the DB did return
        for key, value in result.items():
            if value is not None and value != []:
                fallback[key] = value
        return fallback

    # Without DB, return estimate-based benchmarks using score distribution
    return _compute_from_distribution(overall_score, ministry_type)


def _compute_from_distribution(overall_score: int, ministry_type: str) -> dict:
    """
    Estimate percentile from the expected score distribution.

    The scoring model produces roughly normal distributions centered around
    55-60 with standard deviation ~15 based on calibration testing.
    We use a logistic CDF approximation for smooth percentile mapping.
    """
    import math

    # Distribution parameters by ministry type (calibrated from test runs)
    params = {
        "schools": {"mu": 57, "s": 15},
        "housing": {"mu": 55, "s": 16},
        "elder_care": {"mu": 54, "s": 14},
    }
    p = params.get(ministry_type, params["schools"])

    # Logistic CDF: P(X <= x) = 1 / (1 + exp(-(x - mu) / s_scaled))
    # Scale factor converts logistic spread to approximate normal SD
    s_scaled = p["s"] * 0.5513  # pi / (sqrt(3) * 1.0)
    z = (overall_score - p["mu"]) / s_scaled
    percentile = 100.0 / (1.0 + math.exp(-z))
    percentile = round(min(99.9, max(0.1, percentile)), 1)

    return {
        "percentile_state": percentile,       # Same estimate without state data
        "percentile_national": percentile,
        "percentile_msa": None,
        "state_name": None,
        "msa_name": None,
        "sample_size_state": None,
        "sample_size_national": None,
        "sample_size_msa": None,
        "comparable_markets": [],
    }


async def _compute_from_db(
    overall_score: int,
    state_fips: str,
    ministry_type: str,
    lat: float,
    lon: float,
    demographics: dict,
    market_size_score: int,
    income_score: int,
    competition_score: int,
    family_density_score: int,
) -> dict:
    """Compute percentiles from precomputed tract feasibility scores in the DB."""
    from db.connection import get_session
    from sqlalchemy import select, func, and_
    from db.models import TractFeasibilityScore

    result = {
        "percentile_state": None,
        "percentile_national": None,
        "percentile_msa": None,
        "state_name": None,
        "msa_name": None,
        "sample_size_state": None,
        "sample_size_national": None,
        "sample_size_msa": None,
        "comparable_markets": [],
    }

    try:
        async with get_session() as session:
            # National percentile
            national_below = await session.execute(
                select(func.count()).select_from(TractFeasibilityScore).where(
                    and_(
                        TractFeasibilityScore.ministry_type == ministry_type,
                        TractFeasibilityScore.overall_score < overall_score,
                    )
                )
            )
            national_total = await session.execute(
                select(func.count()).select_from(TractFeasibilityScore).where(
                    TractFeasibilityScore.ministry_type == ministry_type,
                )
            )
            n_below = national_below.scalar() or 0
            n_total = national_total.scalar() or 0

            if n_total > 0:
                result["percentile_national"] = round(n_below / n_total * 100, 1)
                result["sample_size_national"] = n_total

            # State percentile
            if state_fips and len(state_fips) >= 2:
                state_prefix = state_fips[:2]
                state_below = await session.execute(
                    select(func.count()).select_from(TractFeasibilityScore).where(
                        and_(
                            TractFeasibilityScore.ministry_type == ministry_type,
                            TractFeasibilityScore.overall_score < overall_score,
                            TractFeasibilityScore.geoid.startswith(state_prefix),
                        )
                    )
                )
                state_total = await session.execute(
                    select(func.count()).select_from(TractFeasibilityScore).where(
                        and_(
                            TractFeasibilityScore.ministry_type == ministry_type,
                            TractFeasibilityScore.geoid.startswith(state_prefix),
                        )
                    )
                )
                s_below = state_below.scalar() or 0
                s_total = state_total.scalar() or 0

                if s_total > 0:
                    result["percentile_state"] = round(s_below / s_total * 100, 1)
                    result["sample_size_state"] = s_total

            # Comparable markets: find tracts with similar factor scores
            comparable = await _find_comparable_markets(
                session, ministry_type, overall_score,
                market_size_score, income_score, competition_score,
                family_density_score, state_fips,
            )
            result["comparable_markets"] = comparable

    except Exception:
        # Don't let benchmark computation break the analysis
        logger.warning("Benchmark DB query failed, will fall back to distribution estimate", exc_info=True)

    return result


async def _find_comparable_markets(
    session,
    ministry_type: str,
    overall_score: int,
    market_size_score: int,
    income_score: int,
    competition_score: int,
    family_density_score: int,
    state_fips: str,
    limit: int = 8,
):
    """
    Find tracts with the most similar demographic/factor profile.

    Uses Euclidean distance in the 4D factor-score space to find the
    closest matches, then returns them sorted by overall score for
    contextual comparison.
    """
    from sqlalchemy import select, func, and_, cast, Float
    from db.models import TractFeasibilityScore

    # Score similarity = sqrt(sum of squared differences across factors)
    # We want tracts with similar PROFILES but potentially different scores
    similarity = func.sqrt(
        func.pow(cast(TractFeasibilityScore.market_size_score, Float) - market_size_score, 2)
        + func.pow(cast(TractFeasibilityScore.income_score, Float) - income_score, 2)
        + func.pow(cast(TractFeasibilityScore.competition_score, Float) - competition_score, 2)
        + func.pow(cast(TractFeasibilityScore.family_density_score, Float) - family_density_score, 2)
    )

    try:
        stmt = (
            select(
                TractFeasibilityScore.geoid,
                TractFeasibilityScore.overall_score,
                TractFeasibilityScore.market_size_score,
                TractFeasibilityScore.income_score,
                TractFeasibilityScore.competition_score,
                TractFeasibilityScore.family_density_score,
                TractFeasibilityScore.percentile_state,
                TractFeasibilityScore.percentile_national,
                similarity.label("similarity"),
            )
            .where(
                and_(
                    TractFeasibilityScore.ministry_type == ministry_type,
                    TractFeasibilityScore.market_size_score.is_not(None),
                    TractFeasibilityScore.income_score.is_not(None),
                )
            )
            .order_by(similarity)
            .offset(1)  # Skip exact self-match
            .limit(limit)
        )
        rows = await session.execute(stmt)
        return [
            {
                "geoid": row.geoid,
                "overall_score": row.overall_score,
                "market_size_score": row.market_size_score,
                "income_score": row.income_score,
                "competition_score": row.competition_score,
                "family_density_score": row.family_density_score,
                "percentile_state": row.percentile_state,
                "percentile_national": row.percentile_national,
                "similarity_distance": round(row.similarity, 1) if row.similarity else None,
            }
            for row in rows.all()
        ]
    except Exception:
        return []
