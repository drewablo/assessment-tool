"""
Tests for benchmark percentile rankings and hierarchical scoring.

Run with:  pytest backend/tests/test_benchmarks_and_hierarchical.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from api.benchmarks import _compute_from_distribution
from api.hierarchical_scoring import (
    compute_hierarchical_score,
    compute_hierarchical_overall,
    _trend_to_score,
    _choice_tier_score,
    _owner_occupancy_score,
    _economic_volatility_score,
    _catholic_density_tier,
    score_market_size_adaptive,
    score_income_adaptive,
    score_competition_adaptive,
    score_family_density_adaptive,
)


# ---------------------------------------------------------------------------
# Benchmark distribution-based percentile estimation
# ---------------------------------------------------------------------------

class TestBenchmarkDistribution:
    def test_average_score_near_50th_percentile(self):
        result = _compute_from_distribution(57, "schools")
        assert 45 < result["percentile_national"] < 55

    def test_high_score_high_percentile(self):
        result = _compute_from_distribution(90, "schools")
        assert result["percentile_national"] > 90

    def test_low_score_low_percentile(self):
        result = _compute_from_distribution(20, "schools")
        assert result["percentile_national"] < 15

    def test_percentile_bounded_0_100(self):
        low = _compute_from_distribution(0, "schools")
        high = _compute_from_distribution(100, "schools")
        assert 0 < low["percentile_national"] < 100
        assert 0 < high["percentile_national"] < 100

    def test_monotone_increasing(self):
        scores = [10, 30, 50, 70, 90]
        percentiles = [_compute_from_distribution(s, "schools")["percentile_national"] for s in scores]
        assert percentiles == sorted(percentiles)

    def test_different_ministry_types(self):
        for mt in ("schools", "housing", "elder_care"):
            result = _compute_from_distribution(60, mt)
            assert 0 < result["percentile_national"] < 100

    def test_returns_expected_keys(self):
        result = _compute_from_distribution(65, "schools")
        expected = {
            "percentile_state", "percentile_national", "percentile_msa",
            "state_name", "msa_name",
            "sample_size_state", "sample_size_national", "sample_size_msa",
            "comparable_markets",
        }
        assert set(result.keys()) == expected


# ---------------------------------------------------------------------------
# Hierarchical scoring helpers
# ---------------------------------------------------------------------------

class TestTrendToScore:
    def test_growing_is_high(self):
        assert _trend_to_score("Growing") == 85

    def test_declining_is_low(self):
        assert _trend_to_score("Declining") == 20

    def test_stable_is_moderate(self):
        assert _trend_to_score("Stable") == 60

    def test_unknown_is_neutral(self):
        assert _trend_to_score("Unknown") == 50


class TestChoiceTierScore:
    def test_strong(self):
        assert _choice_tier_score("strong") == 90

    def test_established(self):
        assert _choice_tier_score("established") == 65

    def test_none(self):
        assert _choice_tier_score("none") == 35


class TestOwnerOccupancyScore:
    def test_none_is_neutral(self):
        assert _owner_occupancy_score(None) == 50

    def test_high_occupancy_scores_high(self):
        assert _owner_occupancy_score(80) > 75

    def test_low_occupancy_scores_low(self):
        assert _owner_occupancy_score(15) < 25

    def test_monotone_increasing(self):
        pcts = [0, 20, 40, 60, 80]
        scores = [_owner_occupancy_score(p) for p in pcts]
        assert scores == sorted(scores)


class TestEconomicVolatility:
    def test_none_income_is_neutral(self):
        assert _economic_volatility_score(None, 0.0) == 50

    def test_moderate_income_scores_well(self):
        score = _economic_volatility_score(80_000, 0.30)
        assert score > 65

    def test_very_low_income_scores_poorly(self):
        score = _economic_volatility_score(20_000, 0.05)
        assert score < 35


class TestCatholicDensityTier:
    def test_high(self):
        assert _catholic_density_tier(0.35) == "high"

    def test_medium(self):
        assert _catholic_density_tier(0.20) == "medium"

    def test_low(self):
        assert _catholic_density_tier(0.10) == "low"


# ---------------------------------------------------------------------------
# Context-adaptive scoring curves
# ---------------------------------------------------------------------------

class TestAdaptiveMarketSize:
    def test_urban_harder_than_suburban(self):
        ratio = 3.0
        urban = score_market_size_adaptive(ratio, "urban")
        suburban = score_market_size_adaptive(ratio, "suburban")
        assert urban < suburban

    def test_rural_easier_than_suburban(self):
        ratio = 1.0
        rural = score_market_size_adaptive(ratio, "rural")
        suburban = score_market_size_adaptive(ratio, "suburban")
        assert rural > suburban

    def test_monotone_increasing(self):
        for ctx in ("urban", "suburban", "rural"):
            ratios = [0.0, 0.5, 1.0, 2.0, 5.0, 15.0]
            scores = [score_market_size_adaptive(r, ctx) for r in ratios]
            assert scores == sorted(scores), f"Non-monotone for {ctx}"

    def test_bounded(self):
        for ctx in ("urban", "suburban", "rural"):
            assert 0 <= score_market_size_adaptive(0, ctx) <= 100
            assert 0 <= score_market_size_adaptive(20, ctx) <= 100


class TestAdaptiveIncome:
    def test_urban_higher_thresholds(self):
        """Urban areas need higher income for same score."""
        income = 60_000
        urban = score_income_adaptive(income, 0.0, "urban")
        suburban = score_income_adaptive(income, 0.0, "suburban")
        assert urban < suburban

    def test_rural_lower_thresholds(self):
        income = 40_000
        rural = score_income_adaptive(income, 0.0, "rural")
        suburban = score_income_adaptive(income, 0.0, "suburban")
        assert rural > suburban

    def test_none_income_is_neutral(self):
        assert score_income_adaptive(None, 0.0) == 50.0


class TestAdaptiveCompetition:
    def test_high_catholic_density_more_tolerant(self):
        """High-Catholic areas tolerate more saturation before score drops."""
        sat = 0.30
        high = score_competition_adaptive(sat, 0.35)
        low = score_competition_adaptive(sat, 0.10)
        assert high > low

    def test_zero_saturation_all_high(self):
        for pct in (0.10, 0.20, 0.35):
            assert score_competition_adaptive(0.0, pct) > 90


class TestAdaptiveFamilyDensity:
    def test_rural_rewards_lower_density(self):
        pct = 15.0
        rural = score_family_density_adaptive(pct, "rural")
        urban = score_family_density_adaptive(pct, "urban")
        assert rural > urban


# ---------------------------------------------------------------------------
# Full hierarchical score computation
# ---------------------------------------------------------------------------

class TestHierarchicalScore:
    def _make_hierarchical(self, **overrides):
        defaults = dict(
            market_depth_ratio=5.0,
            trend_label="Stable",
            trend_adjustment=0.0,
            median_income=80_000,
            high_income_pct=0.30,
            choice_tier="none",
            comp_validation=65.0,
            comp_saturation=70.0,
            families_pct=30.0,
            owner_occupied_pct=65.0,
            catholic_pct=0.25,
            market_context="suburban",
            ms_score=75.0,
            inc_score=70.0,
            comp_score=68.0,
            fam_score=60.0,
        )
        defaults.update(overrides)
        return compute_hierarchical_score(**defaults)

    def test_returns_all_four_indices(self):
        h = self._make_hierarchical()
        assert h.market_opportunity is not None
        assert h.competitive_position is not None
        assert h.community_fit is not None
        assert h.sustainability_risk is not None

    def test_weights_sum_to_100(self):
        h = self._make_hierarchical()
        total = (
            h.market_opportunity.weight
            + h.competitive_position.weight
            + h.community_fit.weight
            + h.sustainability_risk.weight
        )
        assert total == 100

    def test_overall_in_range(self):
        h = self._make_hierarchical()
        overall = compute_hierarchical_overall(h)
        assert 0 <= overall <= 100

    def test_strong_market_scores_high(self):
        h = self._make_hierarchical(
            ms_score=90, inc_score=85, comp_validation=80, comp_saturation=85,
            fam_score=75, trend_label="Growing",
        )
        overall = compute_hierarchical_overall(h)
        assert overall >= 70

    def test_weak_market_scores_low(self):
        h = self._make_hierarchical(
            ms_score=20, inc_score=25, comp_validation=15, comp_saturation=30,
            fam_score=20, trend_label="Declining",
        )
        overall = compute_hierarchical_overall(h)
        assert overall < 35

    def test_sub_indicators_present(self):
        h = self._make_hierarchical()
        assert len(h.market_opportunity.sub_indicators) == 4
        assert len(h.competitive_position.sub_indicators) == 2
        assert len(h.community_fit.sub_indicators) == 2
        assert len(h.sustainability_risk.sub_indicators) == 2

    def test_growing_trend_boosts_sustainability(self):
        growing = self._make_hierarchical(trend_label="Growing")
        declining = self._make_hierarchical(trend_label="Declining")
        assert growing.sustainability_risk.score > declining.sustainability_risk.score

    def test_school_choice_boosts_affordability(self):
        choice = self._make_hierarchical(choice_tier="strong")
        no_choice = self._make_hierarchical(choice_tier="none")
        assert choice.market_opportunity.score > no_choice.market_opportunity.score
