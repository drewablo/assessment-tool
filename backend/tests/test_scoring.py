"""
Unit tests for core scoring logic.

Run with:  pytest backend/tests/  (from repo root)
         or pytest              (from backend/ directory)
"""

import sys
import os

# Allow imports from backend/ root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from utils import piecewise_linear, haversine_miles
from api.analysis import (
    _score_market_size,
    _score_income,
    _score_competition,
    _score_family_density,
    _score_private_enrollment_rate,
    _score_pipeline_ratio,
    _build_recommendation,
    _estimate_addressable_market,
    _rating,
    _data_confidence,
    _scenario_scores,
    _trend_score_adjustment,
    _build_enrollment_forecast,
    _MARKET_DEPTH_RATIO_SEGMENTS,
    _REFERENCE_ENROLLMENT,
    _MARKET_CONTEXT_ADJUSTMENT,
)
from api.bls_workforce import score_workforce_availability
from modules.housing import _score_housing
from modules.elder_care import _facility_beds, _score_elder_care


# ---------------------------------------------------------------------------
# utils.piecewise_linear
# ---------------------------------------------------------------------------

class TestPiecewiseLinear:
    SEGMENTS = [(0, 0), (50, 50), (100, 100)]

    def test_below_range_clamps_to_first(self):
        assert piecewise_linear(-10, self.SEGMENTS) == 0.0

    def test_above_range_clamps_to_last(self):
        assert piecewise_linear(200, self.SEGMENTS) == 100.0

    def test_at_first_breakpoint(self):
        assert piecewise_linear(0, self.SEGMENTS) == 0.0

    def test_at_last_breakpoint(self):
        assert piecewise_linear(100, self.SEGMENTS) == 100.0

    def test_midpoint_interpolation(self):
        assert piecewise_linear(25, self.SEGMENTS) == pytest.approx(25.0)

    def test_non_linear_segments(self):
        # Curve that accelerates: (0,0) -> (10,50) -> (100,100)
        segments = [(0, 0), (10, 50), (100, 100)]
        assert piecewise_linear(5, segments) == pytest.approx(25.0)
        assert piecewise_linear(55, segments) == pytest.approx(75.0, rel=0.01)

    def test_single_segment_pair_clamps(self):
        segments = [(10, 20), (20, 80)]
        assert piecewise_linear(5, segments) == 20.0
        assert piecewise_linear(25, segments) == 80.0
        assert piecewise_linear(15, segments) == pytest.approx(50.0)


# ---------------------------------------------------------------------------
# utils.haversine_miles
# ---------------------------------------------------------------------------

class TestHaversineMiles:
    def test_same_point_is_zero(self):
        assert haversine_miles(41.8827, -87.6233, 41.8827, -87.6233) == pytest.approx(0.0, abs=1e-6)

    def test_chicago_to_new_york(self):
        # Approximate known distance ~714 miles
        dist = haversine_miles(41.8827, -87.6233, 40.7128, -74.0060)
        assert 700 < dist < 730

    def test_short_distance_accuracy(self):
        # Two points ~1 mile apart along a meridian
        dist = haversine_miles(41.0, -87.0, 41.0145, -87.0)
        assert 0.9 < dist < 1.1

    def test_symmetry(self):
        d1 = haversine_miles(34.0, -118.0, 41.0, -87.0)
        d2 = haversine_miles(41.0, -87.0, 34.0, -118.0)
        assert d1 == pytest.approx(d2)


# ---------------------------------------------------------------------------
# analysis._rating
# ---------------------------------------------------------------------------

class TestRating:
    def test_strong(self):
        assert _rating(75) == "strong"
        assert _rating(100) == "strong"

    def test_moderate(self):
        assert _rating(55) == "moderate"
        assert _rating(74) == "moderate"

    def test_weak(self):
        assert _rating(35) == "weak"
        assert _rating(54) == "weak"

    def test_poor(self):
        assert _rating(0) == "poor"
        assert _rating(34) == "poor"


# ---------------------------------------------------------------------------
# analysis._score_market_size
# ---------------------------------------------------------------------------

class TestScoreMarketSize:
    """Market size scoring operates on market_depth_ratio = addressable / reference_enrollment.
    Ratio 1.0 = barely enough for one school; 3.0 = healthy; 8+ = very strong."""

    def test_zero_ratio_scores_low(self):
        score = _score_market_size(0.0)
        assert score == pytest.approx(5.0)

    def test_high_ratio_scores_high(self):
        score = _score_market_size(15.0)
        assert score >= 95

    def test_urban_scores_lower_than_suburban_for_same_ratio(self):
        ratio = 3.0
        suburban = _score_market_size(ratio, "suburban")
        urban = _score_market_size(ratio, "urban")
        assert urban < suburban

    def test_rural_scores_higher_than_suburban_for_same_ratio(self):
        ratio = 3.0
        suburban = _score_market_size(ratio, "suburban")
        rural = _score_market_size(ratio, "rural")
        assert rural > suburban

    def test_score_is_monotone_increasing(self):
        ratios = [0.0, 0.3, 0.7, 1.0, 1.5, 2.0, 3.0, 5.0, 8.0, 15.0]
        scores = [_score_market_size(r) for r in ratios]
        assert scores == sorted(scores)

    def test_strong_suburban_market_scores_75_plus(self):
        """Coed K-12, 4000 addressable / 400 ref = ratio 10 → strong."""
        score = _score_market_size(10.0, "suburban")
        assert score >= 75

    def test_rural_low_ratio_moderate(self):
        """Rural market, ratio ~1.0 → with +4 rural adjustment: ~39."""
        score = _score_market_size(1.0, "rural")
        assert 30 < score < 50

    def test_girls_hs_affluent_suburb_scores_well(self):
        """Girls HS in affluent suburb: ~629 addressable / 225 ref = 2.8 ratio → 60-75."""
        score = _score_market_size(2.8, "suburban")
        assert 60 <= score <= 75

    def test_same_market_comparable_across_school_types(self):
        """Same geographic market should score within ~15 points regardless of school type.
        Coed K-12: 4000/400=10.0 vs Girls HS: ~1000/225=4.4 — both strong markets."""
        coed_k12 = _score_market_size(10.0, "suburban")
        girls_hs = _score_market_size(4.4, "suburban")
        assert abs(coed_k12 - girls_hs) < 15


# ---------------------------------------------------------------------------
# analysis._score_income
# ---------------------------------------------------------------------------

class TestScoreIncome:
    def test_none_income_returns_neutral(self):
        assert _score_income(None, 0.0) == pytest.approx(50.0)

    def test_low_income_scores_low(self):
        assert _score_income(25_000, 0.0) < 15

    def test_high_income_scores_high(self):
        assert _score_income(150_000, 0.5) > 90

    def test_high_income_pct_adds_bonus(self):
        base = _score_income(80_000, 0.0)
        boosted = _score_income(80_000, 0.4)
        assert boosted > base

    def test_score_capped_at_100(self):
        assert _score_income(200_000, 1.0) <= 100.0


# ---------------------------------------------------------------------------
# analysis._score_family_density
# ---------------------------------------------------------------------------

class TestScoreFamilyDensity:
    def test_none_values_return_neutral(self):
        assert _score_family_density(None, 1000) == pytest.approx(50.0)
        assert _score_family_density(200, None) == pytest.approx(50.0)
        assert _score_family_density(200, 0) == pytest.approx(50.0)

    def test_low_density_scores_low(self):
        # 5% of households have children
        assert _score_family_density(50, 1_000) < 20

    def test_high_density_scores_high(self):
        # 45% of households have children
        assert _score_family_density(450, 1_000) > 85

    def test_monotone_increasing(self):
        households = 1_000
        children_counts = [0, 80, 150, 250, 350, 450, 550]
        scores = [_score_family_density(c, households) for c in children_counts]
        assert scores == sorted(scores)


# ---------------------------------------------------------------------------
# analysis._score_competition
# ---------------------------------------------------------------------------

class TestScoreCompetition:
    def _school(self, dist, is_catholic=True, enrollment=200, tier_weight=1.0, competitor_tier="direct"):
        return {
            "distance_miles": dist, "is_catholic": is_catholic,
            "lat": 41.0, "lon": -87.0, "enrollment": enrollment,
            "tier_weight": tier_weight, "competitor_tier": competitor_tier,
        }

    def test_no_schools_returns_unserved_market_score(self):
        combined, val, sat = _score_competition([], 1000)
        assert sat == pytest.approx(90.0)
        assert 0 <= combined <= 100

    def test_nearby_catholic_school_raises_validation(self):
        _, val_close, _ = _score_competition([self._school(0.5)], 1000)
        _, val_far, _ = _score_competition([self._school(10.0)], 1000)
        assert val_close > val_far

    def test_high_saturation_lowers_sat_score(self):
        # Many large schools close by vs. few small ones
        large_schools = [self._school(1.0, enrollment=500) for _ in range(10)]
        small_schools = [self._school(5.0, enrollment=50)]
        _, _, sat_high = _score_competition(large_schools, 500)
        _, _, sat_low = _score_competition(small_schools, 500)
        assert sat_high < sat_low

    def test_non_catholic_schools_ignored_for_validation(self):
        non_catholic = [self._school(0.5, is_catholic=False, tier_weight=0.4, competitor_tier="moderate")]
        combined, val, sat = _score_competition(non_catholic, 1000)
        # Should behave like unserved market for validation
        assert sat > 80

    def test_tier_weight_reduces_saturation_pressure(self):
        """A secular school (tier_weight=0.4) exerts less saturation pressure than a Catholic school (1.0)."""
        catholic = [self._school(1.0, is_catholic=True, enrollment=500, tier_weight=1.0)]
        secular = [self._school(1.0, is_catholic=False, enrollment=500, tier_weight=0.4, competitor_tier="moderate")]
        # Add one Catholic school to both so validation is non-zero and we measure saturation
        base_catholic = self._school(5.0, is_catholic=True, enrollment=100)
        _, _, sat_catholic = _score_competition(catholic + [base_catholic], 500)
        _, _, sat_secular = _score_competition(secular + [base_catholic], 500)
        # Full-weight Catholic competitor should create more saturation than reduced-weight secular
        assert sat_catholic < sat_secular

    def test_weak_tier_less_impact_than_direct(self):
        """Special emphasis schools (tier_weight=0.15) exert less saturation than direct (1.0)."""
        base = self._school(5.0, is_catholic=True, enrollment=100)
        weak_school = self._school(2.0, is_catholic=False, enrollment=300, tier_weight=0.15, competitor_tier="weak")
        direct_school = self._school(2.0, is_catholic=True, enrollment=300, tier_weight=1.0)
        _, _, sat_with_weak = _score_competition([base, weak_school], 500)
        _, _, sat_with_direct = _score_competition([base, direct_school], 500)
        # Direct competitor should create more saturation pressure (lower score)
        assert sat_with_direct < sat_with_weak

    def test_default_tier_weight_when_missing(self):
        """Schools without tier_weight default to 0.4 (moderate)."""
        school_no_tier = {"distance_miles": 1.0, "is_catholic": True, "lat": 41.0, "lon": -87.0, "enrollment": 200}
        school_explicit = self._school(1.0, is_catholic=True, enrollment=200, tier_weight=0.4)
        _, val_no_tier, sat_no_tier = _score_competition([school_no_tier], 1000)
        _, val_explicit, sat_explicit = _score_competition([school_explicit], 1000)
        assert val_no_tier == pytest.approx(val_explicit)
        assert sat_no_tier == pytest.approx(sat_explicit)


class TestEnrollmentForecast:
    def test_declining_market_sets_cliff_year(self):
        forecast = _build_enrollment_forecast(
            school_age_population=180,
            pipeline_ratio=0.18,
            trend_dict={"school_age_pop_pct": -25.0},
        )
        assert forecast.estimated_cliff_year is not None
        assert forecast.decline_risk == "high"

    def test_growth_market_has_low_risk(self):
        forecast = _build_enrollment_forecast(
            school_age_population=450,
            pipeline_ratio=0.45,
            trend_dict={"school_age_pop_pct": 6.0},
        )
        assert forecast.estimated_cliff_year is None
        assert forecast.decline_risk == "low"


# ---------------------------------------------------------------------------
# analysis._data_confidence
# ---------------------------------------------------------------------------

class TestDataConfidence:
    def test_county_level_is_medium(self):
        assert _data_confidence({"data_geography": "county"}) == "medium"

    def test_low_income_cv_is_high(self):
        assert _data_confidence({"data_geography": "tract", "income_moe_pct": 0.05}) == "high"

    def test_high_income_cv_is_low(self):
        assert _data_confidence({"data_geography": "tract", "income_moe_pct": 0.35}) == "low"

    def test_tract_count_fallback_high(self):
        assert _data_confidence({"data_geography": "tract", "tract_count": 20}) == "high"

    def test_tract_count_fallback_low(self):
        assert _data_confidence({"data_geography": "tract", "tract_count": 2}) == "low"


# ---------------------------------------------------------------------------
# analysis._scenario_scores
# ---------------------------------------------------------------------------

class TestScenarioScores:
    def test_high_confidence_band_is_6(self):
        conservative, optimistic = _scenario_scores(60, "high")
        assert optimistic - conservative == 12

    def test_scores_clamped_to_0_100(self):
        conservative, optimistic = _scenario_scores(3, "low")
        assert conservative == 0
        conservative, optimistic = _scenario_scores(97, "low")
        assert optimistic == 100


# ---------------------------------------------------------------------------
# analysis._trend_score_adjustment
# ---------------------------------------------------------------------------

class TestTrendScoreAdjustment:
    def test_growing_is_positive(self):
        assert _trend_score_adjustment("Growing") > 0

    def test_declining_is_negative(self):
        assert _trend_score_adjustment("Declining") < 0

    def test_stable_is_zero(self):
        assert _trend_score_adjustment("Stable") == 0.0

    def test_unknown_is_zero(self):
        assert _trend_score_adjustment("Unknown") == 0.0


# ---------------------------------------------------------------------------
# analysis._build_recommendation
# ---------------------------------------------------------------------------

class TestBuildRecommendation:
    def test_strong_market(self):
        title, detail = _build_recommendation(
            overall=80, ms=80, inc=80, comp=80, fam=80,
            est_catholic_school_age=2000, n_catholic_schools=2,
            catchment_desc="15-minute drive catchment",
            total_addressable_market=650,
        )
        assert title == "Strong Market Opportunity"
        assert "650" in detail

    def test_moderate_market_mentions_strengths(self):
        title, detail = _build_recommendation(
            overall=60, ms=70, inc=65, comp=40, fam=45,
            est_catholic_school_age=800, n_catholic_schools=1,
            catchment_desc="15-minute drive catchment",
            total_addressable_market=250,
        )
        assert title == "Moderate Market Opportunity"
        assert "sizable addressable market" in detail or "income" in detail

    def test_challenging_market(self):
        title, detail = _build_recommendation(
            overall=40, ms=30, inc=35, comp=55, fam=40,
            est_catholic_school_age=300, n_catholic_schools=0,
            catchment_desc="20-mile radius",
            total_addressable_market=80,
        )
        assert title == "Challenging Market Conditions"

    def test_difficult_market(self):
        title, detail = _build_recommendation(
            overall=20, ms=15, inc=20, comp=30, fam=15,
            est_catholic_school_age=100, n_catholic_schools=3,
            catchment_desc="10-mile radius",
            total_addressable_market=30,
        )
        assert title == "Difficult Market Conditions"

    def test_unserved_strong_market_mentions_unserved(self):
        title, detail = _build_recommendation(
            overall=80, ms=85, inc=82, comp=70, fam=78,
            est_catholic_school_age=3000, n_catholic_schools=0,
            catchment_desc="20-minute drive catchment",
            total_addressable_market=900,
        )
        assert "unserved market" in detail.lower()


# ---------------------------------------------------------------------------
# analysis._estimate_addressable_market
# ---------------------------------------------------------------------------

def _make_income_dist(shares, total_hh):
    """Helper: build income_distribution list from bracket share percentages.
    shares is a dict mapping bracket midpoints to % of households.
    Midpoints must match B19001 bracket midpoints used in census.py."""
    midpoints = [5_000, 12_500, 17_500, 22_500, 27_500, 32_500, 37_500,
                 42_500, 47_500, 55_000, 67_500, 87_500, 112_500, 137_500,
                 175_000, 250_000]
    result = []
    for mp in midpoints:
        pct = shares.get(mp, 0.0)
        result.append((mp, int(total_hh * pct)))
    return result


# Realistic income distributions for mental test cases
_WEALTHY_SUBURB_SHARES = {
    5_000: 0.02, 12_500: 0.01, 17_500: 0.01, 22_500: 0.02, 27_500: 0.02,
    32_500: 0.02, 37_500: 0.03, 42_500: 0.03, 47_500: 0.02,
    55_000: 0.05, 67_500: 0.07, 87_500: 0.09,
    112_500: 0.13, 137_500: 0.12, 175_000: 0.18, 250_000: 0.18,
}
_RURAL_LOW_INCOME_SHARES = {
    5_000: 0.08, 12_500: 0.06, 17_500: 0.06, 22_500: 0.05, 27_500: 0.05,
    32_500: 0.06, 37_500: 0.05, 42_500: 0.05, 47_500: 0.06,
    55_000: 0.10, 67_500: 0.10, 87_500: 0.08,
    112_500: 0.06, 137_500: 0.04, 175_000: 0.04, 250_000: 0.06,
}
_VERY_WEALTHY_SHARES = {
    5_000: 0.01, 12_500: 0.01, 17_500: 0.01, 22_500: 0.01, 27_500: 0.01,
    32_500: 0.01, 37_500: 0.02, 42_500: 0.02, 47_500: 0.02,
    55_000: 0.04, 67_500: 0.04, 87_500: 0.06,
    112_500: 0.10, 137_500: 0.12, 175_000: 0.22, 250_000: 0.30,
}
_MODERATE_INCOME_SHARES = {
    5_000: 0.04, 12_500: 0.03, 17_500: 0.03, 22_500: 0.04, 27_500: 0.04,
    32_500: 0.05, 37_500: 0.05, 42_500: 0.05, 47_500: 0.05,
    55_000: 0.08, 67_500: 0.10, 87_500: 0.12,
    112_500: 0.10, 137_500: 0.08, 175_000: 0.08, 250_000: 0.06,
}


class TestEstimateAddressableMarket:
    """Income-first addressable market model tests."""

    def test_zero_population_returns_zero(self):
        result = _estimate_addressable_market(0, 0.25, [])
        assert result["total_addressable_market"] == 0
        assert result["income_qualified_base"] == 0
        assert result["catholic_boost_contribution"] == 0

    def test_empty_income_distribution_uses_national_rate(self):
        """When no income distribution data, fall back to national private school rate."""
        result = _estimate_addressable_market(10_000, 0.20, [])
        assert result["income_qualified_base"] == int(10_000 * 0.105)
        assert result["catholic_boost_contribution"] > 0

    def test_returns_all_expected_keys(self):
        dist = _make_income_dist(_WEALTHY_SUBURB_SHARES, 10_000)
        result = _estimate_addressable_market(5_000, 0.20, dist)
        expected_keys = {
            "income_qualified_base", "catholic_boost_contribution",
            "catholic_boost_rate", "choice_tier", "choice_income_shift",
            "total_addressable_market",
        }
        assert set(result.keys()) == expected_keys

    def test_total_is_sum_of_base_and_boost(self):
        dist = _make_income_dist(_WEALTHY_SUBURB_SHARES, 15_000)
        result = _estimate_addressable_market(10_000, 0.25, dist)
        assert result["total_addressable_market"] == (
            result["income_qualified_base"] + result["catholic_boost_contribution"]
        )

    def test_higher_income_produces_larger_base(self):
        """Wealthy area should have higher income-qualified base than poor area."""
        wealthy_dist = _make_income_dist(_WEALTHY_SUBURB_SHARES, 10_000)
        poor_dist = _make_income_dist(_RURAL_LOW_INCOME_SHARES, 10_000)
        wealthy = _estimate_addressable_market(5_000, 0.20, wealthy_dist)
        poor = _estimate_addressable_market(5_000, 0.20, poor_dist)
        assert wealthy["income_qualified_base"] > poor["income_qualified_base"]
        # Catholic boost is the same (same pop × same catholic_pct)
        assert wealthy["catholic_boost_contribution"] == poor["catholic_boost_contribution"]

    def test_higher_catholic_pct_produces_larger_boost(self):
        """Higher Catholic % should produce larger Catholic boost."""
        dist = _make_income_dist(_WEALTHY_SUBURB_SHARES, 10_000)
        high_cath = _estimate_addressable_market(5_000, 0.35, dist)
        low_cath = _estimate_addressable_market(5_000, 0.10, dist)
        assert high_cath["catholic_boost_contribution"] > low_cath["catholic_boost_contribution"]
        # Income base should be the same (same income distribution)
        assert high_cath["income_qualified_base"] == low_cath["income_qualified_base"]

    def test_school_choice_strong_state_boosts_market(self):
        """Strong choice state (FL) should produce larger addressable market."""
        dist = _make_income_dist(_MODERATE_INCOME_SHARES, 10_000)
        choice = _estimate_addressable_market(5_000, 0.20, dist, state_abbr="FL")
        no_choice = _estimate_addressable_market(5_000, 0.20, dist, state_abbr="AL")
        assert choice["choice_tier"] == "strong"
        assert no_choice["choice_tier"] == "none"
        assert choice["income_qualified_base"] > no_choice["income_qualified_base"]
        assert choice["choice_income_shift"] == 22_500

    def test_school_choice_established_state(self):
        """Established choice state gets moderate shift."""
        dist = _make_income_dist(_MODERATE_INCOME_SHARES, 5_000)
        result = _estimate_addressable_market(3_000, 0.15, dist, state_abbr="PA")
        assert result["choice_tier"] == "established"
        assert result["choice_income_shift"] == 10_000

    def test_catholic_boost_rate_is_seven_pct(self):
        dist = _make_income_dist(_WEALTHY_SUBURB_SHARES, 5_000)
        result = _estimate_addressable_market(5_000, 0.20, dist)
        assert result["catholic_boost_rate"] == pytest.approx(0.07)

    # -----------------------------------------------------------------------
    # Mental test case calibration anchors
    # -----------------------------------------------------------------------

    def test_wealthy_chicago_suburb_scores_high(self):
        """Wealthy Chicago suburb: 30k HH, 20k eff_pop, 32% Catholic, high income.
        Coed K-12: ref_enrollment=400. Should score 85+ (before trend)."""
        dist = _make_income_dist(_WEALTHY_SUBURB_SHARES, 30_000)
        result = _estimate_addressable_market(20_000, 0.32, dist, state_abbr="IL")
        tam = result["total_addressable_market"]
        assert result["income_qualified_base"] > 2_000
        assert result["catholic_boost_contribution"] > 300
        ratio = tam / 400  # coed K-12 reference
        score = _score_market_size(ratio, "suburban")
        # ratio ~10 → score ~91 before trend; +8 Growing = 99 ✓
        assert score >= 85

    def test_rural_alabama_scores_low(self):
        """Rural Alabama: 5k HH, 3.5k eff_pop, 10% Catholic, low income.
        Coed K-12: ref=400. ratio ~0.96 → score ~35 + 4 rural = ~39. -10 trend = ~29."""
        dist = _make_income_dist(_RURAL_LOW_INCOME_SHARES, 5_000)
        result = _estimate_addressable_market(3_500, 0.10, dist, state_abbr="AL")
        tam = result["total_addressable_market"]
        assert result["income_qualified_base"] < 500
        assert result["catholic_boost_contribution"] < 50
        ratio = tam / 400  # coed K-12 reference
        score = _score_market_size(ratio, "rural")
        # Before declining trend: 30-45ish. After -10: 20-35.
        assert 20 < score < 50

    def test_affluent_fairfield_ct_scores_moderately(self):
        """Affluent Fairfield County CT: very high income, 35% Catholic.
        Coed K-12: ref=400. ratio ~9.8 → score ~90. -10 declining = 80. ✓"""
        dist = _make_income_dist(_VERY_WEALTHY_SHARES, 25_000)
        result = _estimate_addressable_market(15_000, 0.35, dist, state_abbr="CT")
        tam = result["total_addressable_market"]
        assert result["income_qualified_base"] > 2_500
        ratio = tam / 400
        score = _score_market_size(ratio, "suburban")
        # Before trend: ~90. After Declining trend (-10): ~80. ✓
        assert score >= 80

    def test_working_class_catholic_boston_urban(self):
        """Working-class South Boston: moderate income, 40% Catholic, urban.
        Coed K-12: ref=400. ratio ~3.4 → score ~74. -4 urban adj = ~70."""
        dist = _make_income_dist(_MODERATE_INCOME_SHARES, 15_000)
        result = _estimate_addressable_market(8_000, 0.40, dist, state_abbr="MA")
        tam = result["total_addressable_market"]
        assert result["catholic_boost_contribution"] > 150
        ratio = tam / 400
        score = _score_market_size(ratio, "urban")
        assert 60 < score < 80

    def test_affluent_philly_girls_hs_scores_well(self):
        """Girls HS in affluent Philly suburb: ~629 addressable / 225 ref = 2.8.
        Should score 60-75 — NOT the broken 27 from absolute scoring."""
        ratio = 629 / 225  # ~2.8
        score = _score_market_size(ratio, "suburban")
        assert 60 <= score <= 75


# ---------------------------------------------------------------------------
# modules.housing._score_housing
# ---------------------------------------------------------------------------

class TestScoreHousing:
    BASE_DEMO = {
        "cost_burdened_renter_households": 1500,
        "median_household_income": 45_000,
        "renter_households": 4000,
    }

    def test_returns_all_keys(self):
        result = _score_housing(self.BASE_DEMO, [])
        for key in ("overall", "market_size", "income", "competition", "family_density"):
            assert key in result

    def test_overall_in_range(self):
        result = _score_housing(self.BASE_DEMO, [])
        assert 0 <= result["overall"] <= 100

    def test_zero_cost_burdened_lowers_market_size(self):
        demo_low = {**self.BASE_DEMO, "cost_burdened_renter_households": 0}
        score_low = _score_housing(demo_low, [])["market_size"]
        score_high = _score_housing(self.BASE_DEMO, [])["market_size"]
        assert score_low < score_high

    def test_many_nearby_projects_lowers_competition(self):
        projects = [{"distance_miles": 0.5, "li_units": 200} for _ in range(10)]
        score_saturated = _score_housing(self.BASE_DEMO, projects)["competition"]
        score_empty = _score_housing(self.BASE_DEMO, [])["competition"]
        assert score_saturated < score_empty


# ---------------------------------------------------------------------------
# modules.elder_care._score_elder_care
# ---------------------------------------------------------------------------



class TestElderCareFacilityBeds:
    def test_reads_certified_beds_first(self):
        assert _facility_beds({"certified_beds": 120, "licensed_beds": 90}) == 120

    def test_falls_back_to_licensed_or_generic_beds(self):
        assert _facility_beds({"licensed_beds": "88"}) == 88
        assert _facility_beds({"beds": "1,024"}) == 1024

    def test_invalid_values_default_to_zero(self):
        assert _facility_beds({"licensed_beds": "unknown"}) == 0

    def test_nan_values_default_to_zero(self):
        assert _facility_beds({"certified_beds": float("nan")}) == 0
        assert _facility_beds({"certified_beds": float("nan"), "licensed_beds": 75}) == 75

    def test_skips_nan_certified_and_reads_licensed(self):
        assert _facility_beds({"certified_beds": float("nan"), "licensed_beds": "60"}) == 60

class TestScoreElderCare:
    BASE_DEMO = {
        "seniors_75_plus": 1200,
        "seniors_living_alone": 400,
        "seniors_below_200pct_poverty": 300,
        "seniors_65_plus": 2000,
        "median_household_income": 55_000,
    }

    def test_returns_all_keys(self):
        result = _score_elder_care(self.BASE_DEMO, [], mission_mode=False)
        for key in ("overall", "market_size", "income", "competition", "family_density", "occupancy"):
            assert key in result

    def test_overall_in_range(self):
        result = _score_elder_care(self.BASE_DEMO, [], mission_mode=False)
        assert 0 <= result["overall"] <= 100

    def test_mission_mode_uses_vulnerable_senior_population(self):
        market_result = _score_elder_care(self.BASE_DEMO, [], mission_mode=False)
        mission_result = _score_elder_care(self.BASE_DEMO, [], mission_mode=True)
        # Mission mode target = alone*0.5 + poverty*0.5 = 200+150 = 350
        # Market mode target = seniors_75_plus = 1200
        assert mission_result["target_pop"] == pytest.approx(350.0)
        assert market_result["target_pop"] == pytest.approx(1200.0)

    def test_mission_mode_lower_income_scores_higher(self):
        low_income_demo = {**self.BASE_DEMO, "median_household_income": 25_000}
        mission_score = _score_elder_care(low_income_demo, [], mission_mode=True)["income"]
        market_score = _score_elder_care(low_income_demo, [], mission_mode=False)["income"]
        # Mission mode: low income = high need = high score
        assert mission_score > market_score

    def test_high_bed_saturation_lowers_competition(self):
        target_pop = self.BASE_DEMO["seniors_75_plus"]
        many_beds = [{"distance_miles": 0.5, "certified_beds": 400} for _ in range(5)]
        sat_score = _score_elder_care(self.BASE_DEMO, many_beds, mission_mode=False)["competition"]
        empty_score = _score_elder_care(self.BASE_DEMO, [], mission_mode=False)["competition"]
        assert sat_score < empty_score

    def test_high_occupancy_scores_above_85(self):
        facilities = [{"distance_miles": 1.0, "certified_beds": 100, "occupancy_pct": 90} for _ in range(3)]
        result = _score_elder_care(self.BASE_DEMO, facilities, mission_mode=False)
        assert result["occupancy"] > 85

    def test_missing_occupancy_data_scores_neutral_50(self):
        facilities = [{"distance_miles": 1.0, "certified_beds": 100, "occupancy_pct": None}]
        result = _score_elder_care(self.BASE_DEMO, facilities, mission_mode=False)
        assert result["occupancy"] == 50
        assert result["weighted_avg_occupancy_pct"] is None

    def test_occupancy_uses_decay_weighting(self):
        facilities = [
            {"distance_miles": 0.5, "certified_beds": 100, "occupancy_pct": 95},
            {"distance_miles": 20.0, "certified_beds": 100, "occupancy_pct": 55},
        ]
        result = _score_elder_care(self.BASE_DEMO, facilities, mission_mode=False)
        assert result["weighted_avg_occupancy_pct"] is not None
        assert result["weighted_avg_occupancy_pct"] > 80


# ---------------------------------------------------------------------------
# analysis._score_private_enrollment_rate
# ---------------------------------------------------------------------------

class TestScorePrivateEnrollmentRate:
    """Private school enrollment rate scoring (Census B14002)."""

    def test_no_data_returns_neutral(self):
        score, rate = _score_private_enrollment_rate(0, 0)
        assert score == pytest.approx(50.0)
        assert rate is None

    def test_national_average_scores_around_50(self):
        # 10.5% private enrollment rate = national average
        score, rate = _score_private_enrollment_rate(105, 1000)
        assert rate == pytest.approx(10.5)
        assert 45 <= score <= 55

    def test_high_private_rate_scores_high(self):
        # 25% private enrollment rate — very strong private school demand
        score, rate = _score_private_enrollment_rate(250, 1000)
        assert rate == pytest.approx(25.0)
        assert score > 85

    def test_low_private_rate_scores_low(self):
        # 3% private enrollment rate — very weak private school demand
        score, rate = _score_private_enrollment_rate(30, 1000)
        assert rate == pytest.approx(3.0)
        assert score < 20

    def test_monotone_increasing(self):
        rates = [0, 30, 60, 105, 150, 200, 300]
        scores = [_score_private_enrollment_rate(r, 1000)[0] for r in rates]
        assert scores == sorted(scores)

    def test_score_bounded_0_100(self):
        score_low, _ = _score_private_enrollment_rate(0, 1000)
        score_high, _ = _score_private_enrollment_rate(500, 1000)
        assert 0 <= score_low <= 100
        assert 0 <= score_high <= 100


# ---------------------------------------------------------------------------
# analysis._score_pipeline_ratio
# ---------------------------------------------------------------------------

class TestScorePipelineRatio:
    """Kindergarten pipeline scoring (under-5 / school-age ratio)."""

    def test_no_data_returns_neutral(self):
        score, ratio = _score_pipeline_ratio(0, 0)
        assert score == pytest.approx(50.0)
        assert ratio is None

    def test_national_average_scores_around_50(self):
        # ~0.33 ratio is roughly national average (5 birth years / 13 school years)
        score, ratio = _score_pipeline_ratio(330, 1000)
        assert ratio == pytest.approx(0.33, rel=0.05)
        assert 48 <= score <= 58

    def test_strong_pipeline_scores_high(self):
        # 0.5 ratio — strong incoming cohorts
        score, ratio = _score_pipeline_ratio(500, 1000)
        assert ratio == pytest.approx(0.5)
        assert score > 80

    def test_weak_pipeline_scores_low(self):
        # 0.15 ratio — weak pipeline, enrollment cliff risk
        score, ratio = _score_pipeline_ratio(150, 1000)
        assert ratio == pytest.approx(0.15)
        assert score < 25

    def test_monotone_increasing(self):
        under_5_values = [0, 100, 200, 300, 400, 500, 600]
        scores = [_score_pipeline_ratio(u, 1000)[0] for u in under_5_values]
        assert scores == sorted(scores)

    def test_score_bounded_0_100(self):
        score_low, _ = _score_pipeline_ratio(0, 1000)
        score_high, _ = _score_pipeline_ratio(800, 1000)
        assert 0 <= score_low <= 100
        assert 0 <= score_high <= 100


# ---------------------------------------------------------------------------
# bls_workforce.score_workforce_availability
# ---------------------------------------------------------------------------

class TestScoreWorkforceAvailability:
    """Workforce availability index for elder care."""

    def test_no_data_returns_neutral(self):
        score, details = score_workforce_availability(None, 5000)
        assert score == pytest.approx(50.0)
        assert details["available"] is False

    def test_zero_seniors_returns_neutral(self):
        score, details = score_workforce_availability({"elder_care_employment": 100}, 0)
        assert score == pytest.approx(50.0)
        assert details["available"] is False

    def test_strong_workforce_scores_high(self):
        qcew = {
            "elder_care_employment": 3000,
            "elder_care_establishments": 50,
            "elder_care_avg_weekly_wage": 800,
            "total_private_employment": 40000,
            "naics_details": [],
        }
        score, details = score_workforce_availability(qcew, 5000)
        assert details["available"] is True
        # 3000 / 5000 * 1000 = 600 workers per 1k seniors → very strong
        assert score > 70
        assert details["workers_per_1k_seniors"] == pytest.approx(600.0)

    def test_weak_workforce_scores_low(self):
        qcew = {
            "elder_care_employment": 25,
            "elder_care_establishments": 2,
            "elder_care_avg_weekly_wage": 500,
            "total_private_employment": 5000,
            "naics_details": [],
        }
        score, details = score_workforce_availability(qcew, 10000)
        assert details["available"] is True
        # 25 / 10000 * 1000 = 2.5 workers per 1k seniors → very weak
        assert score < 30

    def test_returns_all_expected_keys(self):
        qcew = {
            "elder_care_employment": 500,
            "elder_care_establishments": 15,
            "elder_care_avg_weekly_wage": 700,
            "total_private_employment": 20000,
            "naics_details": [],
        }
        score, details = score_workforce_availability(qcew, 3000)
        assert "workforce_score" in details
        assert "workers_per_1k_seniors" in details
        assert "location_quotient" in details
        assert "establishment_density_score" in details
        assert "note" in details
