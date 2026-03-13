"""Comprehensive feasibility scoring validation/audit suite.

This module is intentionally broad: it validates assumptions, breakpoints,
constants, and scoring behavior so regressions fail loudly with context.
Run with:
    pytest backend/tests/test_feasibility_audit.py -v
"""

import inspect
import math
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api import analysis
from api.analysis import (
    CATHOLIC_PCT_BY_STATE,
    NATIONAL_CATHOLIC_PCT,
    _CATHOLIC_BOOST,
    _CONFIDENCE_BAND,
    _ESTABLISHED_CHOICE_STATES,
    _INCOME_PROPENSITY_SEGMENTS,
    _REFERENCE_ENROLLMENT,
    _STRONG_CHOICE_STATES,
    _WEIGHTING_PROFILES,
    _adjusted_catholic_pct,
    _choice_state_tier,
    _compute_stage1_scores,
    _estimate_addressable_market,
    _scenario_scores,
    _school_choice_bonus,
    _score_competition,
    _score_family_density,
    _score_income,
    _score_market_size,
)
from api.census import ACS_VARIABLES, INFLATION_ADJ_2017_TO_2022, _ACS_2017_TREND_VARS, compute_trend
from api.schools import (
    GENDER_FILTER,
    GRADE_LEVEL_FILTER,
    TIER_DIRECT_WEIGHT,
    TIER_MODERATE_WEIGHT,
    TIER_STRONG_WEIGHT,
    TIER_WEAK_WEIGHT,
    TYPOLOGY_EXCLUDE,
    TYPOLOGY_SPECIAL_EMPHASIS,
    _is_excluded_by_name,
)
from modules.elder_care import (
    MARKET_WEIGHTS,
    MISSION_WEIGHTS,
    SURVIVAL_RATE_65_TO_74,
    SURVIVAL_RATE_75_PLUS,
    _facility_beds,
    _score_elder_care,
)
from modules.housing import HOUSING_WEIGHTS, _score_housing
from tests import test_competitor_tiering
from utils import decay_weight, piecewise_linear


ALL_SEGMENTS = {
    # analysis.py
    "_MARKET_DEPTH_RATIO_SEGMENTS": analysis._MARKET_DEPTH_RATIO_SEGMENTS,
    "_INCOME_SEGMENTS": analysis._INCOME_SEGMENTS,
    "_HIGH_INCOME_BONUS_SEGMENTS": analysis._HIGH_INCOME_BONUS_SEGMENTS,
    "_FAMILY_DENSITY_SEGMENTS": analysis._FAMILY_DENSITY_SEGMENTS,
    "_VALIDATION_WEIGHT_SEGMENTS": analysis._VALIDATION_WEIGHT_SEGMENTS,
    "_UNSERVED_MARKET_SEGMENTS": analysis._UNSERVED_MARKET_SEGMENTS,
    "_SATURATION_RATIO_SEGMENTS": analysis._SATURATION_RATIO_SEGMENTS,
    "_INCOME_PROPENSITY_SEGMENTS": analysis._INCOME_PROPENSITY_SEGMENTS,
    "_S2_TUITION_RATIO_SEGMENTS": analysis._S2_TUITION_RATIO_SEGMENTS,
    "_S2_OPEX_PER_STUDENT_SEGMENTS": analysis._S2_OPEX_PER_STUDENT_SEGMENTS,
    "_S2_OPERATION_GAP_SEGMENTS": analysis._S2_OPERATION_GAP_SEGMENTS,
    "_S2_FUNDRAISING_RATIO_SEGMENTS": analysis._S2_FUNDRAISING_RATIO_SEGMENTS,
    "_S2_PAYROLL_PCT_SEGMENTS": analysis._S2_PAYROLL_PCT_SEGMENTS,
    "_S2_SURPLUS_DEFICIT_SEGMENTS": analysis._S2_SURPLUS_DEFICIT_SEGMENTS,
    "_S2_INVESTMENT_TRANSFERS_SEGMENTS": analysis._S2_INVESTMENT_TRANSFERS_SEGMENTS,
    "_S2_ONE_TIME_INCOME_SEGMENTS": analysis._S2_ONE_TIME_INCOME_SEGMENTS,
    "_S2_PAYROLL_PCT_SEGMENTS_K5": analysis._S2_PAYROLL_PCT_SEGMENTS_K5,
    "_S2_PAYROLL_PCT_SEGMENTS_HS": analysis._S2_PAYROLL_PCT_SEGMENTS_HS,
    "_S2_TUITION_RATIO_SEGMENTS_K5": analysis._S2_TUITION_RATIO_SEGMENTS_K5,
    "_S2_TUITION_RATIO_SEGMENTS_HS": analysis._S2_TUITION_RATIO_SEGMENTS_HS,
    # housing.py (inline segments represented explicitly for audit checks)
    "housing_market_size": [(0, 8), (250, 28), (750, 52), (1500, 72), (3000, 88), (6000, 97)],
    "housing_income_inverted": [(25_000, 98), (40_000, 85), (60_000, 65), (80_000, 45), (100_000, 28), (130_000, 12)],
    "housing_competition": [(0.0, 96), (0.2, 80), (0.4, 62), (0.8, 40), (1.0, 28), (1.4, 12)],
    "housing_family_density": [(0, 10), (10, 30), (20, 55), (30, 75), (45, 92)],
    # elder_care.py (inline segments represented explicitly for audit checks)
    "elder_market_size": [(0, 8), (250, 32), (600, 52), (1200, 74), (2500, 91), (5000, 98)],
    "elder_income_mission_inverted": [(20_000, 97), (28_000, 88), (35_000, 70), (50_000, 45), (80_000, 20), (120_000, 10)],
    "elder_income_market": [(20_000, 10), (35_000, 24), (54_000, 52), (75_000, 74), (100_000, 90), (140_000, 97)],
    "elder_competition": [(0.0, 96), (0.2, 85), (0.4, 66), (0.8, 42), (1.0, 30), (1.4, 14)],
    "elder_family_density": [(0, 20), (8, 35), (15, 55), (25, 76), (35, 90), (50, 97)],
    "elder_occupancy": [(50, 15), (65, 35), (75, 55), (82, 70), (88, 83), (93, 92), (97, 98)],
}


class TestWeightingProfiles:
    def test_all_weight_profiles_sum_to_one(self):
        """Weights must sum to 1.0 or the blended score drifts above/below 100."""
        for name, profile in _WEIGHTING_PROFILES.items():
            assert sum(profile.values()) == pytest.approx(1.0), name

    def test_housing_weights_sum_to_one(self):
        """Housing weighting totals must be normalized for stable score composition."""
        assert sum(HOUSING_WEIGHTS.values()) == pytest.approx(1.0)

    def test_elder_mode_weights_sum_to_one(self):
        """Both elder-care modes must remain normalized to keep scores comparable."""
        assert sum(MISSION_WEIGHTS.values()) == pytest.approx(1.0)
        assert sum(MARKET_WEIGHTS.values()) == pytest.approx(1.0)


class TestSegmentValidity:
    @pytest.mark.parametrize("name,segments", ALL_SEGMENTS.items())
    def test_segment_shapes_are_valid(self, name, segments):
        """Segments must be ordered, unique, and bounded so interpolation is deterministic and safe."""
        xs = [x for x, _ in segments]
        ys = [y for _, y in segments]
        assert len(segments) >= 2, name
        assert xs == sorted(xs), name
        assert len(xs) == len(set(xs)), name
        assert all(0 <= y <= 100 for y in ys), name


class TestScoringBoundaries:
    @pytest.mark.parametrize("context", ["urban", "suburban", "rural"])
    def test_market_size_bounds_and_direction(self, context):
        """Market-size score must stay in range and increase as market depth improves."""
        low = _score_market_size(0, context)
        mid = _score_market_size(5, context)
        high = _score_market_size(50, context)
        assert 0 <= low <= 100
        assert 0 <= high <= 100
        assert low <= mid <= high

    def test_income_bounds_and_direction(self):
        """Income score should grow with income/affluence and clamp to 0-100."""
        low = _score_income(20_000, 0.0)
        high = _score_income(300_000, 1.0)
        unknown = _score_income(None, 0.0)
        assert 0 <= low <= 100
        assert 0 <= high <= 100
        assert high >= low
        assert unknown == 50.0

    def test_competition_boundaries(self):
        """Competition scoring must handle no schools and many schools without NaN/negative outputs."""
        none = _score_competition([], 5_000)
        one = _score_competition([{"is_catholic": True, "distance_miles": 1.0, "tier_weight": 1.0, "enrollment": 200, "lat": 40.0, "lon": -75.0}], 5_000)
        many = _score_competition(
            [{"is_catholic": i % 2 == 0, "distance_miles": 0.8 + i * 0.4, "tier_weight": 1.0 if i % 2 == 0 else 0.4, "enrollment": 200 + 10 * i, "lat": 40.0 + i * 0.01, "lon": -75.0 - i * 0.01} for i in range(20)],
            5_000,
        )
        for triple in (none, one, many):
            assert all(0 <= value <= 100 for value in triple)

    def test_family_density_edge_cases(self):
        """Family-density scoring should return neutral on unknown and valid values on edge denominators."""
        assert _score_family_density(0, 0) == 50.0
        assert 0 <= _score_family_density(100, 1000) <= 100

    def test_housing_and_elder_zero_and_extreme_inputs(self):
        """Housing/elder score engines must remain numerically stable on sparse/extreme inputs."""
        housing_zero = _score_housing({"cost_burdened_renter_households": 0, "median_household_income": 0, "renter_households": 0}, [])
        housing_extreme = _score_housing({"cost_burdened_renter_households": 10000, "median_household_income": 20_000, "renter_households": 12000}, [{"distance_miles": 1.0, "li_units": 200}] * 10)
        elder_zero = _score_elder_care({"median_household_income": 0}, [], True)
        elder_typical = _score_elder_care({"seniors_75_plus": 2000, "seniors_65_plus": 5000, "seniors_living_alone": 800, "seniors_below_200pct_poverty": 600, "median_household_income": 42_000}, [], True)
        for result in (housing_zero, housing_extreme, elder_zero, elder_typical):
            assert 0 <= result["overall"] <= 100


class TestCatholicPopulation:
    def test_state_coverage_and_bounds(self):
        """Catholic state map should include all 50 states + DC and plausible bounded percentages."""
        assert len(CATHOLIC_PCT_BY_STATE) == 51
        assert all(0.05 <= pct <= 0.50 for pct in CATHOLIC_PCT_BY_STATE.values())
        assert NATIONAL_CATHOLIC_PCT == 0.21

    def test_adjusted_catholic_pct_directionality_and_bounds(self):
        """Local private-school propensity must shift Catholic estimate in the expected direction and never overflow 0-1."""
        base = 0.21
        up, _ = _adjusted_catholic_pct(base, 210, 1000)
        same, _ = _adjusted_catholic_pct(base, 105, 1000)
        down, _ = _adjusted_catholic_pct(base, 50, 1000)
        assert up > base
        assert same == pytest.approx(base)
        assert down < base
        assert 0 <= up <= 1
        assert 0 <= down <= 1


class TestAddressableMarket:
    def test_fallback_with_empty_income_distribution(self):
        """Without bracket data, addressable market should use national flat rate fallback."""
        out = _estimate_addressable_market(2000, 0.2, [], "PA")
        assert out["income_qualified_base"] == int(2000 * analysis.NATIONAL_PRIVATE_SCHOOL_RATE)

    def test_higher_income_brackets_contribute_more(self):
        """Income-first model must produce more addressable families for wealthier distributions."""
        low = _estimate_addressable_market(1000, 0.2, [(25_000, 100)], "PA")
        high = _estimate_addressable_market(1000, 0.2, [(150_000, 100)], "PA")
        assert high["income_qualified_base"] > low["income_qualified_base"]

    def test_catholic_boost_is_additive(self):
        """Catholic contribution should be explicit additive uplift tied to catholic_pct."""
        low = _estimate_addressable_market(1000, 0.10, [(75_000, 100)], "PA")
        high = _estimate_addressable_market(1000, 0.30, [(75_000, 100)], "PA")
        assert high["catholic_boost_contribution"] > low["catholic_boost_contribution"]
        assert high["total_addressable_market"] > high["income_qualified_base"]

    def test_school_choice_income_shift_lookup(self):
        """Choice-state tier should shift a $50k family to the documented effective lookup income."""
        base = piecewise_linear(50_000, _INCOME_PROPENSITY_SEGMENTS)
        strong = piecewise_linear(72_500, _INCOME_PROPENSITY_SEGMENTS)
        established = piecewise_linear(60_000, _INCOME_PROPENSITY_SEGMENTS)
        out_strong = _estimate_addressable_market(1000, 0.2, [(50_000, 100)], "FL")
        out_est = _estimate_addressable_market(1000, 0.2, [(50_000, 100)], "PA")
        out_none = _estimate_addressable_market(1000, 0.2, [(50_000, 100)], "AL")
        assert out_strong["choice_income_shift"] == 22_500
        assert out_est["choice_income_shift"] == 10_000
        assert out_none["choice_income_shift"] == 0
        assert out_strong["income_qualified_base"] == int(1000 * (strong / 100.0))
        assert out_est["income_qualified_base"] == int(1000 * (established / 100.0))
        assert out_none["income_qualified_base"] == int(1000 * (base / 100.0))
        assert out_none["total_addressable_market"] > 0


class TestIncomePropensity:
    def test_income_propensity_monotonic_and_anchors(self):
        """Income propensity curve should be monotonic and match documented calibration anchors."""
        incomes = [0, 20_000, 35_000, 50_000, 75_000, 100_000, 150_000, 200_000, 250_000]
        props = [piecewise_linear(i, _INCOME_PROPENSITY_SEGMENTS) for i in incomes]
        assert props == sorted(props)
        assert piecewise_linear(0, _INCOME_PROPENSITY_SEGMENTS) == pytest.approx(3.0)
        assert piecewise_linear(100_000, _INCOME_PROPENSITY_SEGMENTS) == pytest.approx(18.0)
        assert piecewise_linear(200_000, _INCOME_PROPENSITY_SEGMENTS) == pytest.approx(35.0)
        assert (35 / 100) + _CATHOLIC_BOOST <= 0.42


class TestCompetitionScoring:
    def test_decay_weight_curve(self):
        """Distance decay must always be positive and strictly decreasing as distance rises."""
        dists = [0.5, 1, 5, 15, 30]
        vals = [decay_weight(d) for d in dists]
        assert all(v > 0 for v in vals)
        assert vals == sorted(vals, reverse=True)

    def test_competitor_tier_weight_ordering(self):
        """Tier weights encode competitive pressure assumptions and must preserve ordering."""
        assert TIER_DIRECT_WEIGHT > TIER_STRONG_WEIGHT > TIER_MODERATE_WEIGHT > TIER_WEAK_WEIGHT > 0

    def test_validation_saturation_split_and_unserved_market(self):
        """Combined competition score must preserve 60/40 split and unserved markets should be moderate, not extreme."""
        combined, val, sat = _score_competition([], 10_000)
        assert combined == pytest.approx(val * 0.6 + sat * 0.4)
        assert 20 <= combined <= 80


class TestTrendComputation:
    def test_prefers_county_trend_2022_when_present(self):
        """Trend must use county_trend_2022 to avoid tract-vs-county comparison bias."""
        data_2022 = {
            "population_5_to_11": 9999,
            "population_12_to_17": 9999,
            "median_household_income": 200_000,
            "families_with_children": 9999,
            "county_trend_2022": {"school_age_pop": 10_500, "median_income": 66_000, "families_with_children": 4_200},
        }
        data_2017 = {"school_age_pop": 10_000, "median_income": 60_000, "families_with_children": 4_000}
        out = compute_trend(data_2022, data_2017)
        assert out["school_age_pop_pct"] == 5.0
        assert out["families_pct"] == 5.0

    def test_fallback_uses_data_2022_fields(self):
        """When county trend payload is absent, direct 2022 demographic fields should drive trend."""
        out = compute_trend(
            {"population_5_to_11": 5300, "population_12_to_17": 4700, "median_family_income": 65_000, "families_with_children": 4_200},
            {"school_age_pop": 10_000, "median_income": 60_000, "families_with_children": 4_000},
        )
        assert out["school_age_pop_pct"] == 0.0
        assert out["families_pct"] == 5.0

    @pytest.mark.parametrize(
        "d2022,d2017,label",
        [
            ({"population_5_to_11": 6200, "population_12_to_17": 5300, "median_family_income": 80_000, "families_with_children": 4400}, {"school_age_pop": 10_000, "median_income": 60_000, "families_with_children": 4_000}, "Growing"),
            ({"population_5_to_11": 5000, "population_12_to_17": 5000, "median_family_income": 72_000, "families_with_children": 4050}, {"school_age_pop": 10_000, "median_income": 60_000, "families_with_children": 4000}, "Stable"),
            ({"population_5_to_11": 4200, "population_12_to_17": 4300, "median_family_income": 60_000, "families_with_children": 3400}, {"school_age_pop": 10_000, "median_income": 60_000, "families_with_children": 4000}, "Declining"),
            ({"population_5_to_11": 4200, "population_12_to_17": 4300, "median_family_income": 90_000, "families_with_children": 3800}, {"school_age_pop": 10_000, "median_income": 60_000, "families_with_children": 4000}, "Mixed"),
        ],
    )
    def test_trend_labels(self, d2022, d2017, label):
        """Trend labels drive score adjustments and must map predictably from signals."""
        assert compute_trend(d2022, d2017)["trend_label"] == label

    def test_inflation_adjustment_and_zero_population_edge(self):
        """Income real-change should subtract inflation and zero 2017 pop must not crash."""
        out = compute_trend(
            {"population_5_to_11": 1000, "population_12_to_17": 1000, "median_household_income": 65_000, "families_with_children": 1000},
            {"school_age_pop": 0, "median_income": 50_000, "families_with_children": 1000},
        )
        expected_real = round((((65_000 - 50_000) / 50_000) - INFLATION_ADJ_2017_TO_2022) * 100, 1)
        assert out["school_age_pop_pct"] is None
        assert out["income_real_pct"] == expected_real


class TestSchoolChoice:
    def test_choice_state_membership_and_disjointness(self):
        """Choice-state sets encode key affordability assumptions and must stay accurate/disjoint."""
        for state in {"FL", "OH", "IN", "OK", "IA", "AZ"}:
            assert state in _STRONG_CHOICE_STATES
        assert len(_ESTABLISHED_CHOICE_STATES) == 26
        assert _STRONG_CHOICE_STATES.isdisjoint(_ESTABLISHED_CHOICE_STATES)

    def test_choice_bonus_and_tier(self):
        """Choice tier outputs must map to published bonus constants for explainability."""
        assert _school_choice_bonus("FL") == 12.0
        assert _school_choice_bonus("PA") == 5.0
        assert _school_choice_bonus("AL") == 0.0
        assert _choice_state_tier("FL") == "strong"
        assert _choice_state_tier("PA") == "established"
        assert _choice_state_tier("AL") == "none"


class TestScenarioBands:
    def test_band_constants_and_clamping(self):
        """Scenario band constants and clamping protect conservative/optimistic score sanity."""
        assert _CONFIDENCE_BAND == {"high": 6, "medium": 12, "low": 18}
        assert _scenario_scores(3, "low") == (0, 21)
        assert _scenario_scores(98, "high") == (92, 100)


class TestHousingAssumptions:
    def test_income_is_inverted_and_zero_cost_burden_is_low(self):
        """Housing mission needs lower income to score higher and empty need should suppress market-size score."""
        rich = _score_housing({"cost_burdened_renter_households": 1000, "median_household_income": 120_000, "renter_households": 5000}, [])
        poor = _score_housing({"cost_burdened_renter_households": 1000, "median_household_income": 30_000, "renter_households": 5000}, [])
        none_burdened = _score_housing({"cost_burdened_renter_households": 0, "median_household_income": 50_000, "renter_households": 5000}, [])
        assert poor["income"] > rich["income"]
        assert none_burdened["market_size"] <= 12

    def test_lihtc_saturation_uses_distance_decay(self):
        """Near LIHTC capacity should count more than far capacity due to shared decay formula."""
        near = _score_housing({"cost_burdened_renter_households": 1000, "median_household_income": 40_000, "renter_households": 4000}, [{"distance_miles": 1.0, "li_units": 100}])
        far = _score_housing({"cost_burdened_renter_households": 1000, "median_household_income": 40_000, "renter_households": 4000}, [{"distance_miles": 15.0, "li_units": 100}])
        assert near["weighted_units"] > far["weighted_units"]


class TestElderCareAssumptions:
    def test_survival_constants_and_projection_formula(self):
        """Senior projection constants/formulas must remain unchanged for planning horizon comparability."""
        assert SURVIVAL_RATE_65_TO_74 == pytest.approx(0.9798)
        assert SURVIVAL_RATE_75_PLUS == pytest.approx(0.9542)
        seniors_65_plus = 5000
        seniors_75_plus = 2000
        seniors_65_to_74 = seniors_65_plus - seniors_75_plus
        projected_5 = (seniors_65_to_74 * (SURVIVAL_RATE_65_TO_74**5)) + (seniors_75_plus * (SURVIVAL_RATE_75_PLUS**5))
        projected_10 = (seniors_65_to_74 * (SURVIVAL_RATE_65_TO_74**10)) + (seniors_75_plus * (SURVIVAL_RATE_75_PLUS**10))
        assert projected_10 < projected_5 < seniors_65_plus

    def test_mode_differences_income_direction_and_weights(self):
        """Mission/market modes must differ in target population, income direction, and weight profiles."""
        demographics = {
            "seniors_65_plus": 5000,
            "seniors_75_plus": 2000,
            "seniors_living_alone": 800,
            "seniors_below_200pct_poverty": 600,
            "median_household_income": 42_000,
        }
        mission = _score_elder_care(demographics, [], True)
        market = _score_elder_care(demographics, [], False)
        assert mission["target_pop"] == pytest.approx(700)
        assert market["target_pop"] == 2000
        assert MISSION_WEIGHTS != MARKET_WEIGHTS

    def test_income_flip_by_mode(self):
        """Mission mode should reward lower incomes while market mode rewards higher incomes."""
        low = {"seniors_75_plus": 1500, "seniors_65_plus": 3000, "seniors_living_alone": 800, "seniors_below_200pct_poverty": 600, "median_household_income": 35_000}
        high = {"seniors_75_plus": 1500, "seniors_65_plus": 3000, "seniors_living_alone": 300, "seniors_below_200pct_poverty": 150, "median_household_income": 95_000}
        assert _score_elder_care(low, [], True)["income"] > _score_elder_care(high, [], True)["income"]
        assert _score_elder_care(high, [], False)["income"] > _score_elder_care(low, [], False)["income"]

    @pytest.mark.parametrize("raw,expected", [("nan", 0), ("none", 0), ("", 0), (None, 0), ("120", 120), ("1,024", 1024)])
    def test_facility_beds_normalization(self, raw, expected):
        """Facility bed parsing must gracefully handle nullish strings and formatted numerics."""
        assert _facility_beds({"certified_beds": raw}) == expected

    def test_occupancy_high_means_unmet_demand_signal(self):
        """Higher occupancy should map to higher unmet-demand score than low occupancy."""
        base_demo = {"seniors_75_plus": 1500, "seniors_65_plus": 3000, "seniors_living_alone": 500, "seniors_below_200pct_poverty": 300, "median_household_income": 60_000}
        low_occ = _score_elder_care(base_demo, [{"distance_miles": 1.0, "certified_beds": 100, "occupancy_pct": 50}], False)
        high_occ = _score_elder_care(base_demo, [{"distance_miles": 1.0, "certified_beds": 100, "occupancy_pct": 95}], False)
        assert low_occ["occupancy"] < high_occ["occupancy"]


class TestACSVariables:
    def test_acs_variables_completeness_and_uniqueness(self):
        """ACS pull list must retain required fields with no duplicates to avoid silent data gaps."""
        expected = {
            "B01003_001E", "B09001_001E", "B09001_003E", "B09001_004E", "B09001_005E",
            "B19013_001E", "B19125_002E", "B11001_001E", "B11003_001E", "B25003_001E",
            "B19001_001E", "B19001_002E", "B19001_017E", "B01001_004E", "B01001_028E",
            "B14002_003E", "B14002_042E", "B11010_003E", "B17001_030E",
        }
        assert expected.issubset(set(ACS_VARIABLES))
        assert len(ACS_VARIABLES) == len(set(ACS_VARIABLES))

    def test_2017_trend_vars_are_complete(self):
        """Trend variable set should contain the expected 7 fields and be duplicate-free."""
        assert len(_ACS_2017_TREND_VARS) == 7
        for key in ["B01003_001E", "B09001_004E", "B09001_005E", "B19013_001E", "B19125_002E", "B19113_001E", "B11003_001E"]:
            assert key in _ACS_2017_TREND_VARS
        assert len(_ACS_2017_TREND_VARS) == len(set(_ACS_2017_TREND_VARS))


class TestPSSFiltering:
    def test_typology_and_filter_mappings(self):
        """PSS filter constants must preserve intended exclusion/inclusion semantics."""
        assert "9" in TYPOLOGY_EXCLUDE
        assert "8" in TYPOLOGY_SPECIAL_EMPHASIS
        assert not {"1", "2", "3"}.intersection(TYPOLOGY_EXCLUDE)
        for key, values in GRADE_LEVEL_FILTER.items():
            assert key in {"k5", "k8", "high_school", "k12"}
            assert values.issubset({"0", "1", "2", "3"})
        for key, values in GENDER_FILTER.items():
            assert key in {"boys", "girls", "coed"}
            assert values.issubset({"1", "2", "3"})

    def test_name_exclusion_extends_existing_suite(self):
        """Name-based exclusion should retain legacy behavior and catch additional known specialized names."""
        assert _is_excluded_by_name("Pathway School for Learning Disabilities")
        assert _is_excluded_by_name("Sunrise Juvenile Treatment Academy")
        assert not _is_excluded_by_name("Saint Joseph Academy")
        # Re-use existing module coverage as a dependency guard.
        assert callable(test_competitor_tiering._is_excluded_by_name)


class TestReferenceEnrollment:
    def test_reference_enrollment_completeness_and_ranges(self):
        """Reference enrollment constants anchor market-depth ratio calibration and must stay plausible."""
        assert set(_REFERENCE_ENROLLMENT.keys()) == {"k5", "k8", "high_school", "k12"}
        for _, (coed, gendered) in _REFERENCE_ENROLLMENT.items():
            assert 100 <= coed <= 500
            assert 100 <= gendered <= 500
            assert gendered < coed


class TestKnownAnswerSchools:
    def test_wealthy_suburban_market_scores_high(self):
        """Synthetic affluent suburban market should remain strong to guard regression in upside scoring."""
        location = {"lat": 40.0, "lon": -75.0, "state_fips": "42"}  # PA
        demographics = {
            "population_5_to_11": 5000,
            "population_12_to_17": 3000,
            "median_family_income": 105_000,
            "total_households": 20_000,
            "high_income_households": 9_000,
            "families_with_children": 12_000,
            "private_school_enrolled": 1800,
            "total_school_enrolled": 8000,
            "income_distribution": [(35_000, 1000), (75_000, 3000), (112_500, 6000), (175_000, 4000)],
            "historical_2017": {"school_age_pop": 7900, "median_income": 96_000, "families_with_children": 11_700},
            "county_trend_2022": {"school_age_pop": 8000, "median_income": 105_000, "families_with_children": 12_000},
        }
        schools = [
            {"is_catholic": True, "distance_miles": 2.0, "tier_weight": 1.0, "enrollment": 350, "lat": 40.01, "lon": -75.01},
            {"is_catholic": True, "distance_miles": 4.5, "tier_weight": 1.0, "enrollment": 300, "lat": 40.03, "lon": -75.03},
            {"is_catholic": True, "distance_miles": 9.0, "tier_weight": 1.0, "enrollment": 250, "lat": 40.07, "lon": -75.02},
        ] + [
            {"is_catholic": False, "distance_miles": 3 + i, "tier_weight": 0.4, "enrollment": 220, "lat": 40.02 + i * 0.01, "lon": -75.04 - i * 0.01}
            for i in range(8)
        ]
        out = _compute_stage1_scores(location, demographics, schools, "coed", "k12", "standard_baseline", "suburban")
        assert 75 <= out["overall"] <= 95
        assert out["ms"] >= 70
        assert out["inc"] >= 70
        assert 40 <= out["comp"] <= 85

    def test_rural_low_income_market_scores_low(self):
        """Synthetic rural low-income market should remain challenging to guard downside calibration."""
        location = {"lat": 32.0, "lon": -86.0, "state_fips": "01"}  # AL
        demographics = {
            "population_5_to_11": 550,
            "population_12_to_17": 350,
            "median_family_income": 38_000,
            "total_households": 3_000,
            "high_income_households": 150,
            "families_with_children": 1_500,
            "private_school_enrolled": 30,
            "total_school_enrolled": 900,
            "income_distribution": [(22_500, 900), (37_500, 1200), (55_000, 700), (87_500, 200)],
            "historical_2017": {"school_age_pop": 1200, "median_income": 39_000, "families_with_children": 1700},
            "county_trend_2022": {"school_age_pop": 900, "median_income": 38_000, "families_with_children": 1500},
        }
        schools = [{"is_catholic": False, "distance_miles": 8.0, "tier_weight": 0.4, "enrollment": 120, "lat": 32.04, "lon": -86.05}]
        out = _compute_stage1_scores(location, demographics, schools, "coed", "k12", "standard_baseline", "rural")
        assert 20 <= out["overall"] <= 40
        assert out["ms"] <= 45
        assert out["inc"] <= 50


class TestKnownAnswerHousing:
    def test_high_need_urban_area_scores_high(self):
        """Synthetic high-need housing market should score as strong opportunity."""
        demographics = {"cost_burdened_renter_households": 2500, "renter_households": 8000, "median_household_income": 35_000}
        projects = [
            {"distance_miles": 1.5, "li_units": 80},
            {"distance_miles": 2.5, "li_units": 70},
            {"distance_miles": 4.0, "li_units": 50},
        ]
        out = _score_housing(demographics, projects)
        assert 70 <= out["overall"] <= 90
        assert out["market_size"] >= 70
        assert out["income"] >= 80


class TestKnownAnswerElderCare:
    def test_mission_mode_vulnerable_population_scores_moderate_high(self):
        """Mission-mode vulnerable market should score moderate-high and reward lower incomes."""
        demographics = {
            "seniors_65_plus": 5000,
            "seniors_75_plus": 2000,
            "seniors_living_alone": 800,
            "seniors_below_200pct_poverty": 600,
            "median_household_income": 42_000,
        }
        facilities = [
            {"distance_miles": 1.0, "certified_beds": 120, "occupancy_pct": 88},
            {"distance_miles": 3.0, "certified_beds": 90, "occupancy_pct": 87},
            {"distance_miles": 5.0, "certified_beds": 70, "occupancy_pct": 89},
            {"distance_miles": 7.0, "certified_beds": 60, "occupancy_pct": 88},
            {"distance_miles": 9.5, "certified_beds": 60, "occupancy_pct": 86},
        ]
        out = _score_elder_care(demographics, facilities, True)
        assert 60 <= out["overall"] <= 80
        assert out["market_size"] >= 50
        assert out["income"] >= 40

    def test_market_mode_affluent_area_flips_income_signal(self):
        """Market mode in affluent senior market should score high and invert income-direction vs mission mode."""
        demographics = {
            "seniors_65_plus": 5000,
            "seniors_75_plus": 2000,
            "seniors_living_alone": 300,
            "seniors_below_200pct_poverty": 150,
            "median_household_income": 95_000,
        }
        facilities = [{"distance_miles": 2.0, "certified_beds": 100, "occupancy_pct": 90}] * 4
        market = _score_elder_care(demographics, facilities, False)
        mission = _score_elder_care(demographics, facilities, True)
        assert 65 <= market["overall"] <= 85
        assert market["income"] > mission["income"]


class TestCrossMinistryConsistency:
    def test_overall_scores_are_clamped(self):
        """All ministries should emit bounded overall scores in [0,100] under stress inputs."""
        school = _score_market_size(500, "urban")
        housing = _score_housing({"cost_burdened_renter_households": 20_000, "median_household_income": 10_000, "renter_households": 25_000}, [])
        elder = _score_elder_care({"seniors_75_plus": 50_000, "seniors_65_plus": 80_000, "seniors_living_alone": 20_000, "seniors_below_200pct_poverty": 20_000, "median_household_income": 10_000}, [], True)
        assert 0 <= school <= 100
        assert 0 <= housing["overall"] <= 100
        assert 0 <= elder["overall"] <= 100

    def test_piecewise_and_decay_used_consistently(self):
        """Distance weighting and interpolation primitives should be reused across ministries for consistency."""
        comp_src = inspect.getsource(_score_competition)
        house_src = inspect.getsource(_score_housing)
        elder_src = inspect.getsource(_score_elder_care)
        assert "decay_weight" in comp_src and "piecewise_linear" in comp_src
        assert "decay_weight" in house_src and "piecewise_linear" in house_src
        assert "decay_weight" in elder_src and "piecewise_linear" in elder_src

    def test_metric_like_components_have_valid_ranges(self):
        """Per-metric outputs used to build MetricScore objects must remain score-safe and positively weighted."""
        school_weights = _WEIGHTING_PROFILES["standard_baseline"]
        assert all(weight > 0 for weight in school_weights.values())
        housing = _score_housing({"cost_burdened_renter_households": 1000, "median_household_income": 45_000, "renter_households": 4000}, [])
        elder = _score_elder_care({"seniors_75_plus": 1800, "seniors_65_plus": 4000, "seniors_living_alone": 600, "seniors_below_200pct_poverty": 400, "median_household_income": 55_000}, [], False)
        for key in ("market_size", "income", "competition", "family_density"):
            assert 0 <= housing[key] <= 100
            assert 0 <= elder[key] <= 100
