"""
Unit tests for competitor filtering and tiering logic in api/schools.py.

Run with:  pytest backend/tests/test_competitor_tiering.py  (from repo root)
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from api.schools import (
    POEMPH_NAMES,
    POEMPH_EXCLUDE,
    TYPOLOGY_NAMES,
    TYPOLOGY_EXCLUDE,
    TYPOLOGY_SPECIAL_EMPHASIS,
    TIER_DIRECT_WEIGHT,
    TIER_STRONG_WEIGHT,
    TIER_MODERATE_WEIGHT,
    TIER_WEAK_WEIGHT,
    _RELIGIOUS_ORIENT_CODES,
    PSS_AFFILIATION_NAMES,
    _is_excluded_by_name,
    _EXCLUDE_NAME_KEYWORDS,
    _EXCLUDE_SCHOOL_NAMES,
)


# ---------------------------------------------------------------------------
# Tier weight constants
# ---------------------------------------------------------------------------

class TestTierWeightConstants:
    def test_direct_is_highest(self):
        assert TIER_DIRECT_WEIGHT == 1.0

    def test_strong_less_than_direct(self):
        assert TIER_STRONG_WEIGHT < TIER_DIRECT_WEIGHT

    def test_moderate_less_than_strong(self):
        assert TIER_MODERATE_WEIGHT < TIER_STRONG_WEIGHT

    def test_weak_less_than_moderate(self):
        assert TIER_WEAK_WEIGHT < TIER_MODERATE_WEIGHT

    def test_weak_is_positive(self):
        assert TIER_WEAK_WEIGHT > 0

    def test_expected_values(self):
        assert TIER_DIRECT_WEIGHT == 1.0
        assert TIER_STRONG_WEIGHT == 0.7
        assert TIER_MODERATE_WEIGHT == 0.4
        assert TIER_WEAK_WEIGHT == 0.15


# ---------------------------------------------------------------------------
# POEMPH filtering constants
# ---------------------------------------------------------------------------

class TestPoemphFiltering:
    def test_special_ed_is_excluded(self):
        assert "3" in POEMPH_EXCLUDE

    def test_regular_not_excluded(self):
        assert "1" not in POEMPH_EXCLUDE

    def test_special_emphasis_not_excluded(self):
        assert "2" not in POEMPH_EXCLUDE

    def test_poemph_names_complete(self):
        assert POEMPH_NAMES["1"] == "Regular"
        assert POEMPH_NAMES["2"] == "Special Emphasis"
        assert POEMPH_NAMES["3"] == "Special Education"


# ---------------------------------------------------------------------------
# TYPOLOGY filtering constants (replaces POEMPH in PSS 2021-22)
# ---------------------------------------------------------------------------

class TestTypologyFiltering:
    def test_special_ed_excluded(self):
        assert "9" in TYPOLOGY_EXCLUDE

    def test_special_emphasis_not_excluded(self):
        """Special emphasis is tiered as 'weak', not excluded."""
        assert "8" not in TYPOLOGY_EXCLUDE

    def test_special_emphasis_identified(self):
        assert "8" in TYPOLOGY_SPECIAL_EMPHASIS

    def test_catholic_types_not_excluded(self):
        assert "1" not in TYPOLOGY_EXCLUDE
        assert "2" not in TYPOLOGY_EXCLUDE
        assert "3" not in TYPOLOGY_EXCLUDE

    def test_other_religious_not_excluded(self):
        assert "4" not in TYPOLOGY_EXCLUDE
        assert "5" not in TYPOLOGY_EXCLUDE
        assert "6" not in TYPOLOGY_EXCLUDE

    def test_nonsectarian_regular_not_excluded(self):
        assert "7" not in TYPOLOGY_EXCLUDE

    def test_all_nine_codes_present(self):
        assert len(TYPOLOGY_NAMES) == 9
        for i in range(1, 10):
            assert str(i) in TYPOLOGY_NAMES


# ---------------------------------------------------------------------------
# Religious orientation classification
# ---------------------------------------------------------------------------

class TestReligiousOrientCodes:
    def test_catholic_not_in_religious_set(self):
        """Catholic (1) gets its own 'direct' tier, not 'strong'."""
        assert "1" not in _RELIGIOUS_ORIENT_CODES

    def test_nonsectarian_not_in_religious_set(self):
        """Nonsectarian (30) gets 'moderate' tier, not 'strong'."""
        assert "30" not in _RELIGIOUS_ORIENT_CODES

    def test_christian_nondenom_is_religious(self):
        assert "8" in _RELIGIOUS_ORIENT_CODES

    def test_episcopal_is_religious(self):
        assert "14" in _RELIGIOUS_ORIENT_CODES

    def test_lutheran_is_religious(self):
        assert "20" in _RELIGIOUS_ORIENT_CODES
        assert "21" in _RELIGIOUS_ORIENT_CODES

    def test_baptist_is_religious(self):
        assert "5" in _RELIGIOUS_ORIENT_CODES

    def test_jewish_is_religious(self):
        assert "18" in _RELIGIOUS_ORIENT_CODES

    def test_islamic_is_religious(self):
        assert "17" in _RELIGIOUS_ORIENT_CODES

    def test_all_affiliation_codes_classified(self):
        """Every affiliation code in PSS_AFFILIATION_NAMES should be either
        Catholic (1), Nonsectarian (30), or in _RELIGIOUS_ORIENT_CODES."""
        for code in PSS_AFFILIATION_NAMES:
            if code == "1":
                continue  # Catholic → direct
            if code == "30":
                continue  # Nonsectarian → moderate
            assert code in _RELIGIOUS_ORIENT_CODES, (
                f"Affiliation code {code} ({PSS_AFFILIATION_NAMES[code]}) "
                f"is not classified in _RELIGIOUS_ORIENT_CODES"
            )


# ---------------------------------------------------------------------------
# Tiering logic simulation (mirrors the logic in get_nearby_schools)
# ---------------------------------------------------------------------------

def _classify_tier(orient_code: str, poemph_code: str):
    """Simulate the tiering logic from get_nearby_schools."""
    is_catholic = orient_code == "1"
    is_special_emphasis = poemph_code == "2"

    if is_special_emphasis:
        return "weak", TIER_WEAK_WEIGHT
    elif is_catholic:
        return "direct", TIER_DIRECT_WEIGHT
    elif orient_code in _RELIGIOUS_ORIENT_CODES:
        return "strong", TIER_STRONG_WEIGHT
    else:
        return "moderate", TIER_MODERATE_WEIGHT


class TestTieringClassification:
    def test_catholic_regular_is_direct(self):
        tier, weight = _classify_tier("1", "1")
        assert tier == "direct"
        assert weight == 1.0

    def test_catholic_special_emphasis_is_weak(self):
        """Even a Catholic school with special emphasis gets 'weak' tier."""
        tier, weight = _classify_tier("1", "2")
        assert tier == "weak"
        assert weight == 0.15

    def test_baptist_regular_is_strong(self):
        tier, weight = _classify_tier("5", "1")
        assert tier == "strong"
        assert weight == 0.7

    def test_nonsectarian_regular_is_moderate(self):
        tier, weight = _classify_tier("30", "1")
        assert tier == "moderate"
        assert weight == 0.4

    def test_episcopal_special_emphasis_is_weak(self):
        tier, weight = _classify_tier("14", "2")
        assert tier == "weak"
        assert weight == 0.15

    def test_nonsectarian_special_emphasis_is_weak(self):
        tier, weight = _classify_tier("30", "2")
        assert tier == "weak"
        assert weight == 0.15

    def test_unknown_poemph_defaults_to_orient_based_tier(self):
        """When POEMPH is missing (-1), tier is based on orientation only."""
        tier, weight = _classify_tier("1", "-1")
        assert tier == "direct"
        tier, weight = _classify_tier("5", "-1")
        assert tier == "strong"
        tier, weight = _classify_tier("30", "-1")
        assert tier == "moderate"


# ---------------------------------------------------------------------------
# Schema integration
# ---------------------------------------------------------------------------

class TestCompetitorSchoolSchema:
    def test_schema_has_tier_fields(self):
        from models.schemas import CompetitorSchool
        school = CompetitorSchool(
            name="Test School",
            lat=41.0,
            lon=-87.0,
            distance_miles=1.0,
            affiliation="Roman Catholic",
            is_catholic=True,
            competitor_tier="direct",
            tier_weight=1.0,
        )
        assert school.competitor_tier == "direct"
        assert school.tier_weight == 1.0

    def test_schema_defaults(self):
        from models.schemas import CompetitorSchool
        school = CompetitorSchool(
            name="Test School",
            lat=41.0,
            lon=-87.0,
            distance_miles=1.0,
            affiliation="Nonsectarian",
            is_catholic=False,
        )
        assert school.competitor_tier == "moderate"
        assert school.tier_weight == 0.4


# ---------------------------------------------------------------------------
# Name-based exclusion filter
# ---------------------------------------------------------------------------

class TestNameBasedExclusion:
    """Schools serving behavioral, therapeutic, or LD populations should be excluded."""

    # --- Schools that SHOULD be excluded ---

    def test_excludes_carson_valley_childrens_aid(self):
        assert _is_excluded_by_name("Carson Valley Childrens Aid")

    def test_excludes_green_tree_school(self):
        assert _is_excluded_by_name("Green Tree School")

    def test_excludes_aim_academy(self):
        assert _is_excluded_by_name("AIM Academy")

    def test_excludes_behavioral_keyword(self):
        assert _is_excluded_by_name("Sunshine Behavioral Health School")

    def test_excludes_therapeutic_keyword(self):
        assert _is_excluded_by_name("River Valley Therapeutic Day School")

    def test_excludes_autism_keyword(self):
        assert _is_excluded_by_name("Academy for Autism")

    def test_excludes_residential_treatment(self):
        assert _is_excluded_by_name("Pine Grove Residential Treatment Center")

    def test_excludes_juvenile_keyword(self):
        assert _is_excluded_by_name("County Juvenile Academy")

    def test_excludes_learning_differences(self):
        assert _is_excluded_by_name("School for Learning Differences")

    def test_excludes_childrens_home(self):
        assert _is_excluded_by_name("St. Joseph's Children's Home")

    def test_excludes_devereux(self):
        assert _is_excluded_by_name("Devereux Advanced Behavioral Health")

    def test_excludes_psychiatric(self):
        assert _is_excluded_by_name("Eastern Psychiatric Day School")

    def test_excludes_case_insensitive(self):
        assert _is_excluded_by_name("CARSON VALLEY CHILDRENS AID")
        assert _is_excluded_by_name("aim academy")
        assert _is_excluded_by_name("GREEN TREE SCHOOL")

    def test_excludes_benchmark_school(self):
        assert _is_excluded_by_name("Benchmark School")

    def test_excludes_hill_top_preparatory(self):
        assert _is_excluded_by_name("Hill Top Preparatory School")

    def test_excludes_vanguard_school(self):
        assert _is_excluded_by_name("Vanguard School")

    # --- Schools that should NOT be excluded ---

    def test_keeps_typical_catholic_school(self):
        assert not _is_excluded_by_name("St. Mary's Academy")

    def test_keeps_typical_prep_school(self):
        assert not _is_excluded_by_name("Germantown Academy")

    def test_keeps_typical_religious_school(self):
        assert not _is_excluded_by_name("Friends Central School")

    def test_keeps_montessori(self):
        assert not _is_excluded_by_name("Montessori Academy of Philadelphia")

    def test_keeps_typical_high_school(self):
        assert not _is_excluded_by_name("Mount Saint Joseph Academy")

    def test_keeps_stem_school(self):
        assert not _is_excluded_by_name("STEM Academy of Excellence")

    def test_keeps_typical_day_school(self):
        assert not _is_excluded_by_name("Episcopal Academy")

    def test_keeps_typical_private_school(self):
        assert not _is_excluded_by_name("Chestnut Hill Academy")

    # --- Sanity checks on the keyword/name lists ---

    def test_keyword_list_is_nonempty(self):
        assert len(_EXCLUDE_NAME_KEYWORDS) > 0

    def test_explicit_names_list_is_nonempty(self):
        assert len(_EXCLUDE_SCHOOL_NAMES) > 0

    def test_all_keywords_are_lowercase(self):
        for kw in _EXCLUDE_NAME_KEYWORDS:
            assert kw == kw.lower(), f"Keyword '{kw}' should be lowercase"

    def test_all_explicit_names_are_lowercase(self):
        for name in _EXCLUDE_SCHOOL_NAMES:
            assert name == name.lower(), f"Explicit name '{name}' should be lowercase"
