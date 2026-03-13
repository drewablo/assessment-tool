import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.analysis import _score_stage2_component
from modules.housing import _score_stage2_housing
from modules.elder_care import _score_stage2_elder_care
from models.schemas import Stage2Inputs, HousingStage2Inputs, ElderCareStage2Inputs


def test_school_stage2_full_ready_without_total_assets():
    payload = {
        "school_stage2_confirmed": True,
        "school_audit_financials": [
            {"fiscal_year": 2021, "tuition_revenue": 800000, "other_revenue": 300000, "total_expenses": 1020000, "enrollment": 190, "tuition_aid": 120000},
            {"fiscal_year": 2022, "tuition_revenue": 830000, "other_revenue": 330000, "total_expenses": 1080000, "enrollment": 200, "tuition_aid": 130000},
            {"fiscal_year": 2023, "tuition_revenue": 880000, "other_revenue": 360000, "total_expenses": 1120000, "enrollment": 210, "tuition_aid": 140000},
        ],
    }

    result = _score_stage2_component(payload)

    assert result["readiness"] in ("partial", "ready")
    assert result["score"] is not None
    assert "total_assets" not in result["missing_inputs"]


def test_school_stage2_partial_from_historical_only_when_audit_unconfirmed():
    payload = {
        "school_stage2_confirmed": False,
        "school_audit_financials": [
            {"fiscal_year": 2023, "tuition_revenue": 900000, "other_revenue": 300000, "total_expenses": 1100000, "enrollment": 220},
        ],
        "historical_financials": [
            {"year": 2022, "student_count": 210, "tuition_revenue": 850000, "total_revenue": 1200000, "total_expenses": 1080000},
            {"year": 2023, "student_count": 220, "tuition_revenue": 900000, "total_revenue": 1280000, "total_expenses": 1100000},
        ],
    }

    result = _score_stage2_component(payload)

    assert result["readiness"] == "partial"
    assert result["score"] is not None
    assert result["available"] is True
    assert "school_stage2_confirmed" in result["missing_inputs"]


def test_school_stage2_precedence_audit_over_historical_for_same_year():
    payload = {
        "school_stage2_confirmed": True,
        "school_audit_financials": [
            {"fiscal_year": 2023, "tuition_revenue": 500000, "other_revenue": 100000, "total_expenses": 550000, "enrollment": 200},
            {"fiscal_year": 2022, "tuition_revenue": 480000, "other_revenue": 90000, "total_expenses": 530000, "enrollment": 190},
        ],
        "historical_financials": [
            {"year": 2023, "student_count": 999, "tuition_revenue": 999999, "total_revenue": 4000000, "total_expenses": 3999999},
        ],
    }

    result = _score_stage2_component(payload)
    metrics = {c["key"]: c["score"] for c in result["components"]}

    assert result["score"] is not None
    assert metrics["revenue_per_student"] is not None


def test_school_stage2_not_ready_with_no_usable_data():
    result = _score_stage2_component({"school_stage2_confirmed": False})
    assert result["readiness"] == "not_ready"
    assert result["available"] is False


def test_housing_stage2_partial_sets_available_true():
    inputs = Stage2Inputs(housing_financials=HousingStage2Inputs(occupancy_rate=0.9, dscr=1.25))
    result = _score_stage2_housing(inputs)
    assert result["readiness"] == "partial"
    assert result["available"] is True
    assert result["score"] is not None


def test_elder_care_stage2_partial_sets_available_true():
    inputs = Stage2Inputs(elder_care_financials=ElderCareStage2Inputs(occupancy_rate=0.9, days_cash_on_hand=45))
    result = _score_stage2_elder_care(inputs)
    assert result["readiness"] == "partial"
    assert result["available"] is True
    assert result["score"] is not None
