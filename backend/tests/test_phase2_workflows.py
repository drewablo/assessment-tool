import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from fastapi import HTTPException
from main import _facility_feasibility_adjustments, _phase1_decision_pathway, create_portfolio_workspace
from models.schemas import AnalysisRequest, FacilityProfile, PortfolioWorkspaceCreateRequest


class _Score:
    def __init__(self, overall):
        self.overall = overall
        self.stage2 = None


class _Result:
    def __init__(self, score):
        self.feasibility_score = _Score(score)
        self.demographics = type("_Demographics", (), {"data_confidence": "medium"})()


def test_facility_feasibility_adjustments_penalize_constraints():
    adjustment, risks, validations = _facility_feasibility_adjustments(
        AnalysisRequest(
            school_name="X",
            address="123 Main St",
            ministry_type="housing",
            facility_profile=FacilityProfile(
                building_square_footage=9000,
                deferred_maintenance_estimate=1_200_000,
                zoning_use_constraints=["Special use permit"],
                accessibility_constraints=["No elevator"],
                sponsor_operator_capacity="low",
            ),
        )
    )

    assert adjustment < 0
    assert risks
    assert validations


def test_phase1_pathway_includes_partner_assessment_when_partner_selected():
    result = _Result(50)
    request = AnalysisRequest(
        school_name="X",
        address="123 Main St",
        ministry_type="schools",
        facility_profile=FacilityProfile(sponsor_operator_capacity="low"),
    )

    recommendation = _phase1_decision_pathway(result, request)

    assert recommendation.recommended_pathway == "partner"
    assert recommendation.partner_assessment is not None
    assert recommendation.partner_assessment.mission_alignment_score >= 70


@pytest.mark.asyncio
async def test_portfolio_workspace_requires_db(monkeypatch):
    monkeypatch.setattr("main.USE_DB", False)

    with pytest.raises(HTTPException) as exc:
        await create_portfolio_workspace(
            PortfolioWorkspaceCreateRequest(
                engagement_name="Springfield Portfolio",
                client_name="St. Mark Province",
            )
        )

    assert exc.value.status_code == 501
    assert "USE_DB=true" in str(exc.value.detail)
