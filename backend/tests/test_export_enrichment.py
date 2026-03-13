import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from main import export_board_pack, export_csv, export_pdf
from models.schemas import AnalysisRequest


class _DummyResult:
    def __init__(self):
        self.trace_id = "trace-123"
        self.school_name = "Test Campus"
        self.analysis_address = "123 Main St"
        self.board_report_pack = {"ok": True}
        self.benchmark_narrative = {"summary": "ok"}
        self.data_freshness = {"mode": "live"}


@pytest.fixture
def request_payload() -> AnalysisRequest:
    return AnalysisRequest(
        school_name="Test Campus",
        address="123 Main St",
        ministry_type="schools",
        mission_mode=False,
        drive_minutes=20,
        gender="coed",
        grade_level="k12",
        weighting_profile="standard_baseline",
        market_context="suburban",
        care_level="all",
    )


@pytest.mark.asyncio
async def test_export_endpoints_share_enrichment_path(monkeypatch, request_payload):
    async def fake_geocode(_address):
        return {"lat": 41.0, "lon": -87.0, "state_fips": "17", "county_fips": "031"}

    async def fake_run_analysis(_location, _request):
        return _DummyResult()

    enriched = []

    async def fake_enrich(result, request):
        enriched.append((result, request.school_name))
        return result

    csv_seen = []

    def fake_generate_csv_report(result):
        csv_seen.append(result)
        return "a,b\n1,2\n"

    pdf_seen = []

    def fake_generate_pdf_report(result, _request):
        pdf_seen.append(result)
        return b"%PDF-1.4"

    monkeypatch.setattr("main.geocode_address", fake_geocode)
    monkeypatch.setattr("main._run_analysis", fake_run_analysis)
    monkeypatch.setattr("main._enrich_analysis_result", fake_enrich)
    monkeypatch.setattr("main.generate_csv_report", fake_generate_csv_report)
    monkeypatch.setattr("api.pdf_report.generate_pdf_report", fake_generate_pdf_report)

    board = await export_board_pack(request_payload)
    await export_csv(request_payload)
    await export_pdf(request_payload)

    assert len(enriched) == 3
    assert len(csv_seen) == 1
    assert len(pdf_seen) == 1
    assert board["trace_id"] == "trace-123"
