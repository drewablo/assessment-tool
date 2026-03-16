import asyncio
import os
import zipfile
from pathlib import Path

import pandas as pd

from pipeline import ingest_elder_care, ingest_housing


def test_cms_fetch_uses_provider_data_api_structure(monkeypatch):
    captured = {}

    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return {"results": [{"provider_name": "x", "federal_provider_number": "11"}]}

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url, params=None, timeout=None):
            captured["url"] = url
            captured["params"] = params
            return _Resp()

    monkeypatch.setenv("CMS_PROVIDER_DATA_API_URL", "https://data.cms.gov/provider-data/api/1/datastore/query/4pq5-n9py/0")
    monkeypatch.setattr(ingest_elder_care.httpx, "AsyncClient", _Client)

    rows = asyncio.run(ingest_elder_care._fetch_cms_facilities(offset=1000, limit=250))
    assert len(rows) == 1
    assert captured["url"].endswith("/4pq5-n9py/0")
    assert captured["params"] == {"limit": 250, "offset": 1000}


def test_elder_transform_accepts_current_cms_provider_id_field():
    raw = {
        "provider_id": "65A123",
        "provider_name": "Current CMS Facility",
        "latitude": "41.9001",
        "longitude": "-87.6502",
        "provider_city": "Chicago",
        "provider_state": "IL",
    }

    transformed = ingest_elder_care._transform_facility(raw)
    assert transformed is not None
    assert transformed["provider_id"] == "65A123"
    assert transformed["facility_name"] == "Current CMS Facility"
    assert transformed["lat"] == 41.9001
    assert transformed["lon"] == -87.6502


def test_elder_transform_accepts_schema_variants_and_location_string():
    raw = {
        "ccn": "123456",
        "facility_name": "Variant Nursing Home",
        "location": "(41.9001, -87.6502)",
        "provider_city": "Chicago",
        "provider_state": "IL",
    }

    transformed = ingest_elder_care._transform_facility(raw)
    assert transformed is not None
    assert transformed["provider_id"] == "123456"
    assert transformed["facility_name"] == "Variant Nursing Home"
    assert transformed["lat"] == 41.9001
    assert transformed["lon"] == -87.6502


def test_elder_rejection_reasons_not_all_rows_dropped():
    rows = [
        {"provider_name": "No ID", "latitude": 41.0, "longitude": -87.0},
        {"federal_provider_number": "111111", "provider_name": "Zero Coord", "latitude": 0, "longitude": 0},
        {"federal_provider_number": "222222", "provider_name": "Good", "latitude": 41.1, "longitude": -87.1},
    ]

    transformed = [t for row in rows if (t := ingest_elder_care._transform_facility(row))]
    reasons = [ingest_elder_care._transform_rejection_reason(row) for row in rows]

    assert len(transformed) == 1
    assert "missing_provider_id" in reasons
    assert "zero_lat_lon" in reasons
    assert reasons.count(None) == 1


def test_onefact_fetch_reads_csv_rows(monkeypatch):
    class _Resp:
        text = "Facility Name,Latitude,Longitude,City,State,Capacity\nAlpha AL,41.9,-87.6,Chicago,IL,120\n"

        def raise_for_status(self):
            return None

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url, follow_redirects=None, timeout=None):
            assert "assisted-living-facilities.csv" in url
            return _Resp()

    monkeypatch.setattr(ingest_elder_care.httpx, "AsyncClient", _Client)
    rows = asyncio.run(ingest_elder_care._fetch_onefact_assisted_living_rows())
    assert len(rows) == 1
    assert rows[0]["Facility Name"] == "Alpha AL"


def test_onefact_transform_maps_assisted_living_fields():
    raw = {
        "Facility Name": "Alpha AL",
        "Latitude": "41.9001",
        "Longitude": "-87.6502",
        "City": "Chicago",
        "State": "IL",
        "Capacity": "110",
        "Ownership Type": "For Profit",
    }

    transformed = ingest_elder_care._transform_onefact_facility(raw)
    assert transformed is not None
    assert transformed["facility_name"] == "Alpha AL"
    assert transformed["care_level"] == "assisted_living"
    assert transformed["data_source"] == "onefact"
    assert transformed["certified_beds"] == 110
    assert transformed["overall_rating"] is None
    assert transformed["provider_id"].startswith("onefact_")




def test_property_transform_accepts_lihtcpub_style_headers():
    raw = {
        "NLIHC_ID": "CA-23-0001",
        "Proj_Name": "Sample Homes",
        "Latitude": "34.0522",
        "Longitude": "-118.2437",
        "Proj_Cty": "Los Angeles",
        "Proj_St": "CA",
    }
    transformed = ingest_housing._transform_project(raw)
    assert transformed is not None
    assert transformed["hud_id"] == "CA-23-0001"
    assert transformed["project_name"] == "Sample Homes"

def test_lihtc_zip_extraction_ingests_csv_and_cleans_temp(monkeypatch, tmp_path):
    zip_bytes = tmp_path / "lihtcpub.zip"
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("nested/path/LIHTCPUB_2026.CSV", "HUD_ID,PROJECT,LATITUDE,LONGITUDE\n1,Alpha,41.1,-87.1\n")
        zf.writestr("OTHER/ignore.csv", "foo,bar\n1,2\n")

    class _TD:
        def __init__(self, *args, **kwargs):
            self.dir = tmp_path / "work"
            self.dir.mkdir(exist_ok=True)

        def __enter__(self):
            return str(self.dir)

        def __exit__(self, exc_type, exc, tb):
            return False

    async def _fake_download(url: str, path: Path):
        path.write_bytes(zip_bytes.read_bytes())

    monkeypatch.setattr(ingest_housing.tempfile, "TemporaryDirectory", _TD)
    monkeypatch.setattr(ingest_housing, "_download_file", _fake_download)

    rows = asyncio.run(ingest_housing._fetch_lihtc_property_rows_from_zip())
    assert len(rows) == 1
    assert rows[0]["HUD_ID"] == "1"
    assert not (tmp_path / "work" / "LIHTCPUB.ZIP").exists()
    assert not (tmp_path / "work" / "LIHTCPUB_2026.CSV").exists()


def test_lihtc_zip_cleanup_even_when_csv_read_fails(monkeypatch, tmp_path):
    zip_bytes = tmp_path / "lihtcpub.zip"
    with zipfile.ZipFile(zip_bytes, "w") as zf:
        zf.writestr("nested/lihtcpub.csv", "HUD_ID,PROJECT,LATITUDE,LONGITUDE\n")

    class _TD:
        def __init__(self, *args, **kwargs):
            self.dir = tmp_path / "work_fail"
            self.dir.mkdir(exist_ok=True)

        def __enter__(self):
            return str(self.dir)

        def __exit__(self, exc_type, exc, tb):
            return False

    async def _fake_download(url: str, path: Path):
        path.write_bytes(zip_bytes.read_bytes())

    def _boom(path: Path):
        raise RuntimeError("csv parse failed")

    monkeypatch.setattr(ingest_housing.tempfile, "TemporaryDirectory", _TD)
    monkeypatch.setattr(ingest_housing, "_download_file", _fake_download)
    monkeypatch.setattr(ingest_housing, "_rows_from_csv", _boom)

    try:
        asyncio.run(ingest_housing._fetch_lihtc_property_rows_from_zip())
        assert False, "expected runtime error"
    except RuntimeError as exc:
        assert "csv parse failed" in str(exc)

    assert not (tmp_path / "work_fail" / "LIHTCPUB.ZIP").exists()
    assert not (tmp_path / "work_fail" / "lihtcpub.csv").exists()


def test_lihtc_zip_member_selection_prefers_primary_csv():
    selected = ingest_housing._select_lihtc_csv_member(
        ["foo/readme.txt", "nested/other.csv", "CAPS/LIHTCPUB.CSV", "sub/lihtcpub_backup.csv"]
    )
    assert selected == "CAPS/LIHTCPUB.CSV"


def test_hud_default_urls_match_expected_and_can_be_overridden(monkeypatch):
    assert ingest_housing.HUD_LIHTC_PROPERTY_ZIP_URL == "https://www.huduser.gov/lihtc/lihtcpub.zip"
    assert ingest_housing.HUD_LIHTC_TENANT_XLSX_DEFAULT_URL == "https://www.huduser.gov/portal/Datasets/lihtc/2023-LIHTC-Tenant-Tables.xlsx"
    assert ingest_housing.HUD_QCT_DDA_XLSX_DEFAULT_URL == "https://www.huduser.gov/portal/datasets/qct/qct_data_2026.xlsx"

    monkeypatch.setenv("HUD_LIHTC_TENANT_XLSX_URL", "https://example.com/tenant.xlsx")
    monkeypatch.setenv("HUD_QCT_DDA_XLSX_URL", "https://example.com/qct.xlsx")
    assert os.getenv("HUD_LIHTC_TENANT_XLSX_URL") == "https://example.com/tenant.xlsx"
    assert os.getenv("HUD_QCT_DDA_XLSX_URL") == "https://example.com/qct.xlsx"


def test_tenant_xlsx_parsing_fixture(tmp_path):
    xlsx_path = tmp_path / "tenant.xlsx"
    with pd.ExcelWriter(xlsx_path) as writer:
        pd.DataFrame(
            [
                ["Tenant Characteristics", None, None],
                ["Category", "2022", "2023"],
                ["Elderly", 2, 4],
                ["Family", 5, 7],
            ]
        ).to_excel(writer, index=False, header=False, sheet_name="Table 1")
        pd.DataFrame([["Cover"]]).to_excel(writer, index=False, header=False, sheet_name="Cover")

    rows, sheets = ingest_housing._extract_tenant_rows_from_workbook(xlsx_path)
    assert "Table 1" in sheets["sheet_names"]
    transformed = [ingest_housing._transform_tenant(r) for r in rows]
    transformed = [r for r in transformed if r]
    assert transformed
    assert any(r["reporting_year"] == 2023 and r["household_count"] == 4 for r in transformed)


def test_tenant_layout_detection_handles_multirow_headers():
    df = pd.DataFrame(
        [
            ["Table 1: Example", None, None, None],
            ["State", "Households", "Households", "Percent"],
            ["Category", "2022", "2023", "2023"],
            ["Elderly", 10, 12, "40%"],
        ]
    )
    layout = ingest_housing._detect_lihtc_tenant_table_layout(df, "Table 1")
    assert layout["header_row"] is not None
    assert layout["value_cols"]
    assert layout["column_labels"][1]


def test_tenant_sheet_parsing_emits_long_form_cells():
    df = pd.DataFrame(
        [
            ["Tenant Characteristics", None, None],
            ["Category", "2022", "2023"],
            ["Elderly", 2, 4],
            ["Family", 5, "N/A"],
            ["Source: HUD", None, None],
        ]
    )
    layout = ingest_housing._detect_lihtc_tenant_table_layout(df, "Table 1")
    parsed, diag = ingest_housing._parse_lihtc_tenant_sheet(df, layout, "Table 1", 2023)
    normalized = ingest_housing._normalize_lihtc_tenant_summary_rows(parsed, 2023)

    assert diag["normalized_rows"] == len(parsed)
    assert len(parsed) >= 3
    assert any(r["row_label"] == "Elderly" and r["column_label"].endswith("2023") for r in parsed)
    assert any(r["value_text"] == "N/A" for r in parsed)
    assert any(r["table_id"] == "table_1" for r in normalized)


def test_qct_xlsx_parsing_fixture(tmp_path):
    xlsx_path = tmp_path / "qct.xlsx"
    with pd.ExcelWriter(xlsx_path) as writer:
        pd.DataFrame(
            [
                ["HUD QCT", None, None, None],
                ["State", "County", "Tract", "Designation Type"],
                ["12", "086", "12345678901", "QCT"],
            ]
        ).to_excel(writer, index=False, header=False, sheet_name="AL to MO")
        pd.DataFrame(
            [
                ["State", "County", "Tract", "Designation Type"],
                ["48", "201", "48201999999", "DDA"],
            ]
        ).to_excel(writer, index=False, header=False, sheet_name="MT to WY & PR")

    rows, sheets = ingest_housing._extract_qct_rows_from_workbook(xlsx_path)
    assert sheets["chosen_sheets"] == ["AL to MO", "MT to WY & PR"]
    transformed = [ingest_housing._transform_qct(r) for r in rows]
    transformed = [r for r in transformed if r]
    assert transformed
    assert {r["designation_type"] for r in transformed} == {"QCT", "DDA"}


def test_hud_orchestration_reports_steps_independently(monkeypatch):
    async def _ok_property():
        return {"processed": 3, "upserted": 2}

    async def _fail_tenant():
        raise RuntimeError("tenant failed")

    async def _ok_qct():
        return {"processed": 8, "upserted": 8}

    monkeypatch.setattr(ingest_housing, "_ingest_hud_property_async", _ok_property)
    monkeypatch.setattr(ingest_housing, "_ingest_hud_tenant_async", _fail_tenant)
    monkeypatch.setattr(ingest_housing, "_ingest_hud_qct_async", _ok_qct)

    summary = asyncio.run(ingest_housing._ingest_housing_all_async())
    assert summary["lihtc_property"]["status"] == "success"
    assert summary["lihtc_tenant"]["status"] == "failed"
    assert summary["qct_dda"]["status"] == "success"
