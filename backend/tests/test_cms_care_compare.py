import json
import sys
import os

import httpx
import pandas as pd
import pytest

# Allow imports from backend/ root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import competitors.cms_care_compare as cms


def test_load_provider_facilities_uses_provider_beds_without_mds(tmp_path):
    provider_file = tmp_path / "provider.csv"
    pd.DataFrame(
        [
            {
                "provider_name": "Sunrise SNF",
                "ccn": "12345",
                "latitude": 41.88,
                "longitude": -87.63,
                "city": "Chicago",
                "ownership": "Non-profit",
                "number_of_certified_beds": 120,
                "provider_type": "Skilled Nursing Facility",
            }
        ]
    ).to_csv(provider_file, index=False)

    orig_provider = cms.PROVIDER_INFO_FILE
    orig_mds = cms.MDS_QUALITY_FILE
    try:
        cms.PROVIDER_INFO_FILE = provider_file
        cms.MDS_QUALITY_FILE = tmp_path / "missing_mds.csv"

        rows = cms._load_provider_facilities()
    finally:
        cms.PROVIDER_INFO_FILE = orig_provider
        cms.MDS_QUALITY_FILE = orig_mds

    assert len(rows) == 1
    row = rows[0]
    assert row["name"] == "Sunrise SNF"
    assert row["ccn"] == "012345"
    assert row["certified_beds"] == 120
    assert row["licensed_beds"] == 120
    assert row["care_level"] == "skilled_nursing"


def test_load_provider_facilities_parses_socrata_location_and_alt_beds_column(tmp_path):
    provider_file = tmp_path / "provider.csv"
    pd.DataFrame(
        [
            {
                "provider_name": "Location Parsed Facility",
                "ccn": "543210",
                "location": "(41.9001, -87.6502)",
                "number_of_certified_beds_in_facility": "75",
                "provider_type": "Nursing Home",
            }
        ]
    ).to_csv(provider_file, index=False)

    orig_provider = cms.PROVIDER_INFO_FILE
    orig_mds = cms.MDS_QUALITY_FILE
    try:
        cms.PROVIDER_INFO_FILE = provider_file
        cms.MDS_QUALITY_FILE = tmp_path / "missing_mds.csv"

        rows = cms._load_provider_facilities()
    finally:
        cms.PROVIDER_INFO_FILE = orig_provider
        cms.MDS_QUALITY_FILE = orig_mds

    assert len(rows) == 1
    row = rows[0]
    assert row["lat"] == 41.9001
    assert row["lon"] == -87.6502
    assert row["certified_beds"] == 75


def test_load_provider_facilities_mds_filter_applies_only_if_non_empty(tmp_path):
    provider_file = tmp_path / "provider.csv"
    mds_file = tmp_path / "mds.csv"

    pd.DataFrame(
        [
            {
                "provider_name": "Keeps Data Even If MDS Doesn't Match",
                "ccn": "123456",
                "latitude": 41.88,
                "longitude": -87.63,
                "number_of_certified_beds": 110,
                "overall_rating": "3",
            }
        ]
    ).to_csv(provider_file, index=False)
    pd.DataFrame([{"ccn": "999999"}]).to_csv(mds_file, index=False)

    orig_provider = cms.PROVIDER_INFO_FILE
    orig_mds = cms.MDS_QUALITY_FILE
    try:
        cms.PROVIDER_INFO_FILE = provider_file
        cms.MDS_QUALITY_FILE = mds_file

        rows = cms._load_provider_facilities()
    finally:
        cms.PROVIDER_INFO_FILE = orig_provider
        cms.MDS_QUALITY_FILE = orig_mds

    assert len(rows) == 1
    assert rows[0]["name"] == "Keeps Data Even If MDS Doesn't Match"


def test_pick_column_normalizes_spaces_and_underscores(tmp_path):
    """CMS CSV exports use spaces in headers; our candidates use underscores."""
    provider_file = tmp_path / "provider.csv"
    pd.DataFrame(
        [
            {
                "Provider Name": "Space Header SNF",
                "Federal Provider Number": "67890",
                "Latitude": 41.88,
                "Longitude": -87.63,
                "Provider City": "Chicago",
                "Ownership Type": "Non-profit",
                "Number of Certified Beds": 95,
                "Provider Type": "Skilled Nursing Facility",
            }
        ]
    ).to_csv(provider_file, index=False)

    orig_provider = cms.PROVIDER_INFO_FILE
    orig_mds = cms.MDS_QUALITY_FILE
    try:
        cms.PROVIDER_INFO_FILE = provider_file
        cms.MDS_QUALITY_FILE = tmp_path / "missing_mds.csv"

        rows = cms._load_provider_facilities()
    finally:
        cms.PROVIDER_INFO_FILE = orig_provider
        cms.MDS_QUALITY_FILE = orig_mds

    assert len(rows) == 1
    row = rows[0]
    assert row["name"] == "Space Header SNF"
    assert row["certified_beds"] == 95
    assert row["licensed_beds"] == 95
    # "Provider Type" is certification (not care level) — care_level defaults
    # to "skilled_nursing" since dataset 4pq5-n9py only contains nursing homes.
    assert row["care_level"] == "skilled_nursing"
    assert row["certification"] == "Skilled Nursing Facility"


def test_provider_type_medicare_does_not_become_care_level(tmp_path):
    """CMS 'Provider Type' holds certification (Medicare/Medicaid), not care level."""
    provider_file = tmp_path / "provider.csv"
    pd.DataFrame(
        [
            {
                "Provider Name": "Sunny Acres Nursing Home",
                "Federal Provider Number": "33001",
                "Latitude": 40.71,
                "Longitude": -74.00,
                "Provider City": "New York",
                "Ownership Type": "For profit - Corporation",
                "Number of Certified Beds": 200,
                "Provider Type": "Medicare and Medicaid",
            }
        ]
    ).to_csv(provider_file, index=False)

    orig_provider = cms.PROVIDER_INFO_FILE
    orig_mds = cms.MDS_QUALITY_FILE
    try:
        cms.PROVIDER_INFO_FILE = provider_file
        cms.MDS_QUALITY_FILE = tmp_path / "missing_mds.csv"

        rows = cms._load_provider_facilities()
    finally:
        cms.PROVIDER_INFO_FILE = orig_provider
        cms.MDS_QUALITY_FILE = orig_mds

    assert len(rows) == 1
    row = rows[0]
    # care_level should NOT be "all" (the old bug); it defaults to "skilled_nursing"
    assert row["care_level"] == "skilled_nursing"
    # certification captures the actual Provider Type value
    assert row["certification"] == "Medicare and Medicaid"
    # ownership comes from Ownership Type, not Provider Type
    assert row["ownership"] == "For profit - Corporation"
    # beds should be parsed
    assert row["certified_beds"] == 200


def test_old_socrata_column_names(tmp_path):
    """Old Socrata short column names (PROVNAME, BEDCERT, etc.) are recognized."""
    provider_file = tmp_path / "provider.csv"
    pd.DataFrame(
        [
            {
                "PROVNAME": "Legacy SNF",
                "PROVNUM": "012345",
                "LOCATION": "(41.88, -87.63)",
                "CITY": "Chicago",
                "OWNERSHIP": "Non-profit",
                "BEDCERT": 150,
            }
        ]
    ).to_csv(provider_file, index=False)

    orig_provider = cms.PROVIDER_INFO_FILE
    orig_mds = cms.MDS_QUALITY_FILE
    try:
        cms.PROVIDER_INFO_FILE = provider_file
        cms.MDS_QUALITY_FILE = tmp_path / "missing_mds.csv"

        rows = cms._load_provider_facilities()
    finally:
        cms.PROVIDER_INFO_FILE = orig_provider
        cms.MDS_QUALITY_FILE = orig_mds

    assert len(rows) == 1
    row = rows[0]
    assert row["name"] == "Legacy SNF"
    assert row["certified_beds"] == 150
    assert row["care_level"] == "skilled_nursing"


def test_load_provider_facilities_reads_overall_rating_from_provider_file(tmp_path):
    provider_file = tmp_path / "provider.csv"

    pd.DataFrame(
        [
            {
                "provider_name": "Concordia at Rebecca Residence",
                "ccn": "12345.0",
                "latitude": 41.88,
                "longitude": -87.63,
                "number_of_certified_beds": 100,
                "Overall Rating": "4",
            }
        ]
    ).to_csv(provider_file, index=False)

    orig_provider = cms.PROVIDER_INFO_FILE
    orig_mds = cms.MDS_QUALITY_FILE
    try:
        cms.PROVIDER_INFO_FILE = provider_file
        cms.MDS_QUALITY_FILE = tmp_path / "missing_mds.csv"

        rows = cms._load_provider_facilities()
    finally:
        cms.PROVIDER_INFO_FILE = orig_provider
        cms.MDS_QUALITY_FILE = orig_mds

    assert len(rows) == 1
    row = rows[0]
    assert row["name"] == "Concordia at Rebecca Residence"
    assert row["ccn"] == "012345"
    assert row["mds_overall_rating"] == 4


def test_normalize_ccn_strips_decimal_suffix_from_numeric_text():
    assert cms._normalize_ccn("12345.0") == "012345"
    assert cms._normalize_ccn("12345.000") == "012345"


# ---------------------------------------------------------------------------
# Download function tests (mocked HTTP)
# ---------------------------------------------------------------------------

def _make_provider_csv_bytes(n: int = 5) -> bytes:
    """Build a valid CSV payload large enough to pass the minimum-size check."""
    rows = []
    for i in range(n):
        rows.append({
            "provider_name": f"Facility {i}",
            "latitude": 41.88 + i * 0.01,
            "longitude": -87.63 + i * 0.01,
            "number_of_certified_beds": 100 + i,
            "provider_type": "Skilled Nursing Facility",
        })
    csv_text = pd.DataFrame(rows).to_csv(index=False)
    # Pad to exceed _CMS_MIN_FILE_SIZE if needed
    if len(csv_text.encode()) < cms._CMS_MIN_FILE_SIZE:
        csv_text += "\n" * (cms._CMS_MIN_FILE_SIZE - len(csv_text.encode()) + 1)
    return csv_text.encode("utf-8")


def _make_provider_json_results(n: int = 5) -> list[dict]:
    """Build a list of JSON result dicts matching the CMS Provider Data API."""
    return [
        {
            "provider_name": f"JSON Facility {i}",
            "latitude": str(41.88 + i * 0.01),
            "longitude": str(-87.63 + i * 0.01),
            "number_of_certified_beds": str(100 + i),
            "provider_type": "Skilled Nursing Facility",
        }
        for i in range(n)
    ]


@pytest.mark.asyncio
async def test_try_csv_download_success(tmp_path, monkeypatch):
    """CSV download succeeds → file written and returns True."""
    monkeypatch.setattr(cms, "PROVIDER_INFO_FILE", tmp_path / "provider.csv")
    monkeypatch.setattr(cms, "_DATA_DIR", tmp_path)
    csv_bytes = _make_provider_csv_bytes()

    async def mock_get(self, url, **kwargs):
        resp = httpx.Response(200, content=csv_bytes, request=httpx.Request("GET", url))
        return resp

    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)
    result = await cms._try_csv_download()
    assert result is True
    assert cms.PROVIDER_INFO_FILE.exists()


@pytest.mark.asyncio
async def test_try_csv_download_http_error_returns_false(tmp_path, monkeypatch):
    """CSV download gets an HTTP error → returns False."""
    monkeypatch.setattr(cms, "PROVIDER_INFO_FILE", tmp_path / "provider.csv")
    monkeypatch.setattr(cms, "_DATA_DIR", tmp_path)

    async def mock_get(self, url, **kwargs):
        req = httpx.Request("GET", url)
        resp = httpx.Response(410, request=req)
        resp.raise_for_status()

    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)
    result = await cms._try_csv_download()
    assert result is False


@pytest.mark.asyncio
async def test_try_json_api_download_success(tmp_path, monkeypatch):
    """JSON API download with a single page → file written and returns True."""
    monkeypatch.setattr(cms, "PROVIDER_INFO_FILE", tmp_path / "provider.csv")
    monkeypatch.setattr(cms, "_DATA_DIR", tmp_path)
    monkeypatch.setattr(cms, "_CMS_JSON_PAGE_SIZE", 100)
    results = _make_provider_json_results(10)

    async def mock_get(self, url, **kwargs):
        payload = json.dumps({"results": results}).encode()
        resp = httpx.Response(200, content=payload, request=httpx.Request("GET", url))
        return resp

    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)
    # The CSV produced from 10 small rows may be < _CMS_MIN_FILE_SIZE, lower the bar
    monkeypatch.setattr(cms, "_CMS_MIN_FILE_SIZE", 1)
    result = await cms._try_json_api_download()
    assert result is True
    assert cms.PROVIDER_INFO_FILE.exists()
    saved_df = pd.read_csv(cms.PROVIDER_INFO_FILE)
    assert len(saved_df) == 10
    assert "provider_name" in saved_df.columns


@pytest.mark.asyncio
async def test_try_json_api_download_paginates(tmp_path, monkeypatch):
    """JSON API paginates correctly across two pages."""
    monkeypatch.setattr(cms, "PROVIDER_INFO_FILE", tmp_path / "provider.csv")
    monkeypatch.setattr(cms, "_DATA_DIR", tmp_path)
    monkeypatch.setattr(cms, "_CMS_JSON_PAGE_SIZE", 3)
    monkeypatch.setattr(cms, "_CMS_MIN_FILE_SIZE", 100)

    page1 = _make_provider_json_results(3)  # full page → triggers next request
    page2 = _make_provider_json_results(2)  # partial page → stops

    call_count = 0

    async def mock_get(self, url, **kwargs):
        nonlocal call_count
        data = page1 if call_count == 0 else page2
        call_count += 1
        payload = json.dumps({"results": data}).encode()
        return httpx.Response(200, content=payload, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.AsyncClient, "get", mock_get)
    result = await cms._try_json_api_download()
    assert result is True
    assert call_count == 2
    saved_df = pd.read_csv(cms.PROVIDER_INFO_FILE)
    assert len(saved_df) == 5


@pytest.mark.asyncio
async def test_download_cms_provider_data_falls_back_to_json(tmp_path, monkeypatch):
    """When CSV download fails, _download_cms_provider_data falls back to JSON API."""
    monkeypatch.setattr(cms, "PROVIDER_INFO_FILE", tmp_path / "provider.csv")
    monkeypatch.setattr(cms, "_DATA_DIR", tmp_path)
    monkeypatch.setattr(cms, "_CMS_MIN_FILE_SIZE", 100)

    csv_called = False
    json_called = False

    async def mock_csv_fail():
        nonlocal csv_called
        csv_called = True
        return False

    async def mock_json_ok():
        nonlocal json_called
        json_called = True
        return True

    monkeypatch.setattr(cms, "_try_csv_download", mock_csv_fail)
    monkeypatch.setattr(cms, "_try_json_api_download", mock_json_ok)
    result = await cms._download_cms_provider_data()
    assert result is True
    assert csv_called
    assert json_called


@pytest.mark.asyncio
async def test_download_cms_pbj_data_returns_false(tmp_path, monkeypatch):
    """PBJ download is disabled; provider-info census is used instead."""
    monkeypatch.setattr(cms, "PBJ_DAILY_STAFFING_FILE", tmp_path / "pbj.csv")
    monkeypatch.setattr(cms, "_DATA_DIR", tmp_path)

    result = await cms._download_cms_pbj_data()
    assert result is False


def test_cms_pbj_cache_invalid_when_required_columns_missing(tmp_path, monkeypatch):
    pbj_file = tmp_path / "pbj.csv"
    pd.DataFrame([{"provider_id": "1", "foo": "bar"}]).to_csv(pbj_file, index=False)

    monkeypatch.setattr(cms, "PBJ_DAILY_STAFFING_FILE", pbj_file)
    monkeypatch.setattr(cms, "_CMS_MIN_FILE_SIZE", 1)

    assert cms._cms_pbj_cache_is_valid() is False


def test_cms_provider_cache_invalid_when_beds_column_missing(tmp_path, monkeypatch):
    provider_file = tmp_path / "provider.csv"
    pd.DataFrame([{"provider_name": "Hospital-ish row", "latitude": 41.88, "longitude": -87.63}]).to_csv(
        provider_file,
        index=False,
    )

    monkeypatch.setattr(cms, "PROVIDER_INFO_FILE", provider_file)
    monkeypatch.setattr(cms, "_CMS_MIN_FILE_SIZE", 1)

    assert cms._cms_cache_is_valid() is False


def test_load_provider_facilities_ignores_mds_for_overall_rating(tmp_path):
    provider_file = tmp_path / "provider.csv"
    mds_file = tmp_path / "mds.csv"

    pd.DataFrame(
        [
            {
                "provider_name": "Rated SNF",
                "ccn": "123456",
                "latitude": 41.88,
                "longitude": -87.63,
                "number_of_certified_beds": 110,
                "overall_rating": "3",
            }
        ]
    ).to_csv(provider_file, index=False)
    pd.DataFrame([{"ccn": "123456", "overall_rating": "4"}]).to_csv(mds_file, index=False)

    orig_provider = cms.PROVIDER_INFO_FILE
    orig_mds = cms.MDS_QUALITY_FILE
    try:
        cms.PROVIDER_INFO_FILE = provider_file
        cms.MDS_QUALITY_FILE = mds_file

        rows = cms._load_provider_facilities()
    finally:
        cms.PROVIDER_INFO_FILE = orig_provider
        cms.MDS_QUALITY_FILE = orig_mds

    assert len(rows) == 1
    assert rows[0]["mds_overall_rating"] == 3


@pytest.mark.asyncio
async def test_get_nearby_elder_care_facilities_filters_by_min_mds_rating(monkeypatch):
    async def no_download():
        return True

    monkeypatch.setattr(cms, "_cms_cache_is_valid", lambda: True)
    monkeypatch.setattr(cms, "_download_cms_provider_data", no_download)
    monkeypatch.setattr(
        cms,
        "_load_facilities",
        lambda: [
            {
                "name": "Two Star",
                "lat": 41.88,
                "lon": -87.63,
                "care_level": "skilled_nursing",
                "mds_overall_rating": 2,
            },
            {
                "name": "Four Star",
                "lat": 41.881,
                "lon": -87.631,
                "care_level": "skilled_nursing",
                "mds_overall_rating": 4,
            },
        ],
    )

    rows = await cms.get_nearby_elder_care_facilities(41.88, -87.63, 10, "snf", min_mds_overall_rating=3)

    assert len(rows) == 1
    assert rows[0]["name"] == "Four Star"


def test_load_provider_facilities_derives_occupancy_from_census_and_beds(tmp_path):
    provider_file = tmp_path / "provider.csv"
    pd.DataFrame(
        [
            {
                "provider_name": "Occupancy SNF",
                "ccn": "123456",
                "latitude": 41.88,
                "longitude": -87.63,
                "number_of_certified_beds": 120,
                "average_number_of_residents_per_day": 90,
            }
        ]
    ).to_csv(provider_file, index=False)

    orig_provider = cms.PROVIDER_INFO_FILE
    orig_mds = cms.MDS_QUALITY_FILE
    orig_pbj = cms.PBJ_DAILY_STAFFING_FILE
    try:
        cms.PROVIDER_INFO_FILE = provider_file
        cms.MDS_QUALITY_FILE = tmp_path / "missing_mds.csv"
        cms.PBJ_DAILY_STAFFING_FILE = tmp_path / "missing_pbj.csv"

        rows = cms._load_provider_facilities()
    finally:
        cms.PROVIDER_INFO_FILE = orig_provider
        cms.MDS_QUALITY_FILE = orig_mds
        cms.PBJ_DAILY_STAFFING_FILE = orig_pbj

    assert len(rows) == 1
    assert rows[0]["occupancy_pct"] == 75.0


def test_load_provider_facilities_uses_pbj_census_for_occupancy(tmp_path):
    provider_file = tmp_path / "provider.csv"
    pbj_file = tmp_path / "pbj.csv"
    pd.DataFrame(
        [
            {
                "provider_name": "PBJ SNF",
                "ccn": "123456",
                "latitude": 41.88,
                "longitude": -87.63,
                "number_of_certified_beds": 100,
                "average_number_of_residents_per_day": 20,
            }
        ]
    ).to_csv(provider_file, index=False)
    pd.DataFrame(
        [
            {"ccn": "123456", "MDScensus": 90},
            {"ccn": "123456", "MDScensus": 100},
        ]
    ).to_csv(pbj_file, index=False)

    orig_provider = cms.PROVIDER_INFO_FILE
    orig_mds = cms.MDS_QUALITY_FILE
    orig_pbj = cms.PBJ_DAILY_STAFFING_FILE
    try:
        cms.PROVIDER_INFO_FILE = provider_file
        cms.MDS_QUALITY_FILE = tmp_path / "missing_mds.csv"
        cms.PBJ_DAILY_STAFFING_FILE = pbj_file

        rows = cms._load_provider_facilities()
    finally:
        cms.PROVIDER_INFO_FILE = orig_provider
        cms.MDS_QUALITY_FILE = orig_mds
        cms.PBJ_DAILY_STAFFING_FILE = orig_pbj

    assert len(rows) == 1
    assert rows[0]["occupancy_pct"] == 95.0


def test_load_provider_facilities_keeps_occupancy_unknown_when_pbj_missing_ccn(tmp_path):
    provider_file = tmp_path / "provider.csv"
    pbj_file = tmp_path / "pbj.csv"
    pd.DataFrame(
        [
            {
                "provider_name": "PBJ Missing SNF",
                "ccn": "123456",
                "latitude": 41.88,
                "longitude": -87.63,
                "number_of_certified_beds": 100,
                "average_number_of_residents_per_day": 90,
            }
        ]
    ).to_csv(provider_file, index=False)
    pd.DataFrame([{"ccn": "999999", "MDScensus": 95}]).to_csv(pbj_file, index=False)

    orig_provider = cms.PROVIDER_INFO_FILE
    orig_mds = cms.MDS_QUALITY_FILE
    orig_pbj = cms.PBJ_DAILY_STAFFING_FILE
    try:
        cms.PROVIDER_INFO_FILE = provider_file
        cms.MDS_QUALITY_FILE = tmp_path / "missing_mds.csv"
        cms.PBJ_DAILY_STAFFING_FILE = pbj_file

        rows = cms._load_provider_facilities()
    finally:
        cms.PROVIDER_INFO_FILE = orig_provider
        cms.MDS_QUALITY_FILE = orig_mds
        cms.PBJ_DAILY_STAFFING_FILE = orig_pbj

    assert len(rows) == 1
    assert rows[0]["occupancy_pct"] is None
