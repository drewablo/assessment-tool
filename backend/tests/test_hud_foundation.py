import json
from pathlib import Path

import pytest

from pipeline.hud_contracts import validate_columns
from pipeline.ingest_hud_foundation import (
    _normalize_geoid,
    _parse_boundary_wkt,
    _sha256_file,
    determine_tenant_join_method,
)


def test_contract_validation_handles_aliases_for_property():
    cols = {"hud_id", "project_name", "latitude", "longitude"}
    errors = validate_columns("lihtc_property", cols)
    assert errors == []


def test_contract_validation_missing_required_field():
    cols = {"HUD_ID", "PROJECT", "LATITUDE"}
    errors = validate_columns("lihtc_property", cols)
    assert any("LONGITUDE" in err for err in errors)


def test_normalize_geoid():
    assert _normalize_geoid("17031010100") == "17031010100"
    assert _normalize_geoid("1703101010") == "01703101010"
    assert _normalize_geoid("abc") is None


def test_sha256_file(tmp_path: Path):
    target = tmp_path / "sample.json"
    target.write_text(json.dumps({"a": 1}))
    digest = _sha256_file(target)
    assert len(digest) == 64


def test_parse_boundary_wkt_valid_polygon():
    geom = _parse_boundary_wkt("POLYGON ((0 0, 1 0, 1 1, 0 0))", "QCT")
    assert geom is not None


def test_parse_boundary_wkt_invalid():
    with pytest.raises(ValueError):
        _parse_boundary_wkt("NOT_A_WKT", "QCT")


def test_determine_tenant_join_method_prefers_hud_id_exact():
    method, confidence = determine_tenant_join_method(
        hud_id="HUD123",
        geoid11="17031010100",
        property_keys={("HUD123", 2024)},
        dataset_year=2024,
    )
    assert method == "hud_id_exact"
    assert confidence == 1.0


def test_determine_tenant_join_method_tract_fallback_and_unmatched():
    method, confidence = determine_tenant_join_method(
        hud_id=None,
        geoid11="17031010100",
        property_keys=set(),
        dataset_year=2024,
    )
    assert method == "tract_exact"
    assert confidence == 0.7

    method2, confidence2 = determine_tenant_join_method(
        hud_id=None,
        geoid11=None,
        property_keys=set(),
        dataset_year=2024,
    )
    assert method2 == "unmatched"
    assert confidence2 == 0.0
