from main import _build_pipeline_diagnostics


def test_pipeline_diagnostics_flags_empty_tables_and_missing_runs():
    counts = {
        "census_tracts": 0,
        "schools": 0,
        "elder_care_facilities": 10,
        "housing_projects": 2,
        "hud_lihtc_tenant": 1,
        "hud_qct_dda": 1,
    }
    pipelines = {
        "census_acs": {"last_success": None, "freshness_status": "unknown", "last_failure": {"error_message": "timeout"}},
        "nces_pss": {"last_success": None, "freshness_status": "unknown", "last_failure": {"error_message": None}},
        "cms_elder_care": {"last_success": "2026-01-01T00:00:00+00:00", "freshness_status": "fresh", "last_failure": {"error_message": None}},
        "hud_lihtc_property": {"last_success": "2026-01-01T00:00:00+00:00", "freshness_status": "stale", "last_failure": {"error_message": None}},
    }

    diagnostics, ready = _build_pipeline_diagnostics(counts, pipelines)

    assert ready is False
    assert any("Table 'census_tracts' has 0 rows" in d for d in diagnostics)
    assert any("Table 'schools' has 0 rows" in d for d in diagnostics)
    assert any("Pipeline 'census_acs' has never completed successfully" in d for d in diagnostics)
    assert any("Pipeline 'hud_lihtc_property' is stale" in d for d in diagnostics)
    assert any("recent failure" in d for d in diagnostics)


def test_pipeline_diagnostics_ready_when_counts_and_runs_are_healthy():
    counts = {
        "census_tracts": 123,
        "schools": 456,
        "elder_care_facilities": 10,
        "housing_projects": 2,
        "hud_lihtc_tenant": 1,
        "hud_qct_dda": 1,
    }
    pipelines = {
        name: {
            "last_success": "2026-01-01T00:00:00+00:00",
            "freshness_status": "fresh",
            "last_failure": {"error_message": None},
        }
        for name in ["census_acs", "nces_pss", "cms_elder_care", "hud_lihtc_property", "hud_lihtc_tenant", "hud_qct_dda"]
    }

    diagnostics, ready = _build_pipeline_diagnostics(counts, pipelines)

    assert ready is True
    assert diagnostics == []


def test_pipeline_diagnostics_hud_not_ready_when_normalized_tables_empty():
    counts = {
        "census_tracts": 123,
        "schools": 456,
        "elder_care_facilities": 10,
        "housing_projects": 0,
        "hud_lihtc_property": 0,
        "hud_lihtc_tenant": 0,
        "hud_qct_dda": 0,
    }
    pipelines = {
        "census_acs": {"last_success": "2026-01-01T00:00:00+00:00", "freshness_status": "fresh", "last_failure": {"error_message": None}},
        "nces_pss": {"last_success": "2026-01-01T00:00:00+00:00", "freshness_status": "fresh", "last_failure": {"error_message": None}},
        "cms_elder_care": {"last_success": "2026-01-01T00:00:00+00:00", "freshness_status": "fresh", "last_failure": {"error_message": None}},
    }

    diagnostics, ready = _build_pipeline_diagnostics(counts, pipelines)
    assert ready is False
    assert any("hud_lihtc_property" in d for d in diagnostics)
    assert any("hud_lihtc_tenant" in d for d in diagnostics)
    assert any("hud_qct_dda" in d for d in diagnostics)


def test_pipeline_diagnostics_ready_when_hud_normalized_tables_populated_without_legacy_housing():
    counts = {
        "census_tracts": 123,
        "schools": 456,
        "elder_care_facilities": 10,
        "housing_projects": 0,
        "hud_lihtc_property": 12,
        "hud_lihtc_tenant": 8,
        "hud_qct_dda": 4,
    }
    pipelines = {
        name: {
            "last_success": "2026-01-01T00:00:00+00:00",
            "freshness_status": "fresh",
            "last_failure": {"error_message": None},
        }
        for name in ["census_acs", "nces_pss", "cms_elder_care", "hud_lihtc_property", "hud_lihtc_tenant", "hud_qct_dda"]
    }

    diagnostics, ready = _build_pipeline_diagnostics(counts, pipelines)

    assert ready is True
    assert diagnostics == []


def test_pipeline_diagnostics_tenant_and_qct_are_optional_for_readiness():
    counts = {
        "census_tracts": 123,
        "schools": 456,
        "elder_care_facilities": 10,
        "housing_projects": 0,
        "hud_lihtc_property": 12,
        "hud_lihtc_tenant": 0,
        "hud_qct_dda": 0,
    }
    pipelines = {
        name: {
            "last_success": "2026-01-01T00:00:00+00:00",
            "freshness_status": "fresh",
            "last_failure": {"error_message": None},
        }
        for name in ["census_acs", "nces_pss", "cms_elder_care", "hud_lihtc_property"]
    }

    diagnostics, ready = _build_pipeline_diagnostics(counts, pipelines)

    assert ready is True
    assert any("Optional enrichment table 'hud_lihtc_tenant' has 0 rows" in d for d in diagnostics)
    assert any("Optional enrichment table 'hud_qct_dda' has 0 rows" in d for d in diagnostics)
