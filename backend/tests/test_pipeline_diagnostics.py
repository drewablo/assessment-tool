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

    diagnostics, ready, readiness_status = _build_pipeline_diagnostics(counts, pipelines)

    assert ready is False
    assert readiness_status == "not_ready"
    assert any("Table 'census_tracts' has 0 rows" in d for d in diagnostics)
    assert any("Table 'schools' has 0 rows" in d for d in diagnostics)
    # census_acs has no run AND its table is empty → blocking
    assert any("census_acs" in d and "empty" in d for d in diagnostics)
    assert any("stale" in d.lower() for d in diagnostics)
    assert any("recent failure" in d for d in diagnostics)


def test_pipeline_diagnostics_ready_when_counts_and_runs_are_healthy():
    counts = {
        "census_tracts": 123,
        "schools": 456,
        "elder_care_facilities": 10,
        "housing_projects": 2,
        "hud_lihtc_tenant": 1,
        "hud_qct_dda": 1,
        "hud_section_202": 5,
    }
    pipelines = {
        name: {
            "last_success": "2026-01-01T00:00:00+00:00",
            "freshness_status": "fresh",
            "last_failure": {"error_message": None},
        }
        for name in ["census_acs", "nces_pss", "cms_elder_care", "hud_lihtc_property", "hud_lihtc_tenant", "hud_qct_dda", "hud_section_202"]
    }

    diagnostics, ready, readiness_status = _build_pipeline_diagnostics(counts, pipelines)

    assert ready is True
    assert readiness_status == "ready"
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

    diagnostics, ready, readiness_status = _build_pipeline_diagnostics(counts, pipelines)
    assert ready is False
    assert readiness_status == "not_ready"
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
        "hud_section_202": 3,
    }
    pipelines = {
        name: {
            "last_success": "2026-01-01T00:00:00+00:00",
            "freshness_status": "fresh",
            "last_failure": {"error_message": None},
        }
        for name in ["census_acs", "nces_pss", "cms_elder_care", "hud_lihtc_property", "hud_lihtc_tenant", "hud_qct_dda", "hud_section_202"]
    }

    diagnostics, ready, readiness_status = _build_pipeline_diagnostics(counts, pipelines)

    assert ready is True
    assert readiness_status == "ready"
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
        "hud_section_202": 5,
    }
    pipelines = {
        name: {
            "last_success": "2026-01-01T00:00:00+00:00",
            "freshness_status": "fresh",
            "last_failure": {"error_message": None},
        }
        for name in ["census_acs", "nces_pss", "cms_elder_care", "hud_lihtc_property", "hud_section_202"]
    }

    diagnostics, ready, readiness_status = _build_pipeline_diagnostics(counts, pipelines)

    assert ready is True
    assert readiness_status == "ready_with_fallbacks"
    assert any("Optional enrichment table 'hud_lihtc_tenant' has 0 rows" in d for d in diagnostics)
    assert any("Optional enrichment table 'hud_qct_dda' has 0 rows" in d for d in diagnostics)


# --- New tests for data-present-but-no-pipeline-run scenario ---


def test_pipeline_diagnostics_ready_no_tracking_when_data_present_but_no_runs():
    """Tables have data but no pipeline runs recorded → ready_no_tracking, not blocked."""
    counts = {
        "census_tracts": 11227,
        "schools": 20923,
        "elder_care_facilities": 14710,
        "housing_projects": 0,
        "hud_lihtc_property": 51846,
        "hud_lihtc_tenant": 4804,
        "hud_qct_dda": 0,
        "hud_section_202": 100,
    }
    # All pipelines report no success
    pipelines = {
        name: {
            "last_success": None,
            "freshness_status": "unknown",
            "last_failure": {"error_message": None},
        }
        for name in ["census_acs", "nces_pss", "cms_elder_care", "hud_lihtc_property", "hud_lihtc_tenant", "hud_qct_dda"]
    }

    diagnostics, ready, readiness_status = _build_pipeline_diagnostics(counts, pipelines)

    # DB is READY — data exists in all required tables
    assert ready is True
    # But pipeline tracking is missing
    assert readiness_status == "ready_no_tracking"
    # Should NOT contain blocking diagnostics about REQUIRED tables
    assert not any("Table 'census_tracts' has 0 rows" in d for d in diagnostics)
    assert not any("Table 'schools' has 0 rows" in d for d in diagnostics)
    assert not any("Table 'elder_care_facilities' has 0 rows" in d for d in diagnostics)
    # Should contain tracking warnings for required pipelines
    assert any("no recorded successful run" in d and "census_acs" in d for d in diagnostics)
    assert any("no recorded successful run" in d and "cms_elder_care" in d for d in diagnostics)
    assert any("no recorded successful run" in d and "hud_lihtc_property" in d for d in diagnostics)


def test_pipeline_diagnostics_not_ready_when_required_table_empty_regardless_of_pipeline_run():
    """Even if a pipeline has run, if its table is empty, it's not ready."""
    counts = {
        "census_tracts": 0,
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

    diagnostics, ready, readiness_status = _build_pipeline_diagnostics(counts, pipelines)

    assert ready is False
    assert readiness_status == "not_ready"
    assert any("Table 'census_tracts' has 0 rows" in d for d in diagnostics)


def test_pipeline_diagnostics_stale_pipeline_is_warning_not_blocking():
    """A stale pipeline should produce a warning but not block readiness."""
    counts = {
        "census_tracts": 123,
        "schools": 456,
        "elder_care_facilities": 10,
        "housing_projects": 0,
        "hud_lihtc_property": 12,
        "hud_lihtc_tenant": 0,
        "hud_qct_dda": 0,
        "hud_section_202": 5,
    }
    pipelines = {
        "census_acs": {"last_success": "2025-01-01T00:00:00+00:00", "freshness_status": "stale", "last_failure": {"error_message": None}},
        "nces_pss": {"last_success": "2026-01-01T00:00:00+00:00", "freshness_status": "fresh", "last_failure": {"error_message": None}},
        "cms_elder_care": {"last_success": "2026-01-01T00:00:00+00:00", "freshness_status": "fresh", "last_failure": {"error_message": None}},
        "hud_lihtc_property": {"last_success": "2026-01-01T00:00:00+00:00", "freshness_status": "fresh", "last_failure": {"error_message": None}},
    }

    diagnostics, ready, readiness_status = _build_pipeline_diagnostics(counts, pipelines)

    assert ready is True
    assert any("stale" in d.lower() for d in diagnostics)


def test_pipeline_diagnostics_qct_empty_does_not_block_when_lihtc_property_has_data():
    """Missing QCT/DDA (optional enrichment) should not block when LIHTC property data exists."""
    counts = {
        "census_tracts": 123,
        "schools": 456,
        "elder_care_facilities": 10,
        "housing_projects": 0,
        "hud_lihtc_property": 51846,
        "hud_lihtc_tenant": 4804,
        "hud_qct_dda": 0,
        "hud_section_202": 5,
    }
    pipelines = {
        name: {
            "last_success": "2026-01-01T00:00:00+00:00",
            "freshness_status": "fresh",
            "last_failure": {"error_message": None},
        }
        for name in ["census_acs", "nces_pss", "cms_elder_care", "hud_lihtc_property", "hud_section_202"]
    }

    diagnostics, ready, readiness_status = _build_pipeline_diagnostics(counts, pipelines)

    assert ready is True
    # Optional enrichment warning present
    assert any("hud_qct_dda" in d for d in diagnostics)
    # But not blocking
    assert readiness_status == "ready_with_fallbacks"


def test_pipeline_diagnostics_not_ready_when_section202_empty():
    """Matches production scenario: all other required data present, but
    hud_section_202 is empty (never completed) — now blocks readiness."""
    counts = {
        "census_tracts": 84415,
        "schools": 20923,
        "elder_care_facilities": 14710,
        "housing_projects": 0,
        "hud_lihtc_property": 51846,
        "hud_lihtc_tenant": 28824,
        "hud_qct_dda": 7334,
        "hud_section_202": 0,
    }
    pipelines = {
        "census_acs": {"last_success": "2026-03-01T00:00:00+00:00", "freshness_status": "fresh", "last_failure": {"error_message": None}},
        "nces_pss": {"last_success": "2026-03-01T00:00:00+00:00", "freshness_status": "fresh", "last_failure": {"error_message": None}},
        "cms_elder_care": {"last_success": "2026-03-01T00:00:00+00:00", "freshness_status": "fresh", "last_failure": {"error_message": None}},
        "hud_lihtc_property": {"last_success": "2026-03-01T00:00:00+00:00", "freshness_status": "fresh", "last_failure": {"error_message": None}},
        "hud_lihtc_tenant": {"last_success": "2026-03-01T00:00:00+00:00", "freshness_status": "fresh", "last_failure": {"error_message": None}},
        "hud_qct_dda": {
            "last_success": "2026-03-10T00:00:00+00:00",
            "freshness_status": "fresh",
            "last_failure": {
                "finished_at": "2026-03-05T00:00:00+00:00",
                "error_message": "CardinalityViolationError: ON CONFLICT DO UPDATE command cannot affect row a second time",
            },
        },
        "hud_section_202": {"last_success": None, "freshness_status": "unknown", "last_failure": {"error_message": None}},
    }

    diagnostics, ready, readiness_status = _build_pipeline_diagnostics(counts, pipelines)

    # Section 202 is now required — empty table blocks readiness
    assert ready is False
    assert readiness_status == "not_ready"
    # Section 202 blocking diagnostic
    assert any("hud_section_202" in d and "0 rows" in d for d in diagnostics)
    # Section 202 never completed → informational diagnostic
    assert any("hud_section_202" in d and "never completed" in d for d in diagnostics)
    # QCT failure is RESOLVED (success at 03-10 > failure at 03-05) — should NOT appear
    assert not any("hud_qct_dda" in d and "recent failure" in d for d in diagnostics)


def test_pipeline_diagnostics_surfaces_failure_more_recent_than_success():
    """A failure that occurred AFTER the last success should be surfaced."""
    counts = {
        "census_tracts": 123,
        "schools": 456,
        "elder_care_facilities": 10,
        "housing_projects": 0,
        "hud_lihtc_property": 12,
        "hud_lihtc_tenant": 8,
        "hud_qct_dda": 4,
        "hud_section_202": 3,
    }
    pipelines = {
        name: {
            "last_success": "2026-03-01T00:00:00+00:00",
            "freshness_status": "fresh",
            "last_failure": {"error_message": None},
        }
        for name in ["census_acs", "nces_pss", "cms_elder_care", "hud_lihtc_property", "hud_lihtc_tenant", "hud_section_202"]
    }
    # QCT has a failure MORE RECENT than its last success
    pipelines["hud_qct_dda"] = {
        "last_success": "2026-03-01T00:00:00+00:00",
        "freshness_status": "fresh",
        "last_failure": {
            "finished_at": "2026-03-10T00:00:00+00:00",
            "error_message": "SomeError: something broke",
        },
    }

    diagnostics, ready, readiness_status = _build_pipeline_diagnostics(counts, pipelines)

    assert ready is True
    # Failure is more recent than success — should be surfaced
    assert any("hud_qct_dda" in d and "recent failure" in d for d in diagnostics)
