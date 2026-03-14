import hashlib
import io
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from sqlalchemy.engine import make_url

load_dotenv()

logger = logging.getLogger(__name__)

from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

from models.schemas import (
    AnalysisRequest,
    AnalysisResponse,
    BenchmarkNarrative,
    BoardActionRoadmap,
    BoardReportPack,
    CompareAnalysisRequest,
    CompareAnalysisResponse,
    CompareAnalysisSummary,
    DataFreshnessMetadata,
    DataFreshnessSource,
    DataDependencyStatus,
    DecisionPathwayRecommendation,
    ExportReadiness,
    ConfidenceSummary,
    FallbackSummary,
    SectionExplanation,
    PartnerPathAssessment,
    PortfolioWorkspaceCreateRequest,
    PortfolioWorkspaceResponse,
    PortfolioWorkspaceUpdateRequest,
    PortfolioCompareSnapshot,
    SchoolAuditExtractionResponse,
)
from api.geocoder import GeocoderServiceError, geocode_address
from api.census import get_demographics
from api.reports import generate_csv_report
from modules import MODULE_REGISTRY
from api.isochrone import (
    get_isochrone,
    isochrone_effective_radius_miles,
    GRADE_LEVEL_FALLBACK_RADIUS,
)
from api.school_stage2 import dedupe_year_rows, extract_audit_financials
from services.dependency_policy import resolve_run_mode, summarize_dependencies, strict_mode_blockers
from services.analysis_snapshot import snapshot_key, freeze_snapshot, thaw_snapshot

# Database integration (v2)
USE_DB = os.getenv("USE_DB", "").lower() in ("1", "true", "yes")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CACHE_TTL = int(os.getenv("ANALYSIS_CACHE_TTL", "86400"))  # 24 hours default
DEFAULT_RUN_MODE = os.getenv("ANALYSIS_RUN_MODE", "db_with_fallback")
SNAPSHOT_TTL = int(os.getenv("ANALYSIS_SNAPSHOT_TTL", "86400"))

# Global Redis connection state
_redis = None
_redis_retry_after: datetime | None = None
_REDIS_BACKOFF_SECONDS = int(os.getenv("REDIS_RETRY_BACKOFF_SECONDS", "30"))

ISOCHRONE_CACHE_TTL_HOURS = int(os.getenv("ISOCHRONE_CACHE_TTL_HOURS", "168"))



PIPELINE_FRESHNESS_TARGET_HOURS = {
    "census_acs": 24 * 45,
    "nces_pss": 24 * 45,
    "cms_elder_care": 24 * 14,
    "hud_lihtc_property": 24 * 30,
    "hud_lihtc_tenant": 24 * 30,
    "hud_qct_dda": 24 * 30,
    "hud_section_202": 24 * 30,
}



def _build_pipeline_diagnostics(counts: dict, pipelines: dict) -> tuple[list[str], bool, str]:
    """Build actionable DB-readiness diagnostics for operators.

    Readiness is determined by DATA PRESENCE in required tables, not pipeline
    run history.  Pipeline tracking provides freshness/provenance signals but
    missing runs must never block readiness when the underlying data exists.

    Readiness levels:
      ready               – all required data present, pipelines tracked
      ready_with_fallbacks – required baseline data present, optional enrichments missing
      ready_no_tracking   – required data present but pipeline provenance unavailable
      not_ready           – one or more required tables are empty
    """
    diagnostics: list[str] = []
    blocking_diagnostics: list[str] = []
    tracking_warnings: list[str] = []

    # --- Which pipelines map to which tables ---
    pipeline_to_table = {
        "census_acs": "census_tracts",
        "nces_pss": "schools",
        "cms_elder_care": "elder_care_facilities",
        "hud_lihtc_property": "hud_lihtc_property",
        "hud_section_202": "hud_section_202",
    }
    required_pipelines = {"census_acs", "nces_pss", "cms_elder_care", "hud_lihtc_property"}
    optional_pipelines = {"hud_lihtc_tenant", "hud_qct_dda", "hud_section_202"}

    # --- Check required table data presence (the real readiness gate) ---
    table_expectations = {
        "census_tracts": "census_acs",
        "schools": "nces_pss",
        "elder_care_facilities": "cms_elder_care",
    }
    for table, pipeline_name in table_expectations.items():
        if int(counts.get(table) or 0) == 0:
            blocking_diagnostics.append(
                f"Table '{table}' has 0 rows. Run or re-run pipeline '{pipeline_name}'."
            )

    housing_legacy = int(counts.get("housing_projects") or 0)
    housing_normalized = int(counts.get("hud_lihtc_property") or 0)
    tenant_rows = int(counts.get("hud_lihtc_tenant") or 0)
    qct_rows = int(counts.get("hud_qct_dda") or 0)
    if housing_legacy == 0 and housing_normalized == 0:
        blocking_diagnostics.append(
            "Both 'competitors_housing' and 'hud_lihtc_property' have 0 rows. Run HUD LIHTC property ingest."
        )
    if tenant_rows == 0:
        diagnostics.append(
            "Optional enrichment table 'hud_lihtc_tenant' has 0 rows. "
            "Housing baseline remains available; tenant-derived context will be skipped."
        )
    if qct_rows == 0:
        diagnostics.append(
            "Optional enrichment table 'hud_qct_dda' has 0 rows. "
            "Housing baseline remains available; QCT/DDA policy context will be skipped."
        )
    section_202_rows = int(counts.get("hud_section_202") or 0)
    if section_202_rows == 0:
        diagnostics.append(
            "Optional enrichment table 'hud_section_202_properties' has 0 rows. "
            "Senior Housing analysis will proceed without HUD Section 202 competitor data."
        )

    # --- Check pipeline run history (freshness/provenance, not blocking) ---
    for name in sorted(required_pipelines | optional_pipelines):
        info = pipelines.get(name) or {}
        freshness = info.get("freshness_status")
        last_success = info.get("last_success")
        last_failure_info = info.get("last_failure") or {}
        last_failure = last_failure_info.get("error_message")
        last_failure_at = last_failure_info.get("finished_at")

        # Resolve whether the *table* backing this pipeline has data.
        backing_table = pipeline_to_table.get(name)
        table_has_data = int(counts.get(backing_table or "", 0) or 0) > 0

        if not last_success:
            if table_has_data:
                # Data exists but no pipeline run recorded — a tracking gap, not
                # a readiness problem.  Do NOT add to blocking_diagnostics.
                tracking_warnings.append(
                    f"Pipeline '{name}' has no recorded successful run, but its "
                    f"backing table has data. Pipeline tracking should be re-run "
                    f"or the run log back-filled."
                )
            else:
                # No data AND no pipeline run — surface on the appropriate list.
                target = blocking_diagnostics if name in required_pipelines else diagnostics
                target.append(f"Pipeline '{name}' has never completed successfully and its table is empty.")
        elif freshness == "stale":
            msg = f"Pipeline '{name}' is stale (freshness_status=stale). Trigger a refresh run."
            if name in required_pipelines:
                diagnostics.append(msg)  # stale is a freshness concern, not blocking
            else:
                diagnostics.append(msg)

        if last_failure:
            # Only surface the failure if it is more recent than the last
            # success, or if there has never been a successful run.  A
            # failure that predates a subsequent success is resolved.
            failure_is_recent = True
            if last_success and last_failure_at:
                failure_is_recent = last_failure_at > last_success
            if failure_is_recent:
                diagnostics.append(
                    f"Pipeline '{name}' has a recent failure: {str(last_failure)[:200]}"
                )

    all_diagnostics = [*blocking_diagnostics, *tracking_warnings, *diagnostics]
    db_ready_for_analysis = len(blocking_diagnostics) == 0

    if not db_ready_for_analysis:
        readiness_status = "not_ready"
    elif tracking_warnings:
        readiness_status = "ready_no_tracking"
    elif diagnostics:
        readiness_status = "ready_with_fallbacks"
    else:
        readiness_status = "ready"

    return all_diagnostics, db_ready_for_analysis, readiness_status



async def _collect_db_data_health() -> dict:
    """Collect lightweight DB readiness diagnostics for DB-backed analysis surfaces."""
    if not USE_DB:
        return {"use_db": False, "db_connected": False, "warnings": ["USE_DB is disabled."]}

    from db.connection import DATABASE_URL, engine, async_session_factory
    from sqlalchemy import select, func, text
    from db.models import (
        CensusTract,
        CompetitorSchoolRecord,
        CompetitorElderCare,
        CompetitorHousing,
        HudLihtcProperty,
        HudLihtcTenant,
        HudQctDdaDesignation,
        HudSection202Property,
    )

    parsed = make_url(DATABASE_URL)
    db_target = {
        "driver": parsed.drivername,
        "host": parsed.host,
        "port": parsed.port,
        "database": parsed.database,
    }

    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as exc:
        return {
            "use_db": True,
            "db_connected": False,
            "db_target": db_target,
            "warnings": [f"DB connection failed: {exc.__class__.__name__}"],
        }

    async with async_session_factory() as session:
        counts = {}
        for model, key in [
            (CensusTract, "census_tracts"),
            (CompetitorSchoolRecord, "competitors_schools"),
            (CompetitorElderCare, "competitors_elder_care"),
            (CompetitorHousing, "competitors_housing"),
            (HudLihtcProperty, "hud_lihtc_property"),
            (HudLihtcTenant, "hud_lihtc_tenant"),
            (HudQctDdaDesignation, "hud_qct_dda"),
            (HudSection202Property, "hud_section_202"),
        ]:
            counts[key] = int((await session.execute(select(func.count()).select_from(model))).scalar() or 0)

        null_centroid = int((await session.execute(select(func.count()).select_from(CensusTract).where(CensusTract.centroid.is_(None)))).scalar() or 0)
        null_boundary = int((await session.execute(select(func.count()).select_from(CensusTract).where(CensusTract.boundary.is_(None)))).scalar() or 0)
        centroid_ready = int((await session.execute(select(func.count()).select_from(CensusTract).where(func.coalesce(CensusTract.centroid, func.ST_PointOnSurface(CensusTract.boundary)).is_not(None)))).scalar() or 0)
        hud_years = list((await session.execute(select(HudLihtcProperty.dataset_year).distinct().order_by(HudLihtcProperty.dataset_year.desc()).limit(5))).scalars().all())

        # State coverage: which states have census tracts ingested
        state_rows = (await session.execute(
            select(CensusTract.state_fips, func.count()).group_by(CensusTract.state_fips).order_by(CensusTract.state_fips)
        )).all()
        ingested_states = {row[0]: row[1] for row in state_rows}

    # All 50 states + DC
    ALL_STATE_FIPS = {
        "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
        "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
        "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
        "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
        "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
        "56",
    }
    missing_states = sorted(ALL_STATE_FIPS - set(ingested_states.keys()))

    warnings = []
    if counts["census_tracts"] == 0:
        warnings.append("census_tracts is empty; demographics will fall back to live Census API.")
    elif centroid_ready == 0:
        warnings.append("census_tracts has no centroid-ready geometry (centroid/boundary); catchment queries will fail to use DB path.")
    elif null_centroid == counts["census_tracts"]:
        warnings.append("all census tracts have NULL centroid geometry; boundary-derived points will be used and centroids should be backfilled.")

    if missing_states and counts["census_tracts"] > 0:
        warnings.append(
            f"Census data missing for {len(missing_states)} state(s): {', '.join(missing_states)}. "
            f"Analyses in these states will fall back to live Census API. "
            f"Run: ingest_acs_data(states={missing_states[:5]}{'...' if len(missing_states) > 5 else ''})"
        )

    if counts["hud_lihtc_property"] == 0 and counts["competitors_housing"] > 0:
        warnings.append("normalized HUD tables are empty but legacy competitors_housing has rows; housing will use legacy DB path without HUD enrichment.")
    elif counts["hud_lihtc_property"] == 0:
        warnings.append("HUD housing tables are empty; housing module will fall back to CSV loader.")

    for table in ("competitors_schools", "competitors_elder_care"):
        if counts[table] == 0:
            warnings.append(f"{table} is empty; module will fall back to live provider.")

    if counts.get("hud_section_202", 0) == 0:
        warnings.append(
            "hud_section_202_properties is empty; Senior Housing analysis will not include "
            "HUD Section 202 competitor data. Run: ingest-hud-section202"
        )

    return {
        "use_db": True,
        "db_connected": True,
        "db_target": db_target,
        "counts": counts,
        "null_centroid_rows": null_centroid,
        "null_boundary_rows": null_boundary,
        "centroid_ready_rows": centroid_ready,
        "hud_dataset_years": hud_years,
        "census_state_coverage": {
            "ingested_count": len(ingested_states),
            "total_expected": len(ALL_STATE_FIPS),
            "missing_fips": missing_states,
            "ingested_fips": sorted(ingested_states.keys()),
        },
        "warnings": warnings,
        "db_ready_for_analysis": len(warnings) == 0,
    }

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _freshness_status(age_hours: float | None, target_hours: float | None) -> str:
    if age_hours is None or target_hours is None:
        return "unknown"
    if age_hours <= target_hours:
        return "fresh"
    if age_hours <= target_hours * 2:
        return "aging"
    return "stale"


async def _build_data_freshness_metadata() -> DataFreshnessMetadata:
    generated_at = _utcnow_iso()
    source_labels = {
        "census_acs": "US Census ACS",
        "nces_pss": "NCES Private School Survey",
        "cms_elder_care": "CMS Care Compare",
        "hud_lihtc_property": "HUD LIHTC Property",
        "hud_lihtc_tenant": "HUD LIHTC Tenant",
        "hud_qct_dda": "HUD QCT/DDA",
    }

    if not USE_DB:
        return DataFreshnessMetadata(
            mode="live",
            generated_at_utc=generated_at,
            sources=[
                DataFreshnessSource(
                    source_key="live_api",
                    source_label="Live API calls",
                    status="unknown",
                    notes="Live-fetch mode; freshness depends on upstream provider response and release cadence.",
                )
            ],
        )

    from db.connection import get_session
    from db.models import HudIngestRun, PipelineRun
    from sqlalchemy import select

    sources: list[DataFreshnessSource] = []
    async with get_session() as session:
        for key, label in source_labels.items():
            run_stmt = (
                select(PipelineRun)
                .where(PipelineRun.pipeline_name == key, PipelineRun.status == "success")
                .order_by(PipelineRun.finished_at.desc())
                .limit(1)
            )
            run = (await session.execute(run_stmt)).scalar_one_or_none()
            if not run or not run.finished_at:
                sources.append(
                    DataFreshnessSource(
                        source_key=key,
                        source_label=label,
                        status="unknown",
                        notes="No successful refresh run found.",
                    )
                )
                continue

            finished = run.finished_at
            if finished.tzinfo is None:
                finished = finished.replace(tzinfo=timezone.utc)
            age_hours = round((datetime.now(timezone.utc) - finished).total_seconds() / 3600, 1)
            status = _freshness_status(age_hours, PIPELINE_FRESHNESS_TARGET_HOURS.get(key))

            sources.append(
                DataFreshnessSource(
                    source_key=key,
                    source_label=label,
                    last_refreshed_utc=finished.replace(microsecond=0).isoformat(),
                    freshness_hours=age_hours,
                    status=status,
                )
            )

        hud_source_labels = {
            "lihtc_property": "HUD LIHTC Property (normalized)",
            "lihtc_tenant": "HUD LIHTC Tenant (normalized)",
            "qct_dda": "HUD QCT/DDA (normalized)",
        }
        for source_family, label in hud_source_labels.items():
            stmt = (
                select(HudIngestRun)
                .where(HudIngestRun.source_family == source_family, HudIngestRun.status == "success")
                .order_by(HudIngestRun.finished_at.desc())
                .limit(1)
            )
            run = (await session.execute(stmt)).scalar_one_or_none()
            if not run or not run.finished_at:
                sources.append(
                    DataFreshnessSource(
                        source_key=f"hud_{source_family}",
                        source_label=label,
                        status="unknown",
                        notes="No successful normalized HUD ingest run found.",
                    )
                )
                continue

            finished = run.finished_at
            if finished.tzinfo is None:
                finished = finished.replace(tzinfo=timezone.utc)
            age_hours = round((datetime.now(timezone.utc) - finished).total_seconds() / 3600, 1)
            status = _freshness_status(age_hours, 24 * 365)
            sources.append(
                DataFreshnessSource(
                    source_key=f"hud_{source_family}",
                    source_label=label,
                    last_refreshed_utc=finished.replace(microsecond=0).isoformat(),
                    freshness_hours=age_hours,
                    status=status,
                    notes=f"dataset_year={run.dataset_year}; source_version={run.source_version or 'unknown'}",
                )
            )

    return DataFreshnessMetadata(mode="db_precomputed", generated_at_utc=generated_at, sources=sources)


def _apply_reliability_metadata(result: AnalysisResponse, *, run_mode: str, dependency_counts: dict | None, fallback_notes: list[str], strict_blockers: list[str]) -> AnalysisResponse:
    result.run_mode = run_mode
    result.catchment_mode = result.catchment_type
    result.data_dependencies = summarize_dependencies(dependency_counts)
    result.fallback_summary = FallbackSummary(
        used=bool(fallback_notes),
        notes=fallback_notes,
    )

    confidence_level = (result.demographics.data_confidence or "medium").lower()
    contributors = [
        f"demographics_confidence={confidence_level}",
        f"stage2_readiness={(result.feasibility_score.stage2.readiness if result.feasibility_score.stage2 else 'not_ready')}",
        f"fallback_used={bool(fallback_notes)}",
    ]
    if strict_blockers:
        confidence_level = "low"
        contributors.append("strict_mode_blockers_present")
    result.confidence_summary = ConfidenceSummary(
        level=confidence_level if confidence_level in {"high", "medium", "low"} else "medium",
        contributors=contributors,
    )
    confidence_level_final = result.confidence_summary.level

    export_reasons = [row.dataset for row in result.data_dependencies if row.export_blocking_in_strict and not row.available]
    ready = len(export_reasons) == 0 and (confidence_level_final != "low")
    status = "ready" if ready else ("blocked" if run_mode == "db_strict" else "warning")
    reason_text = []
    if export_reasons:
        reason_text.append(f"Missing export-critical datasets: {', '.join(export_reasons)}")
    if confidence_level_final == "low":
        reason_text.append("Overall confidence is low; board-ready packaging should be treated as directional.")
    result.export_readiness = ExportReadiness(ready=ready, status=status, reasons=reason_text)

    missing = [row.dataset for row in result.data_dependencies if not row.available]
    result.section_explanations = [
        SectionExplanation(
            section="catchment",
            inputs_used=["isochrone" if result.catchment_mode == "isochrone" else "radius"],
            inputs_missing=[],
            fallback_used=["radius fallback"] if result.catchment_mode == "radius" else [],
            confidence_impact="medium" if result.catchment_mode == "radius" else "low",
        ),
        SectionExplanation(
            section="demographics_and_competition",
            inputs_used=["census", "module_competitors"],
            inputs_missing=missing,
            fallback_used=fallback_notes,
            confidence_impact="high" if fallback_notes else "low",
        ),
    ]

    if strict_blockers:
        result.outcome = "strict_mode_blocked"
    elif fallback_notes:
        result.outcome = "degraded_success"
    else:
        result.outcome = "success"
    return result


async def _enrich_analysis_result(result: AnalysisResponse, request: AnalysisRequest) -> AnalysisResponse:
    if result.decision_pathway is None:
        result.decision_pathway = _phase1_decision_pathway(result, request)
    if result.benchmark_narrative is None:
        result.benchmark_narrative = _build_benchmark_narrative(result)
    if result.data_freshness is None:
        result.data_freshness = await _build_data_freshness_metadata()
    if result.board_report_pack is None:
        result.board_report_pack = _build_board_report_pack(result)
    return result


def _build_benchmark_narrative(result: AnalysisResponse) -> BenchmarkNarrative | None:
    benchmarks = result.feasibility_score.benchmarks
    if not benchmarks:
        return None

    state_pct = benchmarks.percentile_state
    national_pct = benchmarks.percentile_national
    cohort = f"{result.ministry_type.replace('_', ' ').title()} peer markets ({result.state_name or 'US context'})"

    comparable = []
    for row in (benchmarks.comparable_markets or [])[:3]:
        geoid = row.get("geoid")
        score = row.get("overall_score")
        if geoid and score is not None:
            comparable.append(f"Tract {geoid}: score {score}/100")

    percentile_parts = []
    if state_pct is not None:
        percentile_parts.append(f"{state_pct:.1f}th percentile in-state")
    if national_pct is not None:
        percentile_parts.append(f"{national_pct:.1f}th percentile nationally")
    if not percentile_parts:
        percentile_parts.append("benchmark percentile data is currently limited")

    summary = f"This location sits at {', '.join(percentile_parts)} against the {cohort}."
    if comparable:
        summary += f" Nearest comparable markets: {', '.join(comparable)}."

    return BenchmarkNarrative(
        peer_cohort=cohort,
        in_state_percentile=state_pct,
        national_percentile=national_pct,
        nearest_comparable_markets=comparable,
        narrative_summary=summary,
    )


def _build_board_report_pack(result: AnalysisResponse) -> BoardReportPack:
    pathway = result.decision_pathway
    recommended = pathway.recommended_pathway if pathway else "review"
    risks = list((pathway.top_risks if pathway else [])[:3])
    if not risks:
        risks = ["Insufficient risk inputs captured in this run; complete local diligence."]

    benchmark_text = result.benchmark_narrative.narrative_summary if result.benchmark_narrative else "Benchmark narrative unavailable."
    executive_summary = (
        f"Recommended pathway: {recommended}. Overall feasibility score: {result.feasibility_score.overall}/100. "
        f"{benchmark_text}"
    )

    options = [
        "Continue current model with targeted operating improvements",
        "Transform service mix to match local demand and affordability",
        "Pursue partner-led operating model with mission safeguards",
        "Plan structured closure/repurposing if sustainability cannot be achieved",
    ]

    immediate_actions = (pathway.next_12_month_actions if pathway else [])[:3] or [
        "Validate assumptions with local leadership and governing board.",
        "Run downside and upside scenarios before board vote.",
        "Define accountable owners for 12-month milestones.",
    ]

    stage2 = result.feasibility_score.stage2
    confidence_notes = [
        f"Data confidence: {result.demographics.data_confidence or 'unknown'}.",
        f"Stage 2 readiness: {stage2.readiness if stage2 else 'not_ready'}.",
        f"Scenario range: {result.feasibility_score.scenario_conservative}–{result.feasibility_score.scenario_optimistic}.",
    ]

    methodology = [
        "Score combines market size, income fit, competition, and density factors (0-100 scale).",
        f"Weight profile: {result.feasibility_score.weighting_profile}.",
        f"Stage 2 formula version: {(stage2.formula_version if stage2 else 'n/a')}.",
        f"Benchmark sample sizes — state: {(result.feasibility_score.benchmarks.sample_size_state if result.feasibility_score.benchmarks else 'n/a')}, national: {(result.feasibility_score.benchmarks.sample_size_national if result.feasibility_score.benchmarks else 'n/a')}.",
    ]

    return BoardReportPack(
        executive_summary=executive_summary,
        key_risks=risks,
        strategic_options=options,
        immediate_next_actions=immediate_actions,
        action_roadmap=BoardActionRoadmap(
            months_12=immediate_actions,
            months_24=[
                "Track KPI trend vs baseline and recalibrate operating model.",
                "Confirm governance fit and capital plan before expansion commitments.",
            ],
            months_36=[
                "Evaluate mission outcomes and long-term sustainability against board thresholds.",
                "Decide scale-up, partnership deepening, or orderly transition.",
            ],
        ),
        methodology_assumptions=methodology,
        confidence_notes=confidence_notes,
    )


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _request_from_compare(request: CompareAnalysisRequest, ministry_type: str) -> AnalysisRequest:
    return AnalysisRequest(
        school_name=request.school_name,
        address=request.address,
        ministry_type=ministry_type,
        mission_mode=request.mission_mode,
        drive_minutes=request.drive_minutes,
        gender=request.gender,
        grade_level=request.grade_level,
        weighting_profile=request.weighting_profile,
        market_context=request.market_context,
        care_level=request.care_level,
        housing_target_population=request.housing_target_population,
        min_mds_overall_rating=request.min_mds_overall_rating,
        stage2_inputs=request.stage2_inputs,
        facility_profile=request.facility_profile,
    )


async def _get_redis():
    global _redis, _redis_retry_after
    if _redis is not None:
        return _redis

    if _redis_retry_after and _now_utc() < _redis_retry_after:
        return None

    try:
        import redis.asyncio as aioredis
        client = aioredis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=2)
        await client.ping()
        _redis = client
        _redis_retry_after = None
        logger.info("Redis cache connected at %s", REDIS_URL)
        return _redis
    except Exception:
        _redis = None
        _redis_retry_after = _now_utc() + timedelta(seconds=_REDIS_BACKOFF_SECONDS)
        logger.warning("Redis unavailable — response caching temporarily disabled; retry in %ss", _REDIS_BACKOFF_SECONDS, exc_info=True)
        return None


def _mark_redis_failure():
    global _redis, _redis_retry_after
    _redis = None
    _redis_retry_after = _now_utc() + timedelta(seconds=_REDIS_BACKOFF_SECONDS)



def _cache_key(request: AnalysisRequest) -> str:
    """Stable cache key from analysis request fields that affect output."""
    payload = json.dumps({
        "ministry_type": request.ministry_type,
        "address": request.address.strip().lower(),
        "drive_minutes": request.drive_minutes,
        "grade_level": request.grade_level,
        "gender": request.gender,
        "weighting_profile": request.weighting_profile,
        "market_context": request.market_context,
        "mission_mode": request.mission_mode,
    }, sort_keys=True)
    digest = hashlib.sha256(payload.encode()).hexdigest()[:16]
    return f"analysis:{request.ministry_type}:{digest}"


def _runner_up_pathways(primary: str) -> list[str]:
    order = ["continue", "transform", "partner", "close"]
    return [p for p in order if p != primary][:2]


def _pathway_confidence(data_confidence: str | None, stage2_readiness: str | None) -> str:
    base = {"high": 3, "medium": 2, "low": 1}.get((data_confidence or "").lower(), 1)
    readiness_adj = {"ready": 1, "partial": 0, "not_ready": -1}.get((stage2_readiness or "").lower(), -1)
    value = base + readiness_adj
    if value >= 4:
        return "high"
    if value >= 2:
        return "medium"
    return "low"


def _pathway_for_score(overall: int) -> str:
    if overall >= 75:
        return "continue"
    if overall >= 55:
        return "transform"
    if overall >= 40:
        return "partner"
    return "close"


def _facility_feasibility_adjustments(request: AnalysisRequest) -> tuple[int, list[str], list[str]]:
    profile = request.facility_profile
    if not profile:
        return 0, [], []

    adjustment = 0
    risks: list[str] = []
    validations: list[str] = []

    if profile.building_square_footage is not None and profile.building_square_footage < 10000:
        adjustment -= 5
        risks.append("Facility square footage may constrain target service model capacity.")

    if profile.deferred_maintenance_estimate is not None and profile.deferred_maintenance_estimate >= 1_000_000:
        adjustment -= 8
        risks.append("Deferred maintenance exposure may require significant upfront capital.")

    if profile.sponsor_operator_capacity == "low":
        adjustment -= 7
        risks.append("Sponsor/operator capacity is low for the selected pathway.")
    elif profile.sponsor_operator_capacity == "high":
        adjustment += 3

    if profile.zoning_use_constraints:
        adjustment -= 4
        risks.append("Zoning/use constraints may delay approvals or narrow program options.")
        validations.append("Confirm zoning and permitted-use pathway with local counsel and planning office.")

    if profile.accessibility_constraints:
        adjustment -= 3
        risks.append("Accessibility gaps may increase compliance retrofit scope.")
        validations.append("Commission accessibility and life-safety assessment before final pathway selection.")

    return adjustment, risks[:2], validations[:2]


def _partner_path_assessment(pathway: str, request: AnalysisRequest) -> PartnerPathAssessment | None:
    if pathway != "partner":
        return None

    profile = request.facility_profile
    mission_score = 72
    if profile:
        if profile.sponsor_operator_capacity == "low":
            mission_score = 78
        elif profile.sponsor_operator_capacity == "high":
            mission_score = 66

    checklist = [
        "Identify 3-5 mission-aligned operators with regional execution track record.",
        "Define reserved powers, mission protections, and escalation clauses.",
        "Draft transition and communications plan for staff, families, and community.",
    ]
    if profile and profile.zoning_use_constraints:
        checklist.append("Validate that partner operating model fits current zoning/use conditions.")

    return PartnerPathAssessment(
        mission_alignment_score=mission_score,
        governance_model_options=[
            "Management agreement with sponsor retained ownership",
            "Joint venture with reserved mission governance rights",
            "Lease-to-operate model with performance covenants",
        ],
        risk_transfer_profile="moderate" if mission_score >= 70 else "low",
        partnership_readiness_checklist=checklist,
    )


def _phase1_decision_pathway(result: AnalysisResponse, request: AnalysisRequest | None = None) -> DecisionPathwayRecommendation:
    overall = result.feasibility_score.overall
    stage2 = result.feasibility_score.stage2
    facility_adjustment = 0
    facility_risks: list[str] = []
    facility_validations: list[str] = []

    if request is not None:
        facility_adjustment, facility_risks, facility_validations = _facility_feasibility_adjustments(request)

    adjusted_overall = max(0, min(100, overall + facility_adjustment))
    pathway = _pathway_for_score(adjusted_overall)
    stage2_readiness = stage2.readiness if stage2 else "not_ready"
    confidence = _pathway_confidence(result.demographics.data_confidence, stage2_readiness)

    risks: list[str] = []
    if confidence == "low":
        risks.append("Low-confidence data may change pathway ranking after local validation.")
    if not stage2 or stage2.readiness == "not_ready":
        risks.append("Stage 2 institutional economics data is incomplete.")
    elif stage2.readiness == "partial":
        risks.append("Stage 2 score is partial; missing inputs can materially change the result.")
    if adjusted_overall < 55:
        risks.append("Current market and operating indicators suggest elevated sustainability risk.")
    risks.extend(facility_risks)

    required_validations = [
        "Validate local partner landscape and mission alignment through stakeholder interviews.",
        "Complete Stage 2 operating and financial inputs for full institutional analysis.",
        "Run board-level scenario review for conservative and optimistic ranges.",
    ]
    required_validations.extend(facility_validations)

    actions = [
        "Confirm decision criteria and ministry priorities with leadership and board.",
        "Complete missing data collection and refresh analysis with validated inputs.",
        "Develop a 12-month implementation plan with milestones and accountability owners.",
    ]

    return DecisionPathwayRecommendation(
        recommended_pathway=pathway,
        confidence=confidence,
        runner_up_pathways=_runner_up_pathways(pathway),
        top_risks=risks[:3],
        required_validations=required_validations[:4],
        next_12_month_actions=actions,
        partner_assessment=_partner_path_assessment(pathway, request) if request else None,
    )


def _compare_decision_factors(ministry_type: str, pathway: str) -> dict:
    defaults = {
        "schools": {"capital_intensity": "medium", "regulatory_complexity": "medium", "operator_dependency": "optional", "time_to_launch_months_estimate": 18},
        "housing": {"capital_intensity": "high", "regulatory_complexity": "high", "operator_dependency": "required", "time_to_launch_months_estimate": 30},
        "elder_care": {"capital_intensity": "high", "regulatory_complexity": "high", "operator_dependency": "required", "time_to_launch_months_estimate": 24},
    }
    factors = defaults.get(ministry_type, defaults["schools"]).copy()
    if pathway == "continue":
        factors["time_to_launch_months_estimate"] = max(6, factors["time_to_launch_months_estimate"] - 8)
        if factors["operator_dependency"] == "required":
            factors["operator_dependency"] = "optional"
    elif pathway == "close":
        factors["time_to_launch_months_estimate"] = 6
        factors["capital_intensity"] = "low"
        factors["operator_dependency"] = "none"
    return factors

@asynccontextmanager
async def lifespan(app: FastAPI):
    await _get_redis()  # Warm up Redis connection at startup
    if USE_DB:
        from db.connection import init_db, close_db, async_session_factory
        from db.maintenance import backfill_census_centroids

        await init_db()
        async with async_session_factory() as session:
            backfilled = await backfill_census_centroids(session)
            await session.commit()
        if backfilled:
            logger.info("Backfilled %s census tract centroids from boundary geometry at startup", backfilled)

        db_health = await _collect_db_data_health()
        logger.info("DB data-health at startup: connected=%s ready=%s warnings=%s", db_health.get("db_connected"), db_health.get("db_ready_for_analysis"), db_health.get("warnings"))
        yield
        await close_db()
    else:
        yield


app = FastAPI(
    title="Ministry Feasibility API",
    description="Market feasibility analysis for schools, housing, and elder care using Census and ministry-specific data.",
    version="2.0.0",
    lifespan=lifespan,
)

FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL, "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def api_key_middleware(request: Request, call_next):
    """Require X-API-Key header on all requests when INTERNAL_API_KEY is configured."""
    if not INTERNAL_API_KEY:
        return await call_next(request)
    # Allow CORS preflight and health checks through
    if request.method == "OPTIONS" or request.url.path == "/api/health":
        return await call_next(request)
    if request.headers.get("X-API-Key") != INTERNAL_API_KEY:
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
    return await call_next(request)


@app.get("/api/health")
async def health():
    info = {"status": "ok", "version": "2.0.0", "database": USE_DB}
    if USE_DB:
        health_payload = await _collect_db_data_health()
        info["db_connected"] = health_payload.get("db_connected", False)
        info["db_target"] = health_payload.get("db_target")
        info["warnings"] = health_payload.get("warnings", [])[:3]
    return info


@app.get("/api/data-health")
async def data_health():
    """Detailed DB-readiness diagnostics for DB-first analysis paths."""
    return await _collect_db_data_health()


@app.post("/api/analyze", response_model=AnalysisResponse)
async def analyze(request: AnalysisRequest):
    """
    Run a full ministry market feasibility analysis.
    Combines Census ACS demographic data with module-specific competitor data.
    Uses drive-time isochrone (OpenRouteService) for the catchment boundary when
    ORS_API_KEY is set; falls back to a grade-level-adaptive radius otherwise.
    Results are cached in Redis for 24 hours (keyed by address + parameters).
    """
    run_mode = resolve_run_mode(request.run_mode if hasattr(request, "run_mode") else None, USE_DB)
    dependency_health = await _collect_db_data_health() if USE_DB else {}
    dependency_counts = dependency_health.get("counts", {}) if isinstance(dependency_health, dict) else {}
    blockers = strict_mode_blockers(dependency_counts) if run_mode == "db_strict" else []
    if blockers:
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "STRICT_MODE_BLOCKED",
                "message": "db_strict mode blocked because required datasets are unavailable.",
                "run_mode": run_mode,
                "blockers": blockers,
            },
        )

    # --- Redis cache read ---
    redis = await _get_redis()
    cache_key = _cache_key(request)
    if redis:
        try:
            cached = await redis.get(cache_key)
            if cached:
                logger.debug("Cache hit for key %s", cache_key)
                cached_result = AnalysisResponse.model_validate_json(cached)
                cached_result = _apply_reliability_metadata(
                    cached_result,
                    run_mode=run_mode,
                    dependency_counts=dependency_counts,
                    fallback_notes=cached_result.fallback_summary.notes if cached_result.fallback_summary else [],
                    strict_blockers=blockers,
                )
                return await _enrich_analysis_result(cached_result, request)
        except Exception:
            _mark_redis_failure()
            logger.warning("Redis get failed for key %s", cache_key, exc_info=True)

    try:
        location = await geocode_address(request.address)
    except GeocoderServiceError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "GEOCODER_UNAVAILABLE",
                "message": "Address geocoding service is currently unavailable.",
                "detail": str(exc),
            },
        ) from exc
    if not location:
        raise HTTPException(
            status_code=422,
            detail={
                "error_code": "GEOCODE_FAILED",
                "message": "Could not geocode the provided address.",
                "detail": "Please verify the address is a valid US street address including city and state.",
            },
        )

    raw_result, context = await _run_analysis(location, request, run_mode=run_mode)
    result = _apply_reliability_metadata(
        raw_result,
        run_mode=run_mode,
        dependency_counts=dependency_counts,
        fallback_notes=context.get("fallback_notes", []),
        strict_blockers=blockers,
    )
    result = await _enrich_analysis_result(result, request)

    # --- Redis cache write ---
    if redis:
        try:
            await redis.setex(cache_key, CACHE_TTL, result.model_dump_json())
            await redis.setex(snapshot_key(request), SNAPSHOT_TTL, json.dumps(freeze_snapshot(result)))
        except Exception:
            _mark_redis_failure()
            logger.warning("Redis set failed for key %s", cache_key, exc_info=True)

    return result




@app.post("/api/analyze/compare", response_model=CompareAnalysisResponse)
# REVIEW[PERF]: Compare runs each ministry sequentially and bypasses the /api/analyze Redis cache path, so repeated compares for the same address recompute all modules.
async def analyze_compare(request: CompareAnalysisRequest):
    """Run analysis for multiple ministry types at one address and return a side-by-side summary."""
    if not request.ministry_types:
        raise HTTPException(status_code=422, detail="Provide at least one ministry type to compare.")

    deduped_types = list(dict.fromkeys(request.ministry_types))

    try:
        location = await geocode_address(request.address)
    except GeocoderServiceError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "error_code": "GEOCODER_UNAVAILABLE",
                "message": "Address geocoding service is currently unavailable.",
                "detail": str(exc),
            },
        ) from exc
    if not location:
        raise HTTPException(
            status_code=422,
            detail={
                "error_code": "GEOCODE_FAILED",
                "message": "Could not geocode the provided address.",
                "detail": "Please verify the address is a valid US street address including city and state.",
            },
        )

    summaries = []
    for ministry_type in deduped_types:
        single_request = _request_from_compare(request, ministry_type)
        redis = await _get_redis()
        cache_key = _cache_key(single_request)
        result = None
        if redis:
            try:
                cached = await redis.get(cache_key)
                if cached:
                    result = AnalysisResponse.model_validate_json(cached)
            except Exception:
                _mark_redis_failure()
                logger.warning("Redis get failed for compare key %s", cache_key, exc_info=True)

        if result is None:
            result, _ = await _run_analysis(location, single_request)
            if redis:
                try:
                    await redis.setex(cache_key, CACHE_TTL, result.model_dump_json())
                except Exception:
                    _mark_redis_failure()
                    logger.warning("Redis set failed for compare key %s", cache_key, exc_info=True)
        pathway = _phase1_decision_pathway(result, single_request)
        factors = _compare_decision_factors(ministry_type, pathway.recommended_pathway)
        fit_band = "high" if result.feasibility_score.overall >= 75 else "medium" if result.feasibility_score.overall >= 55 else "low"

        summaries.append(
            CompareAnalysisSummary(
                ministry_type=ministry_type,
                overall_score=result.feasibility_score.overall,
                scenario_conservative=result.feasibility_score.scenario_conservative,
                scenario_optimistic=result.feasibility_score.scenario_optimistic,
                recommendation=result.recommendation,
                recommendation_detail=result.recommendation_detail,
                recommended_pathway=pathway.recommended_pathway,
                pathway_confidence=pathway.confidence,
                fit_band=fit_band,
                capital_intensity=factors["capital_intensity"],
                regulatory_complexity=factors["regulatory_complexity"],
                operator_dependency=factors["operator_dependency"],
                time_to_launch_months_estimate=factors["time_to_launch_months_estimate"],
            )
        )

    summaries.sort(key=lambda row: row.overall_score, reverse=True)

    return CompareAnalysisResponse(
        school_name=request.school_name,
        analysis_address=request.address,
        compared_ministry_types=deduped_types,
        results=summaries,
    )

@app.post("/api/export/board-pack")
async def export_board_pack(request: AnalysisRequest):
    """Run analysis and return board-ready report pack JSON."""
    try:
        location = await geocode_address(request.address)
    except GeocoderServiceError as exc:
        raise HTTPException(status_code=503, detail=f"Geocoder unavailable: {exc}") from exc
    if not location:
        raise HTTPException(status_code=422, detail="Could not geocode the provided address.")

    run_mode = resolve_run_mode(request.run_mode if hasattr(request, "run_mode") else None, USE_DB)
    redis = await _get_redis()
    result = thaw_snapshot(await redis.get(snapshot_key(request))) if redis else None
    if result is None:
        result, ctx = await _run_analysis(location, request, run_mode=run_mode)
        result = _apply_reliability_metadata(result, run_mode=run_mode, dependency_counts=None, fallback_notes=ctx.get("fallback_notes", []), strict_blockers=[])
    result = await _enrich_analysis_result(result, request)
    if not result.export_readiness.ready:
        result.outcome = "export_blocked_readiness"
    return {
        "trace_id": result.trace_id,
        "school_name": result.school_name,
        "analysis_address": result.analysis_address,
        "board_report_pack": result.board_report_pack,
        "benchmark_narrative": result.benchmark_narrative,
        "data_freshness": result.data_freshness,
        "export_readiness": result.export_readiness,
    }


@app.post("/api/export/csv")
async def export_csv(request: AnalysisRequest):
    """Run analysis and return results as a downloadable CSV file."""
    try:
        location = await geocode_address(request.address)
    except GeocoderServiceError as exc:
        raise HTTPException(status_code=503, detail=f"Geocoder unavailable: {exc}") from exc
    if not location:
        raise HTTPException(status_code=422, detail="Could not geocode the provided address.")

    run_mode = resolve_run_mode(request.run_mode if hasattr(request, "run_mode") else None, USE_DB)
    redis = await _get_redis()
    result = thaw_snapshot(await redis.get(snapshot_key(request))) if redis else None
    if result is None:
        result, ctx = await _run_analysis(location, request, run_mode=run_mode)
        result = _apply_reliability_metadata(result, run_mode=run_mode, dependency_counts=None, fallback_notes=ctx.get("fallback_notes", []), strict_blockers=[])
    result = await _enrich_analysis_result(result, request)
    csv_content = generate_csv_report(result)
    filename = f"feasibility_{request.school_name.replace(' ', '_')}.csv"

    return StreamingResponse(
        io.StringIO(csv_content),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/schools/stage2/extract-audits", response_model=SchoolAuditExtractionResponse)
async def extract_school_stage2_audits(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="Upload at least one PDF audit file.")

    rows = []
    warnings = []
    for i, uploaded in enumerate(files):
        if not uploaded.filename.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {uploaded.filename}")
        payload = await uploaded.read()
        extracted = extract_audit_financials(payload, uploaded.filename, i)
        rows.extend(extracted["rows"])
        warnings.extend(extracted.get("warnings", []))

    deduped = dedupe_year_rows(rows)
    return SchoolAuditExtractionResponse(years=deduped, warnings=warnings)


# --- v2 endpoints: pipeline status and opportunity discovery ---


@app.get("/api/pipeline/status")
async def pipeline_status():
    """Show data pipeline run status, freshness signals, and retry guidance."""
    if not USE_DB:
        raise HTTPException(status_code=501, detail="Database not enabled. Set USE_DB=true.")

    from db.connection import async_session_factory
    from pipeline.base import get_latest_run
    from sqlalchemy import select, func, desc
    from db.models import (
        CensusTract,
        CompetitorSchoolRecord,
        CompetitorElderCare,
        CompetitorHousing,
        HudIngestRun,
        HudLihtcProperty,
        HudLihtcTenant,
        HudQctDdaDesignation,
        HudRawSnapshot,
        HudSection202Property,
        PipelineRun,
    )

    async with async_session_factory() as session:
        counts = {}
        for model, label in [
            (CensusTract, "census_tracts"),
            (CompetitorSchoolRecord, "schools"),
            (CompetitorElderCare, "elder_care_facilities"),
            (CompetitorHousing, "housing_projects"),
            (HudRawSnapshot, "hud_raw_snapshots"),
            (HudLihtcProperty, "hud_lihtc_property"),
            (HudLihtcTenant, "hud_lihtc_tenant"),
            (HudQctDdaDesignation, "hud_qct_dda"),
            (HudQctDdaDesignation, "hud_qct_dda_designations"),
            (HudSection202Property, "hud_section_202"),
        ]:
            result = await session.execute(select(func.count()).select_from(model))
            counts[label] = result.scalar()

        pipelines = {}
        stale_pipelines = []
        for name in ["census_acs", "nces_pss", "cms_elder_care", "hud_lihtc_property", "hud_lihtc_tenant", "hud_qct_dda", "hud_section_202"]:
            run = await get_latest_run(session, name)
            failed = await session.execute(
                select(PipelineRun)
                .where(PipelineRun.pipeline_name == name, PipelineRun.status == "failed")
                .order_by(desc(PipelineRun.finished_at), desc(PipelineRun.started_at))
                .limit(1)
            )
            last_failed = failed.scalar_one_or_none()

            if run and run.finished_at:
                finished = run.finished_at.replace(tzinfo=timezone.utc) if run.finished_at.tzinfo is None else run.finished_at
                age_hours = round((datetime.now(timezone.utc) - finished).total_seconds() / 3600, 1)
                freshness = _freshness_status(age_hours, PIPELINE_FRESHNESS_TARGET_HOURS.get(name))
                if freshness == "stale":
                    stale_pipelines.append(name)
                pipelines[name] = {
                    "last_success": finished.isoformat(),
                    "records_processed": run.records_processed,
                    "records_inserted": run.records_inserted,
                    "freshness_hours": age_hours,
                    "freshness_status": freshness,
                    "last_failure": {
                        "finished_at": last_failed.finished_at.isoformat() if last_failed and last_failed.finished_at else None,
                        "error_message": last_failed.error_message if last_failed else None,
                    },
                }
            else:
                pipelines[name] = {
                    "last_success": None,
                    "records_processed": None,
                    "records_inserted": None,
                    "freshness_hours": None,
                    "freshness_status": "unknown",
                    "last_failure": {
                        "finished_at": last_failed.finished_at.isoformat() if last_failed and last_failed.finished_at else None,
                        "error_message": last_failed.error_message if last_failed else None,
                    },
                }

        hud_ingest = {}
        for source_family in ["lihtc_property", "lihtc_tenant", "qct_dda"]:
            last = await session.execute(
                select(HudIngestRun)
                .where(HudIngestRun.source_family == source_family)
                .order_by(desc(HudIngestRun.started_at))
                .limit(1)
            )
            row = last.scalar_one_or_none()
            hud_ingest[source_family] = {
                "status": row.status if row else "unknown",
                "dataset_year": row.dataset_year if row else None,
                "source_version": row.source_version if row else None,
                "started_at": row.started_at.isoformat() if row and row.started_at else None,
                "finished_at": row.finished_at.isoformat() if row and row.finished_at else None,
                "error_message": row.error_message if row else None,
            }

    diagnostics, db_ready_for_analysis, readiness_status = _build_pipeline_diagnostics(counts, pipelines)

    return {
        "record_counts": counts,
        "pipelines": pipelines,
        "stale_pipelines": stale_pipelines,
        "retry_recommended": len(stale_pipelines) > 0,
        "db_ready_for_analysis": db_ready_for_analysis,
        "readiness_status": readiness_status,
        "diagnostics": diagnostics,
        "hud_ingest": hud_ingest,
    }


@app.get("/api/opportunities")
async def opportunities(
    ministry_type: str = "schools",
    state: str = None,
    min_score: int = 60,
    limit: int = 50,
):
    """Discover top feasibility opportunities by ministry type."""
    if not USE_DB:
        raise HTTPException(status_code=501, detail="Database not enabled. Set USE_DB=true.")

    from db.connection import get_session
    from db.queries import get_top_opportunities

    async with get_session() as session:
        results = await get_top_opportunities(
            session,
            ministry_type=ministry_type,
            state_fips=state,
            min_score=min_score,
            limit=limit,
        )

    return [
        {
            "geoid": r.geoid,
            "ministry_type": r.ministry_type,
            "overall_score": r.overall_score,
            "market_size_score": r.market_size_score,
            "income_score": r.income_score,
            "competition_score": r.competition_score,
            "family_density_score": r.family_density_score,
            "percentile_state": r.percentile_state,
            "percentile_national": r.percentile_national,
        }
        for r in results
    ]


# ---------------------------------------------------------------------------
# History endpoints (require USE_DB=true)
# ---------------------------------------------------------------------------


@app.get("/api/history")
async def get_history(
    ministry_type: str | None = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    """List past analysis records, newest first. Requires USE_DB=true."""
    if not USE_DB:
        raise HTTPException(status_code=501, detail="Database not enabled. Set USE_DB=true.")

    from db.connection import get_session
    from db.models import AnalysisRecord
    from sqlalchemy import select, desc

    async with get_session() as session:
        q = select(AnalysisRecord).order_by(desc(AnalysisRecord.created_at)).offset(skip).limit(limit)
        if ministry_type:
            q = q.where(AnalysisRecord.ministry_type == ministry_type)
        rows = (await session.execute(q)).scalars().all()

    return [
        {
            "id": r.id,
            "school_name": r.school_name,
            "address": r.address,
            "ministry_type": r.ministry_type,
            "overall_score": r.overall_score,
            "result_summary": r.result_summary,
            "request_params": r.request_params,
            "created_at": r.created_at.isoformat(),
        }
        for r in rows
    ]


@app.get("/api/history/{record_id}")
async def get_history_record(record_id: int):
    """Retrieve a single analysis record by ID. Requires USE_DB=true."""
    if not USE_DB:
        raise HTTPException(status_code=501, detail="Database not enabled. Set USE_DB=true.")

    from db.connection import get_session
    from db.models import AnalysisRecord
    from sqlalchemy import select

    async with get_session() as session:
        row = (await session.execute(select(AnalysisRecord).where(AnalysisRecord.id == record_id))).scalar_one_or_none()

    if not row:
        raise HTTPException(status_code=404, detail={"error_code": "NOT_FOUND", "message": f"Analysis record {record_id} not found."})

    return {
        "id": row.id,
        "school_name": row.school_name,
        "address": row.address,
        "ministry_type": row.ministry_type,
        "lat": row.lat,
        "lon": row.lon,
        "overall_score": row.overall_score,
        "request_params": row.request_params,
        "result_summary": row.result_summary,
        "created_at": row.created_at.isoformat(),
    }


@app.delete("/api/history/{record_id}", status_code=204)
async def delete_history_record(record_id: int):
    """Delete a single analysis record. Requires USE_DB=true."""
    if not USE_DB:
        raise HTTPException(status_code=501, detail="Database not enabled. Set USE_DB=true.")

    from db.connection import get_session
    from db.models import AnalysisRecord
    from sqlalchemy import select, delete

    async with get_session() as session:
        result = await session.execute(delete(AnalysisRecord).where(AnalysisRecord.id == record_id))
        await session.commit()

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail={"error_code": "NOT_FOUND", "message": f"Analysis record {record_id} not found."})


# ---------------------------------------------------------------------------
# PDF export
# ---------------------------------------------------------------------------


@app.post("/api/export/pdf")
async def export_pdf(request: AnalysisRequest):
    """Run analysis and return results as a downloadable PDF report."""
    from api.pdf_report import generate_pdf_report

    try:
        location = await geocode_address(request.address)
    except GeocoderServiceError as exc:
        raise HTTPException(status_code=503, detail=f"Geocoder unavailable: {exc}") from exc
    if not location:
        raise HTTPException(
            status_code=422,
            detail={"error_code": "GEOCODE_FAILED", "message": "Could not geocode the provided address."},
        )

    run_mode = resolve_run_mode(request.run_mode if hasattr(request, "run_mode") else None, USE_DB)
    redis = await _get_redis()
    result = thaw_snapshot(await redis.get(snapshot_key(request))) if redis else None
    if result is None:
        result, ctx = await _run_analysis(location, request, run_mode=run_mode)
        result = _apply_reliability_metadata(result, run_mode=run_mode, dependency_counts=None, fallback_notes=ctx.get("fallback_notes", []), strict_blockers=[])
    result = await _enrich_analysis_result(result, request)
    pdf_bytes = generate_pdf_report(result, request)
    safe_name = request.school_name.replace(" ", "_")
    filename = f"feasibility_{safe_name}.pdf"

    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# Scoring weights endpoint
# ---------------------------------------------------------------------------


@app.get("/api/scoring/weights")
async def scoring_weights():
    """Return the scoring weight definitions for all ministry types."""
    return {
        "schools": {
            "market_size": {"weight": 0.35, "description": "Income-first addressable market with Catholic affiliation boost"},
            "income": {"weight": 0.25, "description": "Median household income and high-income share"},
            "competition": {"weight": 0.25, "description": "Distance-decayed competitor presence (validation 60%, saturation 40%)"},
            "family_density": {"weight": 0.15, "description": "Share of households with school-age children"},
        },
        "housing": {
            "market_size": {"weight": 0.40, "description": "Cost-burdened renter population"},
            "income": {"weight": 0.30, "description": "Income levels relative to AMI thresholds"},
            "competition": {"weight": 0.20, "description": "Existing LIHTC unit saturation"},
            "family_density": {"weight": 0.10, "description": "Renter burden intensity"},
        },
        "elder_care": {
            "mission_mode": {
                "market_size": {"weight": 0.35, "description": "Senior population 65+ and growth trajectory"},
                "income": {"weight": 0.15, "description": "Ability to pay — private-pay market depth"},
                "competition": {"weight": 0.30, "description": "CMS facility bed capacity vs. senior demand"},
                "family_density": {"weight": 0.10, "description": "Senior household density and isolation index"},
                "occupancy": {"weight": 0.10, "description": "Facility occupancy and bed availability"},
            },
            "market_mode": {
                "market_size": {"weight": 0.35, "description": "Senior population 65+ and growth trajectory"},
                "income": {"weight": 0.25, "description": "Ability to pay — private-pay market depth"},
                "competition": {"weight": 0.25, "description": "CMS facility bed capacity vs. senior demand"},
                "family_density": {"weight": 0.05, "description": "Senior household density and isolation index"},
                "occupancy": {"weight": 0.10, "description": "Facility occupancy and bed availability"},
            },
        },
        # Backward-compatible legacy block (school-oriented). Prefer `hierarchical_by_ministry`.
        "hierarchical": {
            "market_opportunity": {"weight": 0.45, "description": "Population size, income, and pipeline demand"},
            "competitive_position": {"weight": 0.30, "description": "Market validation and saturation pressure"},
            "community_fit": {"weight": 0.15, "description": "Mission alignment and demographic compatibility"},
            "sustainability_risk": {"weight": 0.10, "description": "Income volatility and demographic stability"},
        },
        # Module-aware hierarchical metadata. Shape mirrors the legacy block per module:
        # {module: {index_key: {weight: float, description: str}}}
        "hierarchical_by_ministry": {
            "schools": {
                "market_opportunity": {"weight": 0.45, "description": "Population size, income, and pipeline demand"},
                "competitive_position": {"weight": 0.30, "description": "Market validation and saturation pressure"},
                "community_fit": {"weight": 0.15, "description": "Mission alignment and demographic compatibility"},
                "sustainability_risk": {"weight": 0.10, "description": "Income volatility and demographic stability"},
            },
            "housing": {
                "market_opportunity": {"weight": 0.45, "description": "Renter need, affordability pressure, and unmet housing demand"},
                "competitive_position": {"weight": 0.30, "description": "Supply pressure, subsidy saturation, and delivery competition"},
                "community_fit": {"weight": 0.15, "description": "Neighborhood fit, mission alignment, and local housing context"},
                "sustainability_risk": {"weight": 0.10, "description": "Development viability, operating resilience, and demographic stability"},
            },
            "elder_care": {
                "market_opportunity": {"weight": 0.45, "description": "Senior demand, aging trends, and need intensity"},
                "competitive_position": {"weight": 0.30, "description": "Care-market competition, bed supply pressure, and service saturation"},
                "community_fit": {"weight": 0.15, "description": "Community aging profile and mission/service alignment"},
                "sustainability_risk": {"weight": 0.10, "description": "Staffing and operational viability with long-term demographic stability"},
            },
        },
    }


async def _run_analysis(location: dict, request: AnalysisRequest, run_mode: str = DEFAULT_RUN_MODE) -> tuple[AnalysisResponse, dict]:
    """
    Orchestrate the full analysis pipeline:
    1. Fetch drive-time isochrone (sequential — needed before data fetch).
    2. Fetch Census demographics and school data concurrently.
    3. Compute feasibility score and return AnalysisResponse.

    When USE_DB is enabled, demographics are read from the precomputed database
    instead of making live Census API calls.
    """

    drive_minutes = request.drive_minutes
    grade_level = request.grade_level

    # Step 1: Attempt isochrone fetch (with DB cache when enabled)
    isochrone_polygon = None
    fallback_notes: list[str] = []
    if USE_DB:
        from db.connection import get_session
        from db.queries import lookup_cached_isochrone, save_isochrone

        async with get_session() as session:
            cached = await lookup_cached_isochrone(
                session,
                lat=location["lat"],
                lon=location["lon"],
                drive_minutes=drive_minutes,
                max_age_hours=ISOCHRONE_CACHE_TTL_HOURS,
            )
            if cached and cached.polygon_geojson:
                import json

                isochrone_polygon = json.loads(cached.polygon_geojson)

    if not isochrone_polygon:
        isochrone_polygon = await get_isochrone(location["lat"], location["lon"], drive_minutes)

    if isochrone_polygon:
        effective_radius = isochrone_effective_radius_miles(
            location["lat"], location["lon"], isochrone_polygon
        )
        catchment_type = "isochrone"

        if USE_DB:
            from db.connection import get_session
            from db.queries import save_isochrone

            try:
                async with get_session() as session:
                    await save_isochrone(
                        session,
                        lat=location["lat"],
                        lon=location["lon"],
                        drive_minutes=drive_minutes,
                        polygon_geojson=isochrone_polygon,
                        effective_radius_miles=effective_radius,
                    )
                    await session.commit()
            except Exception:
                logger.warning("Failed to cache isochrone for (%.4f, %.4f) %dmin", location["lat"], location["lon"], drive_minutes, exc_info=True)
    else:
        effective_radius = GRADE_LEVEL_FALLBACK_RADIUS.get(grade_level, 12.0)
        catchment_type = "radius"
        fallback_notes.append("Isochrone unavailable; used grade-level radius fallback.")

    # Step 2: Fetch demographics (DB-backed first, with automatic live fallback)
    demographics = None
    used_live_demographics_fallback = False
    if USE_DB:
        from db.connection import get_session
        from db.demographics import aggregate_demographics

        async with get_session() as session:
            demographics = await aggregate_demographics(
                session=session,
                lat=location["lat"],
                lon=location["lon"],
                radius_miles=effective_radius,
                state_fips=location["state_fips"],
                county_fips=location.get("county_fips"),
                isochrone_geojson=isochrone_polygon,
            )

        # DB mode enabled but demographics lookup returned zero tracts —
        # either pipelines not populated or this area was not ingested.
        if (demographics or {}).get("tract_count", 0) == 0:
            data_geo = (demographics or {}).get("data_geography", "unknown")
            logger.warning(
                "USE_DB=true but no census tract rows were found for catchment "
                "after spatial+county+state fallbacks; falling back to live Census API. "
                "lookup_context=(lat=%.6f lon=%.6f radius_miles=%.2f catchment_type=%s "
                "state_fips=%s county_fips=%s data_geography=%s). "
                "Likely cause: this state/county has not been ingested yet, or tracts "
                "exist without centroid/boundary geometry.",
                location["lat"],
                location["lon"],
                effective_radius,
                catchment_type,
                location["state_fips"],
                location.get("county_fips"),
                data_geo,
            )
            used_live_demographics_fallback = True
            fallback_notes.append("DB demographics unavailable for this area; used live Census API fallback.")

    if (not USE_DB) or used_live_demographics_fallback:
        demographics = await get_demographics(
            lat=location["lat"],
            lon=location["lon"],
            county_fips=location["county_fips"],
            state_fips=location["state_fips"],
            radius_miles=effective_radius,
            isochrone_polygon=isochrone_polygon,
        )

    # Step 3: Dispatch to ministry module analyzer
    module = MODULE_REGISTRY.get(request.ministry_type, MODULE_REGISTRY["schools"])
    result = await module.analyzer(
        location=location,
        demographics=demographics,
        request=request,
        radius_miles=effective_radius,
        drive_minutes=drive_minutes,
        isochrone_polygon=isochrone_polygon,
        catchment_type=catchment_type,
    )
    result.ministry_type = request.ministry_type
    result.trace_id = result.trace_id or str(uuid.uuid4())
    result.data_freshness = await _build_data_freshness_metadata()
    result.benchmark_narrative = _build_benchmark_narrative(result)

    # Step 4: Persist analysis to history (if DB enabled)
    if USE_DB:
        from db.connection import get_session
        from db.models import AnalysisRecord
        from geoalchemy2.shape import from_shape
        from shapely.geometry import Point
        try:
            async with get_session() as session:
                record = AnalysisRecord(
                    school_name=request.school_name,
                    address=request.address,
                    ministry_type=request.ministry_type,
                    lat=location["lat"],
                    lon=location["lon"],
                    location_point=from_shape(Point(location["lon"], location["lat"]), srid=4326),
                    request_params=request.model_dump(),
                    result_summary={
                        "overall_score": result.feasibility_score.overall,
                        "recommendation": result.recommendation,
                    },
                    overall_score=result.feasibility_score.overall,
                )
                session.add(record)
                await session.commit()
        except Exception:
            logger.warning("Failed to persist analysis history for '%s'", request.school_name, exc_info=True)

    return result, {"used_live_demographics_fallback": used_live_demographics_fallback, "fallback_notes": fallback_notes, "catchment_type": catchment_type}


async def _load_workspace(session, workspace_id: str):
    from db.models import PortfolioWorkspaceRecord
    from sqlalchemy import select
    return (await session.execute(select(PortfolioWorkspaceRecord).where(PortfolioWorkspaceRecord.workspace_id == workspace_id))).scalar_one_or_none()


@app.post("/api/portfolio/workspaces", response_model=PortfolioWorkspaceResponse)
async def create_portfolio_workspace(request: PortfolioWorkspaceCreateRequest):
    """Create a Phase 2 portfolio workspace for multi-site consulting workflows."""
    if not USE_DB:
        raise HTTPException(status_code=501, detail="Portfolio workspace persistence requires USE_DB=true.")

    from db.connection import get_session
    from db.models import PortfolioWorkspaceRecord

    workspace_id = str(uuid.uuid4())
    workspace = PortfolioWorkspaceResponse(
        workspace_id=workspace_id,
        engagement_name=request.engagement_name,
        client_name=request.client_name,
        candidate_locations=request.candidate_locations,
        scenario_sets=request.scenario_sets,
        compare_snapshots=[],
    )

    async with get_session() as session:
        session.add(PortfolioWorkspaceRecord(workspace_id=workspace_id, payload=workspace.model_dump(), version=1))

    return workspace


@app.get("/api/portfolio/workspaces/{workspace_id}", response_model=PortfolioWorkspaceResponse)
async def get_portfolio_workspace(workspace_id: str):
    if not USE_DB:
        raise HTTPException(status_code=501, detail="Portfolio workspace persistence requires USE_DB=true.")

    from db.connection import get_session

    async with get_session() as session:
        row = await _load_workspace(session, workspace_id)

    if not row:
        raise HTTPException(status_code=404, detail={"error_code": "NOT_FOUND", "message": f"Workspace {workspace_id} not found."})
    return PortfolioWorkspaceResponse.model_validate(row.payload)


@app.patch("/api/portfolio/workspaces/{workspace_id}", response_model=PortfolioWorkspaceResponse)
async def update_portfolio_workspace(
    workspace_id: str,
    request: PortfolioWorkspaceUpdateRequest,
    if_match_version: int | None = Header(default=None, alias="If-Match-Version"),
):
    if not USE_DB:
        raise HTTPException(status_code=501, detail="Portfolio workspace persistence requires USE_DB=true.")

    from db.connection import get_session

    async with get_session() as session:
        row = await _load_workspace(session, workspace_id)
        if not row:
            raise HTTPException(status_code=404, detail={"error_code": "NOT_FOUND", "message": f"Workspace {workspace_id} not found."})
        if if_match_version is not None and row.version != if_match_version:
            raise HTTPException(status_code=409, detail={"error_code": "VERSION_CONFLICT", "message": "Workspace version conflict.", "current_version": row.version})

        updated = PortfolioWorkspaceResponse.model_validate(row.payload)
        if request.engagement_name is not None:
            updated.engagement_name = request.engagement_name
        if request.client_name is not None:
            updated.client_name = request.client_name
        if request.candidate_locations is not None:
            updated.candidate_locations = request.candidate_locations
        if request.scenario_sets is not None:
            updated.scenario_sets = request.scenario_sets

        row.payload = updated.model_dump()
        row.version += 1

    return updated


@app.post("/api/portfolio/workspaces/{workspace_id}/compare-snapshots", response_model=PortfolioWorkspaceResponse)
async def add_portfolio_compare_snapshot(workspace_id: str, request: CompareAnalysisRequest):
    if not USE_DB:
        raise HTTPException(status_code=501, detail="Portfolio workspace persistence requires USE_DB=true.")

    from db.connection import get_session

    compare_result = await analyze_compare(request)

    async with get_session() as session:
        row = await _load_workspace(session, workspace_id)
        if not row:
            raise HTTPException(status_code=404, detail={"error_code": "NOT_FOUND", "message": f"Workspace {workspace_id} not found."})

        updated = PortfolioWorkspaceResponse.model_validate(row.payload)
        updated.compare_snapshots.append(
            PortfolioCompareSnapshot(
                snapshot_id=str(uuid.uuid4()),
                label=f"{request.school_name} · {request.address}",
                school_name=compare_result.school_name,
                analysis_address=compare_result.analysis_address,
                compared_ministry_types=compare_result.compared_ministry_types,
                results=compare_result.results,
            )
        )

        row.payload = updated.model_dump()
        row.version += 1

    return updated
