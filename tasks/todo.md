# TODO — Full codebase audit (excluding Stage 2 analysis)

## Plan
- [x] Review existing notes (`tasks/lessons.md`) and current repository state.
- [x] Map core system shape:
  - [x] Frontend user flow, navigation, and analysis/report export interactions.
  - [x] Backend request lifecycle and shared error-handling paths.
  - [x] Module boundaries (schools/housing/elder care) and shared scoring/reporting.
  - [x] Data ingest pipelines and runtime data dependencies/readiness gates.
- [x] Trace and evaluate primary runtime paths with stability + graceful degradation lens:
  - [x] Success path for baseline feasibility analysis.
  - [x] Missing-data paths and fallback behavior.
  - [x] Failure/timeout/error propagation behavior.
- [x] Perform structured audit by domain:
  - [x] Product/workflow coherence and v2 scope clarity.
  - [x] Frontend UX/interpretability/trust.
  - [x] Backend API consistency, reliability, and observability.
  - [x] Architecture/maintainability/coupling.
  - [x] Data pipeline design and critical-path appropriateness.
  - [x] Scoring/analysis transparency (excluding Stage 2).
  - [x] Testing/operations/deployment hygiene.
- [x] Produce required deliverables in order:
  - [x] Codebase map.
  - [x] Findings register with severity/category/evidence/impact/action.
  - [x] Delete/simplify recommendations.
  - [x] "If rebuilding today" memo.
  - [x] Prioritized phased roadmap.
  - [x] Open questions/ambiguities.
- [x] Validate report consistency against stated product goals:
  - [x] Fast directional feasibility.
  - [x] Board-ready analysis/export.
  - [x] Stability above all.
- [x] Update this file with completion/review notes and commit changes.

## Review notes
- Completed full audit and delivered findings in `tasks/full_audit_report.md`.
- Focused recommendations prioritize stability, graceful degradation, and simplification over feature expansion.
- Excluded Stage 2 redesign details except where shared architecture/coupling impacts baseline reliability.

## Plan — Data readiness + response model hardening
- [x] Audit pipeline run persistence and completion semantics.
- [x] Align doctor readiness outputs to required-vs-optional dependencies.
- [x] Trace census catchment fallback path and improve lookup + logging.
- [x] Replace dict assignments for typed response fields with Pydantic model instances.
- [x] Add regression tests for tracking, readiness, lookup normalization, and typed nesting.

## Review notes — Data readiness + response model hardening
- Fixed detached pipeline-run completion updates and removed ingest flows that created extra completion runs.
- Updated doctor/readiness to report `readiness_status` (`ready`, `ready_with_fallbacks`, `not_ready`) while keeping strict blockers for required baselines.
- Corrected pipeline-status tracking names for HUD (`hud_lihtc_property`, `hud_lihtc_tenant`, `hud_qct_dda`) and expanded diagnostics.
- Improved census catchment observability and county fallback normalization for 3-digit vs 5-digit FIPS variants.
- Converted nested reliability metadata assignment to typed schema models and added tests.
