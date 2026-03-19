# TODO — Deferred dashboard domain roadmap capture

## Plan
- [x] Review repository instructions and existing dashboard planning artifacts.
- [x] Decide the smallest durable place to store the deferred dashboard domain roadmap.
- [x] Add a dedicated planning document that captures domain priorities, sub-tabs, data needs, and required new shared components.
- [x] Align dashboard preview metadata with the updated deferred-domain framing where the current copy is out of date.
- [x] Update top-level documentation references so the roadmap is discoverable.
- [x] Run targeted verification, then update review notes.

## Review notes
- Added a dedicated deferred-dashboard roadmap document with domain priorities, data requirements, and shared-component gating.
- Updated the dashboard preview copy to reflect the new deferred-domain framing, especially elder-care partnership matchmaking and housing demographic trends folding into Community Profile.
- Verified the frontend metadata changes with the existing frontend test suite and a standalone TypeScript check.

---

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


## Plan — Wave 1 deferred dashboard domains
- [x] Review `DEFERRED_DASHBOARD_DOMAINS.md` and map the smallest Wave 1 implementation slice across preview/live dashboard code.
- [x] Extend shared dashboard primitives for Wave 1 needs (`DistributionChart.referenceLine` and a partner-focused facility table component).
- [x] Update dashboard module configuration/rendering so Wave 1 schools, elder-care, and housing views present the new domain framing.
- [x] Run targeted frontend verification, then update review notes.

## Review notes — Wave 1 deferred dashboard domains
- Added a `DistributionChart.referenceLine` option for the schools affordability distribution view and wired the Wave 1 tuition-fit overlay into preview/live module configs.
- Added a dedicated `PartnerFacilityTable` and surfaced it for the elder-care partnership-viability sidebar while keeping the shared competitor table for existing landscape views.
- Updated preview/live dashboard metadata so schools affordability, elder-care partnership viability, and housing community-profile demographic trends have Wave 1-specific labels, tabs, and highlight cards.
- Verified the frontend changes with the existing frontend tests plus a full TypeScript no-emit check; `next lint` could not run non-interactively because the repo does not yet have an ESLint config.
