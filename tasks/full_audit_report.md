# Full codebase audit (excluding Stage 2 analysis redesign)

## 1) Codebase map

### Main subsystems
- **Frontend (Next.js app)**
  - Primary analysis workflow at `/` (single long form + results dashboard).
  - Intelligence console at `/intelligence` for pipeline status/opportunities/methodology views.
  - Local-storage project persistence and prefill behavior.
- **Backend (FastAPI monolith)**
  - `/api/analyze` orchestrates geocoding → catchment → demographics → module analyzer → response enrichment.
  - `/api/analyze/compare`, export endpoints (CSV/PDF/board-pack), scoring metadata endpoints, DB history and portfolio workspace endpoints.
  - Shared middleware for optional API-key enforcement.
- **Module analyzers**
  - `schools` (shared scoring engine in `api/analysis.py`).
  - `housing` (HUD/LIHTC-specific path with DB and CSV fallback).
  - `elder_care` (CMS-focused scoring and competitor retrieval).
- **Data layer + pipelines**
  - Optional DB-first mode (`USE_DB=true`) with PostGIS + Redis + Celery.
  - Pipeline CLI for Census/NCES/CMS/HUD ingestion and pipeline status.

### Major request flows
1. Frontend calls `/api/analyze`.
2. Backend geocodes address with Census API (service errors mapped to 503).
3. Backend computes catchment from ORS isochrone or grade-level radius fallback.
4. Backend fetches demographics (DB aggregate with live ACS fallback) and module-specific competitor data (DB with live/CSV fallback).
5. Module computes scores/recommendation and backend enriches with decision pathway, benchmark narrative, freshness metadata, board pack.
6. Optional export endpoints rerun analysis then emit CSV/PDF/JSON.

### Main data flows
- **Runtime data path**: user request -> third-party APIs + optional precomputed DB -> in-process scoring -> JSON response.
- **Batch data path**: ingestion pipelines load normalized tables for demographics/competitors/HUD enrichment.
- **Caching**:
  - In-process memory cache for geocoding/demographics.
  - Redis cache for analysis responses when available.
  - DB cache for isochrones in DB mode.

### Complexity hotspots
- `backend/main.py` is a high-coupling orchestrator that also owns pathway logic, export wiring, health/readiness checks, cache handling, and portfolio endpoints.
- `frontend/src/components/AnalysisForm.tsx` combines multiple products/modes in one oversized component.
- Mixed live-vs-DB fallback logic exists in many places with inconsistent warning/visibility patterns.
- Multiple “phase” concerns (baseline analysis + advanced artifacts) are interleaved in the primary flow.

### Biggest risk zones
- **Product risk**: primary workflow is overloaded for “fast directional” use.
- **Reliability risk**: availability of many optional systems can alter results and behavior in non-obvious ways.
- **Maintainability risk**: monolithic files + duplicated endpoint behaviors and mixed responsibility boundaries.
- **Trust risk**: strong recommendations are generated even when confidence/readiness is partial and assumptions are heuristic-heavy.

---

## 2) Findings register

### A. Bugs / broken features

1. **Portfolio workflow tests appear out of sync with current implementation**  
   - Severity: **high**  
   - Category: bugs/broken features  
   - Evidence: tests reference in-memory `_PORTFOLIO_WORKSPACES`, while runtime portfolio endpoints are DB-backed and guarded by `USE_DB`.  
   - Impact: likely false confidence from stale tests; can mask regressions in collaboration workflow.  
   - Recommended action: align/replace obsolete tests with DB-backed contract tests; remove dead assumptions.

2. **Leftover reject/patch artifacts in repo (`*.rej`, `main.patch`)**  
   - Severity: **medium**  
   - Category: bugs/broken features  
   - Evidence: multiple rejected patch files are committed.  
   - Impact: indicates unresolved merge/conflict residue; raises risk of shipping partially applied fixes and confuses maintainers.  
   - Recommended action: remove artifacts immediately; confirm intended changes are fully integrated.

3. **Inconsistent frontend API error handling quality**  
   - Severity: **medium**  
   - Category: bugs/broken features  
   - Evidence: `runAnalysis` manually parses one error shape; other functions use generic static messages; parse helper not consistently used.  
   - Impact: users get uneven and low-context failure messaging, reducing debuggability and trust.  
   - Recommended action: standardize API error envelope + single client parser and friendly mapping.

### B. Architecture / maintainability

4. **`backend/main.py` is overgrown and functionally monolithic**  
   - Severity: **high**  
   - Category: architecture/maintainability  
   - Evidence: one file mixes orchestration, cache plumbing, decision-pathway heuristics, board-pack content synthesis, health/readiness checks, and workspace/history endpoints.  
   - Impact: high regression risk, hard onboarding, and slow safe change velocity.  
   - Recommended action: split into explicit application services (analysis orchestration, exports, readiness/ops, portfolio) plus thin routers.

5. **Cross-cutting fallback policy is distributed and inconsistent**  
   - Severity: **high**  
   - Category: architecture/maintainability  
   - Evidence: each module independently decides fallback behavior and user visibility; no central “required vs optional dataset” contract object.  
   - Impact: unpredictable degradation and inconsistent confidence semantics between modules.  
   - Recommended action: introduce explicit dataset dependency registry and centralized policy engine (`required`, `optional`, `disabled_feature`).

6. **Frontend form and results surfaces are too coupled to every feature variant**  
   - Severity: **high**  
   - Category: architecture/maintainability  
   - Evidence: one large form includes baseline inputs, optional advanced inputs, and several module-specific branches.  
   - Impact: difficult UX evolution; high chance of side effects when changing any input path.  
   - Recommended action: split into `QuickScreenForm` and `AdvancedInputsForm`; isolate module-specific sections into separate components.

### C. UX / product

7. **Primary workflow is not optimized for “fast directional feasibility”**  
   - Severity: **high**  
   - Category: UX/product  
   - Evidence: default entry point exposes many controls and advanced concepts early (weight profiles, stage toggles, facility profile, etc.).  
   - Impact: slows first-pass use; increases cognitive load and inconsistent usage across staff.  
   - Recommended action: default to a 60-second quick screen with minimal fields + deferred advanced mode.

8. **Board-ready outputs are bolted onto runtime response rather than treated as a distinct product surface**  
   - Severity: **high**  
   - Category: UX/product  
   - Evidence: board pack and pathway content are generated in same runtime response path and can be exported ad hoc from operational screen.  
   - Impact: weak separation between directional screening vs board-grade deliverables, risking overinterpretation of early outputs.  
   - Recommended action: introduce explicit “promote to board memo” flow requiring minimum confidence/readiness checks.

9. **Result explainability is present but still too narrative-heavy and assumption-implicit**  
   - Severity: **medium**  
   - Category: UX/product  
   - Evidence: many score adjustments and boosts are embedded in prose; users must infer which data quality level materially affected recommendation strength.  
   - Impact: trust erosion when recommendations conflict with local intuition.  
   - Recommended action: add per-metric “inputs used / missing / fallback used / confidence impact” panel.

### D. Data / pipeline

10. **DB readiness and runtime fallback logic are partially contradictory**  
   - Severity: **high**  
   - Category: data/pipeline  
   - Evidence: DB mode can report warnings/non-ready while request-level analysis still silently falls back to live/CSV paths.  
   - Impact: output reproducibility and comparability degrade across runs/environments.  
   - Recommended action: formalize run mode in response (`db_strict`, `db_with_fallback`, `live_only`) and include hard gating rules for exports.

11. **Too many datasets sit near critical path without explicit value/optionality contracts**  
   - Severity: **medium**  
   - Category: data/pipeline  
   - Evidence: housing enrichments and freshness metadata are deeply wired while baseline value per dataset is uneven.  
   - Impact: higher fragility and operational burden than needed for directional feasibility.  
   - Recommended action: trim critical-path datasets to minimum viable baseline; move enrichments to optional “context overlays”.

12. **Catchment strategy can materially change analysis mode without strong UX disclosure**  
   - Severity: **medium**  
   - Category: data/pipeline  
   - Evidence: ORS isochrone fallback to radius occurs automatically.  
   - Impact: analysts may compare outputs with different catchment semantics unknowingly.  
   - Recommended action: prominently disclose catchment mode and confidence effect in result header and exports.

### E. Performance / reliability

13. **Repeated endpoint recomputation for exports and compare paths**  
   - Severity: **medium**  
   - Category: performance/reliability  
   - Evidence: export endpoints rerun analysis; compare iterates ministry runs with repeated logic.  
   - Impact: unnecessary latency/load and more opportunities for upstream failures.  
   - Recommended action: create reusable analysis execution artifact keyed by trace/request; exports should consume frozen result snapshots.

14. **In-process caches are non-shared and process-local**  
   - Severity: **medium**  
   - Category: performance/reliability  
   - Evidence: geocoder/demographics memory caches live in process globals.  
   - Impact: inconsistent cache behavior across workers/restarts; limited production benefit.  
   - Recommended action: move critical caches to Redis or remove if not operationally meaningful.

15. **Observability is insufficiently structured for reliability-first goals**  
   - Severity: **high**  
   - Category: performance/reliability  
   - Evidence: logs are mostly free-text; no clearly defined request outcome taxonomy (success + fallback type + degraded reason).  
   - Impact: hard to diagnose systemic quality drift and fallback frequency.  
   - Recommended action: add structured events/metrics for each pipeline step and fallback path.

### F. Testing / operations

16. **Test suite breadth exists, but coherence and trustworthiness are mixed**  
   - Severity: **high**  
   - Category: testing/operations  
   - Evidence: many tests exist, including fallback/resilience tests, but stale patterns and duplicate/reject artifacts indicate maintenance drift.  
   - Impact: pass/fail signal may not reflect real runtime behavior for critical flows.  
   - Recommended action: define test pyramid by product-critical path; delete or rewrite brittle legacy tests; enforce CI quality gates.

17. **Security/auth model is acceptable for internal-only MVP but weak for multi-team production use**  
   - Severity: **medium**  
   - Category: testing/operations  
   - Evidence: credential auth uses env-based username/password pairs; no RBAC/audit trail in app layer.  
   - Impact: poor accountability and harder incident response as usage grows.  
   - Recommended action: move to SSO-backed auth with role claims and request-level user attribution.

---

## 3) Delete / simplify recommendations

### Remove entirely
- Committed `*.rej` and patch artifacts.
- Legacy/dead test assumptions not aligned with DB-backed runtime behavior.
- Any UI controls not proven to affect baseline directional output quality (after telemetry review).

### Make optional (off by default)
- Advanced/board-pack enrichment panels in the default analysis workflow.
- Opportunity/portfolio “intelligence” features when `USE_DB=false` (hide instead of render + error explanation).
- Non-essential scoring tunables for first-pass screen (e.g., weighting profile in quick mode).

### Move out of critical runtime path
- Board-pack generation and heavy export composition should consume persisted analysis snapshots, not rerun live analysis.
- Optional housing enrichments (tenant/QCT-DDA overlays) should not influence baseline availability.
- Freshness metadata assembly should be asynchronous/cheaply cached, not recomputed in all response paths.

### Simplify substantially
- Split user journeys into:
  1) **Quick Screen** (minimal inputs, stable baseline score, explicit uncertainty)  
  2) **Deep Review** (expanded controls + enrichment + exports)
- Consolidate fallback policy into one shared engine and one consistent UX contract.
- Reduce monolithic backend/frontend files into bounded feature slices.

---

## 4) “If rebuilding today” memo

If rebuilding for the same purpose, I would keep the **three-module directional scoring concept** and the **export/reporting requirement**, but radically simplify runtime architecture around a stable baseline.

### Keep
- Ministry modules (schools/housing/elder care) with common scoring contract.
- Geospatial catchment + demographics + competitor framing.
- Internal workflow with client-ready export capability.

### Simplify
- One canonical baseline data contract per module with strict required fields.
- Deterministic run modes and explicit confidence/fallback encoding.
- One shared analysis orchestration service with immutable result snapshots.

### Remove
- Runtime coupling of board-level narrative generation to every baseline call.
- Low-value feature branching in the primary form.
- Legacy/dead code/test paths and patch residue.

### Redesign
- **Product shape:** clearly split “quick directional screen” from “board-ready analysis package.”
- **Architecture shape:**
  - API layer (thin)
  - Analysis service (pure compute + fallback policy)
  - Data access service (provider adapters + readiness contracts)
  - Reporting service (snapshot-in, PDF/board-pack-out)
- **Reliability model:**
  - baseline always available with clear degraded mode
  - optional enrichments never block baseline
  - strict mode available for board deliverables requiring minimum data health

Yes — the tool should explicitly separate:
1) quick directional feasibility, and  
2) deeper board-ready analysis/export.

---

## 5) Prioritized roadmap

### Phase 0 — stop the bleeding (1–2 weeks)
- Remove repo conflict artifacts and stale test assumptions.
- Add structured response flags: catchment mode, fallback mode, data confidence contributors.
- Standardize error envelope across backend and frontend.

### Phase 1 — reliability and correctness (2–4 weeks)
- Refactor analysis orchestration from `main.py` into service modules.
- Introduce explicit dataset dependency registry (`required` vs `optional`).
- Add structured logs/metrics for fallback events and analysis outcomes.

### Phase 2 — graceful degradation and simplification (3–6 weeks)
- Implement deterministic run modes (`live_only`, `db_with_fallback`, `db_strict`).
- Ensure all optional enrichments are non-blocking and visible as such.
- Move export pipeline to snapshot-based outputs.

### Phase 3 — UX/product cleanup (3–5 weeks)
- Launch two-lane UX: Quick Screen vs Deep Review.
- Hide advanced controls by default; improve explainability panel (inputs used/missing/fallback impacts).
- Improve board-ready flow with explicit readiness checks and disclaimers.

### Phase 4 — architecture simplification / v2 direction (6+ weeks)
- Establish stable service boundaries and typed contracts.
- Consolidate scoring transparency model and calibration governance process.
- Rebuild intelligence/portfolio surfaces only after baseline reliability KPIs are met.

---

## 6) Questions / ambiguities that materially affect architecture

1. What minimum confidence/readiness threshold is required before content is considered “board-ready”?  
2. Should exports be allowed when critical datasets are stale, or only with explicit client-facing caveats?  
3. Is reproducibility (same input -> same result over time) a hard requirement for internal governance?  
4. Which module is the highest-priority “must-never-fail” path operationally?  
5. Do you want strict separation of internal exploratory narratives vs client-deliverable narratives?  
6. What is acceptable latency target for quick screen (p50/p95)?  
7. Is SSO/RBAC required in the next planning horizon, or is shared credential auth acceptable?  
8. Should intelligence/portfolio features remain in the same app or be split into a separate operator console?

