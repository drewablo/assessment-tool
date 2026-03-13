# Executive Summary
The codebase is in generally solid shape for an MVP-style assessment platform, with clear module boundaries and workable scoring flows across Schools, Elder Care, and Low-Income Housing. The largest immediate risks are around **state persistence and caching consistency** rather than catastrophic scoring failures: several external sources are fetched live without a shared cache strategy, and one user-facing history restore flow is currently unreliable. The backend is safe to extend, but extension work will be slower and riskier unless caching and state management are standardized first. No critical arithmetic defects were confirmed in core scoring formulas during this pass, but there are medium/high reliability and operational issues that should be addressed before scaling usage. Overall: deployable for controlled internal use, but not yet robust enough for high-volume or multi-worker production without targeted hardening.

# Scope Reviewed
## Backend areas reviewed
- API layer: `backend/main.py`, `backend/api/*` (analysis, schools, school_stage2, census, geocoder, isochrone, reports, pdf_report, benchmarks, bls_workforce, hierarchical scoring).
- Ministry modules: `backend/modules/schools.py`, `backend/modules/elder_care.py`, `backend/modules/housing.py`, shared module framework/base.
- Competitor/data fetchers: `backend/competitors/cms_care_compare.py`, `backend/competitors/hud_lihtc.py`.
- DB/caching/data access: `backend/db/connection.py`, `backend/db/models.py`, `backend/db/queries.py`, `backend/db/demographics.py`.
- Shared models/utilities: `backend/models/schemas.py`, `backend/utils.py`.
- Data pipeline/config references relevant to runtime behavior: `backend/pipeline/*.py`.

## Frontend areas reviewed
- App pages/routes: `frontend/src/app/page.tsx`, `frontend/src/app/intelligence/page.tsx`, auth/health API route wrappers.
- Shared API/types layer: `frontend/src/lib/api.ts`, `frontend/src/lib/types.ts`, `frontend/src/lib/intelligence.js`.
- UI components/hooks: `AnalysisForm`, `ResultsDashboard`, `HistoryPanel`, `Stage2Dashboard`, `CompetitorTable`, `DemographicsPanel`, `SchoolMap`, `WhatIfSimulator`, and related shared display components.
- Supporting config: Next/Tailwind/TS config files.

## Modules reviewed
- Schools
- Elder Care
- Low-Income Housing
- Shared API/caching/scoring/data-fetching and shared UI/types infrastructure

## Unavailable or excluded
- No unavailable files encountered in in-scope backend/frontend source trees.
- Binary/reference artifacts (e.g., PDF source report files) were not reviewed as executable logic.

# Critical Bugs
1. **History restore does not reliably reconstruct original request context**  
   - Severity: **High**  
   - File: `frontend/src/components/HistoryPanel.tsx`  
   - Line: around `handleRestore`  
   - Impact: users may click restore and get only partial/defaulted request parameters, leading to misleading reruns.  
   - Recommended fix direction: include `request_params` in history list API response and map it explicitly in `AnalysisHistoryRecord`, then restore directly from that typed field.

# Caching Audit
## External data source inventory
| Source | Referenced in | Access method | Cached? | Cache implementation | Key strategy | TTL/freshness | Invalidation | Fallback on miss/failure | Consistency notes |
|---|---|---|---|---|---|---|---|---|---|
| Census Geocoder | `backend/api/geocoder.py`, `backend/main.py` | Live HTTP GET | **No** | None | N/A | N/A | N/A | returns `None` and 422 upstream | One-off live call path; no shared cache |
| Census ACS 2022/2017 | `backend/api/census.py`, `backend/main.py` | Live HTTP GET to ACS API | **Partial** (DB mode only) | In DB mode, pre-ingested tract tables are read; live mode uncached | DB keys by tract geoid | Pipeline freshness target (45 days) | pipeline reruns overwrite | county-level fallback in some paths | Split behavior by USE_DB causes divergence |
| Census TIGER tract geometry | `backend/api/census.py` | Live HTTP GET to TIGER ArcGIS | **No** in live mode | None | N/A | N/A | N/A | empty tract set fallback | Additional uncached external hit in live mode |
| OpenRouteService Isochrone | `backend/api/isochrone.py`, `backend/main.py`, `backend/db/queries.py` | Live HTTP POST | **Yes** (DB mode) | `isochrone_cache` table | nearest point within tolerance + drive minutes | **No explicit TTL** | none except new insertions | radius fallback when ORS missing/fails | Cache exists but freshness policy missing |
| NCES PSS (Schools) | `backend/api/schools.py`, pipeline ingest | ZIP download + local CSV | **Yes** | Local filesystem CSV + in-memory DataFrame | single file path + module global DF | file-size validity only | delete/replace file | returns empty school list if unavailable | Local cache pattern differs from Redis/DB caches |
| CMS Care Compare provider data | `backend/competitors/cms_care_compare.py` | CSV bulk or paginated JSON API | **Yes** | Local filesystem CSV | file path + file-size validity | file-size validity only | replace file by redownload | Overpass fallback if no facilities | No TTL; download-on-demand in request path |
| CMS PBJ staffing | `backend/competitors/cms_care_compare.py` | intended download (disabled) | Effectively **No** | Stubbed/disabled loader | N/A | N/A | N/A | continues without staffing enrichment | Inconsistent with provider-file handling |
| Assisted living dataset (onefact GitHub) | `backend/competitors/cms_care_compare.py` | CSV download | **Yes** | Local filesystem CSV | file path + size check | file-size validity only | replace file | continue with CMS-only data | separate one-off cache logic |
| Overpass OSM elder-care fallback | `backend/competitors/cms_care_compare.py` | Live HTTP POST | **No** | None | N/A | N/A | N/A | returns empty list on failure | live-only fallback path |
| BLS QCEW workforce | `backend/api/bls_workforce.py`, elder care module | Live HTTP GET CSV | **No** | None | N/A | N/A | N/A | returns neutral score (50) with note | no memoization despite repeated county queries |
| HUD LIHTC projects | `backend/competitors/hud_lihtc.py`, `backend/pipeline/ingest_housing.py` | local CSV read per request (runtime) + pipeline API ingest | **Partial** | runtime reads static file; no memory cache | file path | none | external file replacement | empty list if file missing | inconsistent with schools/CMS in-memory reuse |
| Redis analysis response cache | `backend/main.py` | Redis get/setex | **Yes** | key-value cache | hash(ministry+address+params) | env TTL default 24h | expiry only | bypass cache if Redis unavailable | transient outage disables retries for process |

## Cache coverage analysis
- Strongest cache coverage: top-level `/api/analyze` response cache and local NCES/CMS file caches.
- Weakest coverage: geocoding, live ACS/TIGER paths, Overpass fallback, BLS workforce, and LIHTC runtime reads.
- DB mode and live mode behave differently, creating materially different cache behavior and latency.

## Inconsistency analysis
- Caching patterns vary across Redis, DB tables, local files, and no-cache one-offs with no shared abstraction.
- TTL strategy is inconsistent: Redis has TTL, but DB isochrone and local file caches mostly do not.
- Error handling for cache failures is generally graceful but observability is weak (limited hit/miss metrics).

## Invalidation analysis
- Redis: time-based invalidation only.
- DB isochrone: no explicit invalidation or age checks.
- Local files (NCES/CMS/AL): validity is size-based, not freshness-based.
- Pipeline-refreshed DB tables: freshness implied by pipeline run metadata, but not uniformly enforced at query points.

## Highest-risk caching gaps
1. No cache for geocoder + live ACS/TIGER path (high repeated external dependency risk).
2. Redis reconnection sentinel disables retries after one outage.
3. Isochrone DB cache has no TTL/freshness guard.
4. LIHTC CSV repeatedly re-read from disk without in-memory memoization.
5. Lack of standardized cache instrumentation (hit/miss/error counters).

# Findings by Lens
## [BUG]
1. **History restore reads wrong payload shape**  
   - Severity: High  
   - Confidence: Confirmed  
   - File: `frontend/src/components/HistoryPanel.tsx`  
   - Line(s): `handleRestore` block  
   - Summary: restore attempts to read `stored.request_params` from `result_summary`, but `result_summary` is not the canonical request payload.  
   - Why it matters: users can unknowingly rerun with defaults/partial params.  
   - Fix direction: return and consume `request_params` explicitly in list/history contract.

## [BACKEND]
1. **Portfolio workspace storage is process-local in-memory state**  
   - Severity: High  
   - Confidence: Confirmed  
   - File: `backend/main.py`  
   - Summary: `_PORTFOLIO_WORKSPACES` is non-persistent and not shared across workers.  
   - Why it matters: data loss on restart and inconsistent behavior behind load-balanced workers.  
   - Fix direction: migrate to DB-backed storage with optimistic versioning.

## [FRONTEND]
1. **History record type omits request_params, encouraging unsafe casting**  
   - Severity: Medium  
   - Confidence: Confirmed  
   - File: `frontend/src/lib/types.ts`  
   - Summary: interface does not model request payloads needed for restore workflows.  
   - Why it matters: increases type drift and runtime restore bugs.  
   - Fix direction: update type to include optional `request_params` and align API.

## [CACHE]
1. **Redis client disables all future reconnect attempts after one failure**  
   - Severity: High  
   - Confidence: Confirmed  
   - File: `backend/main.py`  
   - Summary: sentinel `False` is cached process-wide.  
   - Why it matters: long-running process can run uncached for hours after brief outage.  
   - Fix direction: add retry/backoff and periodic re-probe.

2. **Isochrone cache has no TTL/freshness policy**  
   - Severity: Medium  
   - Confidence: Confirmed  
   - File: `backend/db/queries.py`  
   - Summary: nearest cached polygon reused indefinitely.  
   - Why it matters: persistent stale boundaries if provider/model changes.  
   - Fix direction: add `created_at` age filter and/or scheduled cleanup.

3. **Geocoder + live demographics are uncached one-off external calls**  
   - Severity: Medium  
   - Confidence: Confirmed  
   - Files: `backend/api/geocoder.py`, `backend/api/census.py`  
   - Summary: repeated identical inputs always call external APIs.  
   - Why it matters: avoidable latency/rate-limit exposure.  
   - Fix direction: add short-lived request cache (Redis/DB) keyed by normalized address and tract set.

## [PERF]
1. **Compare endpoint bypasses analyze-level cache and runs sequentially**  
   - Severity: Medium  
   - Confidence: Confirmed  
   - File: `backend/main.py`  
   - Summary: each compare request recomputes all ministry analyses even for repeated requests.  
   - Why it matters: multiplied response time and upstream API pressure.  
   - Fix direction: reuse analyze cache or fan out concurrent module runs with shared geocode/demographics context.

2. **LIHTC CSV reloaded on every request**  
   - Severity: Medium  
   - Confidence: Confirmed  
   - File: `backend/competitors/hud_lihtc.py`  
   - Summary: no in-memory DataFrame reuse.  
   - Why it matters: unnecessary disk IO and parsing overhead under load.  
   - Fix direction: mirror NCES/CMS module-level cache pattern.

## [DEBT]
- No additional material debt-only findings beyond caching/state consistency patterns above.

## [UX]
- No standalone high-confidence UX defects were confirmed in reviewed files; UX issues observed are primarily downstream effects of restore/caching reliability gaps.

# Cross-Cutting Patterns
1. **Caching strategy fragmentation** across Schools, Elder Care, Housing, and shared API layer (Redis vs DB vs file vs none) increases regression risk when adding new sources.
2. **Runtime data-source behavior diverges by environment mode** (`USE_DB` vs live) with different freshness and fallback semantics, affecting all three ministry modules.
3. **API/Type contract drift** between backend history payloads and frontend restore logic affects shared infrastructure and especially school workflow continuity.

# Modules That Passed Review Cleanly
- `backend/modules/frameworks.py` (shared benchmark/hierarchical scaffolding looked internally consistent).
- `backend/modules/base.py` and `backend/modules/__init__.py` (clean, minimal abstractions).
- `frontend/src/components/ScoreGauge.tsx`, `MetricCard.tsx`, `LoadingSkeleton.tsx`, `TrendPanel.tsx` (no material issues found).
- `frontend/src/app/layout.tsx`, `frontend/src/app/api/health/route.ts` (simple and clean).

# Recommended Fix Priority
## Phase 1 (deploy blockers)
1. Fix history restore contract and typed mapping (`request_params` handling).
2. Replace in-memory portfolio workspace storage for production deployments.

## Phase 2 (correctness and data integrity)
1. Add Redis reconnect policy (remove permanent disable-once sentinel behavior).
2. Add isochrone cache TTL/freshness constraints.

## Phase 3 (maintainability and consistency)
1. Standardize external-source caching abstractions and instrumentation.
2. Unify cache/freshness semantics between live mode and DB mode.

## Phase 4 (UX and performance polish)
1. Optimize compare endpoint with cache reuse/concurrency.
2. Add in-memory LIHTC dataset cache.
3. Consider lightweight caching for geocoding and live demographics calls.
