# Dashboard Audit — Phase 0 Reconnaissance

_Date:_ 2026-03-18

## Scope and method

This audit inventories the current React frontend, the backend API surface, core data contracts, and the data already computed for the three ministry modules (Schools, Elder Care, Low-Income Housing). It now also records the current rollout-state implementation details after the additive live dashboard, ZCTA ingest pipeline, and catchment-intersection fixes landed.

### Important findings up front

1. **The repo already has the baseline libraries needed for the requested dashboard direction.**
   - UI styling: **custom Tailwind CSS**, not shadcn or MUI.
   - Charting: **Recharts is already installed**.
   - Mapping: **Leaflet + react-leaflet are already installed**.
2. **The current frontend is a Stage 1/Stage 2 assessment app, not a tabbed analytics workspace.** It has a working analysis flow and several visual panels, but not the unified NAIS-style domain/subtab experience.
3. **An additive ZIP-level dashboard endpoint now exists, but it is still rollout-stage rather than fully mature.**
   - `POST /api/dashboard` now returns ZIP lists, a ZIP `FeatureCollection`, per-ZIP metric maps, drilldowns, and dashboard metadata.
   - The endpoint is additive and does **not** replace `POST /api/analyze`.
   - The current per-ZIP values are dashboard-oriented live mappings built from catchment/module analysis outputs; they should not yet be mistaken for a finalized ZIP-native analytical model.
4. **A ZIP/ZCTA boundary cache pipeline now exists, but production readiness still depends on cache coverage.**
   - `python -m pipeline.cli ingest-zcta` now downloads and caches Census ZCTA boundaries for dashboard use.
   - When the cache is unavailable, the backend now reports `geometry_source=cache_unavailable` and returns an empty ZIP feature collection; the frontend renders an explicit empty/degraded-state message instead of synthetic ZIP tiles.
5. **Projection support is now partially generalized, but not yet fully source-native.**
   - There is now a shared projection-envelope utility with confidence labels / bounds for dashboard time-series payloads.
   - Elder care still retains its original 5-year/10-year senior projection logic.
   - The current dashboard time series are normalized into a shared contract, but some historical series are still derived/back-cast from existing module outputs rather than from a dedicated ZIP-native historical warehouse.
6. **The design-reference path named in the request is missing in this checkout.** `/docs/design-reference/` was not present during audit.

---

## 1. Frontend inventory

### 1.1 Route/page inventory

| Route | File | Status | What it does | API calls |
|---|---|---|---|---|
| `/` | `frontend/src/app/page.tsx` | **Complete for current product, with additive live dashboard rollout** | Main analysis workflow: choose ministry + parameters, submit analysis, render Stage 1/Stage 2 results, then optionally expand the live ZIP dashboard, plus local saved projects and optional DB-backed history panel. | `POST /api/analyze`; additive live dashboard via `POST /api/dashboard`; history interactions via `GET /api/history`, `DELETE /api/history/{id}`. |
| `/intelligence` | `frontend/src/app/intelligence/page.tsx` | **Complete but operational/admin-facing, not end-user dashboard** | Pipeline health, opportunities explorer, weighting methodology, portfolio candidate save actions. | `GET /api/pipeline/status`, `GET /api/opportunities`, `GET /api/scoring/weights`, `GET/PATCH /api/portfolio/workspaces/{id}`. |
| `/login` | `frontend/src/app/login/page.tsx` | **Complete** | Credentials login screen using NextAuth. | Frontend-side auth only through NextAuth route. |
| `/api/health` | `frontend/src/app/api/health/route.ts` | **Complete utility route** | Simple Next.js health JSON. | None. |
| `/api/auth/[...nextauth]` | `frontend/src/app/api/auth/[...nextauth]/route.ts` | **Complete utility route** | Credentials auth provider using `AUTH_USERS`. | None to backend API. |

### 1.2 Shared/component inventory

Below is the effective component catalog used by the current frontend.

| Component | File | Status | Current role | Data shown | Backend/API dependency |
|---|---|---|---|---|---|
| `AnalysisForm` | `frontend/src/components/AnalysisForm.tsx` | **Complete** | Main input form for schools/housing/elder-care analysis and Stage 2 inputs. | Address, ministry settings, drive time, grade/care settings, Stage 2 financial inputs, facility profile, saved projects. | `POST /api/schools/stage2/extract-audits` for audit-PDF extraction; parent submits to `POST /api/analyze`. |
| `ResultsDashboard` | `frontend/src/components/ResultsDashboard.tsx` | **Complete for current app, now with additive live dashboard section** | Orchestrates all result cards/panels after an analysis run and now hosts an expandable live ZIP dashboard beneath the existing Stage 1/Stage 2 content. | Overall score, recommendations, factor cards, map, demographics, gravity, trend, competitors, what-if, Stage 2, export actions, additive live ZIP dashboard. | Depends on `AnalysisResponse` from `POST /api/analyze`; additive live dashboard calls `POST /api/dashboard`; export actions call `POST /api/export/csv`, `POST /api/export/pdf`, `POST /api/export/board-pack`. |
| `LoadingSkeleton` | `frontend/src/components/LoadingSkeleton.tsx` | **Complete** | Loading placeholder during analysis. | Skeleton UI only. | None. |
| `HistoryPanel` | `frontend/src/components/HistoryPanel.tsx` | **Complete** | View/restore/delete historical analyses when DB mode is enabled. | Historical analyses list with filters and restore action. | `GET /api/history`, `DELETE /api/history/{id}`. |
| `ScoreGauge` | `frontend/src/components/ScoreGauge.tsx` | **Complete** | Circular overall feasibility gauge. | Overall score and score band. | Analysis payload only. |
| `MetricCard` | `frontend/src/components/MetricCard.tsx` | **Complete** | Per-factor score card with tooltip/sub-indicators. | Market size, income, competition, family density, occupancy, workforce. | Analysis payload only. |
| `BenchmarkPanel` | `frontend/src/components/BenchmarkPanel.tsx` | **Complete but conditional** | Percentile benchmarking and comparable-market table. | State/national/MSA percentiles and comparable tract markets. | Analysis payload only. |
| `HierarchicalScorePanel` | `frontend/src/components/HierarchicalScorePanel.tsx` | **Complete** | Secondary hierarchical scoring view. | Market opportunity, competitive position, community fit, sustainability risk. | Analysis payload only. |
| `SchoolMap` | `frontend/src/components/SchoolMap.tsx` | **Complete but not choropleth** | Leaflet map showing analysis center, catchment, and competitors. | Point markers + catchment polygon. No ZIP shading. | Analysis payload only (`isochrone_polygon`, competitors, center lat/lon). |
| `DemographicsPanel` | `frontend/src/components/DemographicsPanel.tsx` | **Complete** | Summary demographic cards + compact bar chart. | Population, families, income, seniors, housing burden, projections where available. | Analysis payload only. |
| `PopulationGravityPanel` | `frontend/src/components/PopulationGravityPanel.tsx` | **Complete** | Directional market opportunity for schools. | Direction buckets (N/NE/E/etc.) for school-age/income-qualified population and growth signals. | Analysis payload only. |
| `ElderCareGravityPanel` | `frontend/src/components/ElderCareGravityPanel.tsx` | **Complete** | Directional senior-market breakdown. | Seniors by direction, isolation, poverty-style measures. | Analysis payload only. |
| `HousingGravityPanel` | `frontend/src/components/HousingGravityPanel.tsx` | **Complete** | Directional housing-need breakdown. | Cost-burdened renters, renter households, burden ratios by direction. | Analysis payload only. |
| `TrendPanel` | `frontend/src/components/TrendPanel.tsx` | **Complete but limited** | Simple trend summary card used by the legacy result flow. | % change in school-age pop, income, families; no chart. | Analysis payload only (`trend`). |
| `CompetitorTable` | `frontend/src/components/CompetitorTable.tsx` | **Complete** | Sortable, paginated competitor/facility table for all modules. | Nearby schools, housing projects, or elder care facilities. | Analysis payload only (`competitor_schools`). |
| `WhatIfSimulator` | `frontend/src/components/WhatIfSimulator.tsx` | **Complete for schools only** | Financial what-if tool seeded from analysis results. | Enrollment, tuition, aid, subsidy, estimated operating surplus/deficit. | Analysis payload only. |
| `Stage2Dashboard` | `frontend/src/components/Stage2Dashboard.tsx` | **Complete but compact** | Institutional/operating health panel. | KPI component scores and readiness across schools/housing/elder care. | Analysis payload only (`feasibility_score.stage2`). |
| `LiveModuleDashboard` | `frontend/src/components/dashboard/modules/LiveModuleDashboard.tsx` | **New, rollout-stage** | Fetches live dashboard payloads and embeds the shared module dashboard shell inside the post-analysis user flow. | Loading/error states plus live ZIP choropleth, drilldowns, distributions, and projections. | `POST /api/dashboard`. |

### 1.3 Frontend completeness relative to the requested NAIS-style dashboard

#### Already implemented and reusable
- Leaflet map infrastructure.
- Recharts chart infrastructure.
- Competitor/facility tables.
- Summary score cards and modular result panels.
- Export actions for CSV/PDF/board pack.
- Empty-state handling in multiple panels.
- Shared dashboard shell components (`ChoroplethMap`, `TrendChart`, `DistributionChart`, `ZipDrilldownCard`, `DashboardSidebar`, `TabbedSubview`, `ParameterBar`).
- Additive live dashboard mounting inside the post-analysis workflow.

#### Present, but only partially aligned to the target design
- **Trend handling now exists in both legacy and live-dashboard forms**, but some live time series are still derived/back-cast rather than fully source-native.
- **Map handling now includes ZIP choropleth support**, but production behavior still depends on ZCTA cache coverage and now degrades to an explicit empty-geometry state when ZIP shapes are unavailable.
- **Stage 2 and what-if views remain available outside the live dashboard shell**, which is intentional during rollout but still leaves a split experience.
- **Direction/gravity panels still coexist with the new ZIP dashboard**, so the product currently carries both legacy and rollout-era analytical surfaces at once.

#### Still missing from the target dashboard experience
- Fully production-hardened ZIP analytics backed by ZIP-native source aggregation.
- Guaranteed real ZCTA geometry coverage in every deployed environment.
- Final unification / simplification of the legacy result flow and the new live dashboard flow.
- Broader performance hardening for larger real-world ZIP datasets.

---

## 2. Backend API inventory

### 2.1 Active API endpoints

| Endpoint | Method | Status | Purpose | What it returns | ZIP-level breakdown? | Time-series? | GeoJSON boundaries? |
|---|---|---:|---|---|---|---|---|
| `/api/health` | GET | Active | Basic API/service health. | Service status, version, optional DB health summary. | No. | No. | No. |
| `/api/data-health` | GET | Active | Detailed DB readiness diagnostics. | DB connectivity/readiness metadata. | No. | No. | No. |
| `/api/analyze` | POST | Active | Main ministry analysis endpoint. | `AnalysisResponse` with demographics, competitors, scores, recommendation, optional trend/gravity/forecast, catchment polygon. | **No ZIP breakdown.** Catchment-wide aggregated output. | **Partial.** Trend summary + optional forecast/projection fields only. | **Partial.** Returns `isochrone_polygon` catchment GeoJSON, not ZIP boundaries. |
| `/api/dashboard` | POST | Active, additive rollout endpoint | Module dashboard payload for the new ZIP-level dashboard shell. | ZIP codes, ZIP `FeatureCollection`, per-ZIP metric maps, drilldowns, module-specific time series, highlight cards, projection metadata, freshness metadata, geometry source, and ZIP-selection metadata. | **Yes, partial.** ZIP outputs now exist, with tract-backed rollups when available and area-weighted fallback estimates otherwise. | **Yes, partial.** Shared historical/projected arrays now exist, with confidence labeling. | **Yes, partial.** Uses cached Census ZCTA geometry when available; otherwise returns an empty geometry payload with explicit metadata rather than synthetic ZIP tiles. |
| `/api/analyze/compare` | POST | Active | Compare multiple ministry types at one address. | Ranked ministry summaries only. | No. | No. | No. |
| `/api/export/board-pack` | POST | Active | Export board-pack JSON. | Board narrative pack + freshness/readiness metadata. | No. | No. | No. |
| `/api/export/csv` | POST | Active | Export CSV report. | File stream. | No. | No. | No. |
| `/api/export/pdf` | POST | Active | Export PDF report. | File stream. | No. | No. | No. |
| `/api/schools/stage2/extract-audits` | POST | Active | Extract financial rows from uploaded PDF audits. | Parsed school audit years + warnings. | No. | No. | No. |
| `/api/pipeline/status` | GET | Active | Operational pipeline health dashboard endpoint. | Record counts, pipeline freshness, HUD ingest status. | No. | **Operational timestamps only**, not analytics series. | No. |
| `/api/opportunities` | GET | Active | Find top tract-level opportunities from precomputed scores. | List of tract GEOIDs with factor scores and percentiles. | No ZIPs; **tracts only**. | No. | No. |
| `/api/history` | GET | Active | List saved analysis runs. | Analysis history summary records. | No. | No. | No. |
| `/api/history/{record_id}` | GET | Active | Fetch one saved analysis record. | Saved analysis metadata and summary. | No. | No. | No. |
| `/api/history/{record_id}` | DELETE | Active | Delete saved analysis record. | Empty 204. | No. | No. | No. |
| `/api/scoring/weights` | GET | Active | Return methodology/weights metadata. | Weight definitions by ministry. | No. | No. | No. |
| `/api/portfolio/workspaces` | POST | Active | Create portfolio workspace. | Workspace payload. | No. | No. | No. |
| `/api/portfolio/workspaces/{workspace_id}` | GET | Active | Fetch workspace. | Workspace payload. | No. | No. | No. |
| `/api/portfolio/workspaces/{workspace_id}` | PATCH | Active | Update workspace. | Updated workspace payload. | No. | No. | No. |
| `/api/portfolio/workspaces/{workspace_id}/compare-snapshots` | POST | Active | Save compare results into workspace. | Updated workspace with compare snapshot. | No. | No. | No. |

### 2.2 Analysis endpoint contract details (`/api/analyze`)

The main endpoint returns a broad `AnalysisResponse`, but it is optimized for the current assessment workflow rather than the requested dashboard envelope.

#### What it already includes
- Analysis center and location context.
- Catchment metadata (`radius_miles`, `catchment_minutes`, `catchment_type`).
- **Catchment GeoJSON polygon** via `isochrone_polygon`.
- Module-wide demographics in `demographics`.
- Competitor/facility list in `competitor_schools` (used generically across modules).
- Multi-factor score breakdowns.
- Optional supporting analytical objects:
  - `trend`
  - `population_gravity`
  - `enrollment_forecast`
  - `data_freshness`
  - `benchmark_narrative`
  - `board_report_pack`

#### What `AnalysisResponse` still does **not** include
- ZIP list for the catchment.
- ZIP-level metric dictionary.
- ZIP boundary `FeatureCollection`.
- Per-ZIP income distribution.
- Per-ZIP current vs projected drilldown payloads.
- Shared historical time-series arrays in `{ year, value, projected }[]` shape.
- A consistent dashboard envelope like the one requested in Phase 1D.

#### What is now available additively via `/api/dashboard`
- ZIP list for the catchment.
- ZIP boundary `FeatureCollection`.
- Per-ZIP metric maps.
- ZIP drilldown payloads.
- Shared time-series arrays with projected labeling.
- Dashboard metadata including freshness, projection years, and geometry source.

### 2.3 Spatial data reality check

#### Present today
- **Catchment polygon GeoJSON** for isochrone/radius mode.
- **Census tract geometry** in the database and DB queries.
- Spatial filtering/querying by tract geometry and cached isochrone polygons.
- **A ZCTA boundary cache pipeline** (`ingest-zcta`) that downloads and stores Census ZCTA geometry in a backend-usable cache file.
- **A live dashboard ZIP `FeatureCollection` response path** that prefers cached Census ZCTA geometry.

#### Still missing / partial today
- **True ZIP-native spatial aggregation queries** from tract/census storage into a durable ZIP analytical layer.
- **Guaranteed production geometry coverage** without degraded states; the current implementation still depends on the ZCTA cache, and a cache miss produces an explicit empty geometry payload.
- **DB-backed ZIP centroid / selection infrastructure** beyond the current dashboard payload shaping path.

### 2.4 Time-series / projection reality check

#### Present today
- `trend` object: high-level delta summary between historical and current ACS vintages.
- `enrollment_forecast` schema exists for schools.
- Elder care module computes `seniors_projected_5yr` and `seniors_projected_10yr`.
- Historical tract table support exists in DB (`CensusTractHistory`) for trend calculations.
- A shared projection-envelope helper now exists and adds confidence bands / lower-upper bounds for projected dashboard points.
- `/api/dashboard` now returns shared historical + projected arrays in a dashboard-oriented contract shape.

#### Still missing / partial today
- **Source-native multi-year histories at ZIP grain**; some dashboard series are still derived from existing module outputs rather than a dedicated historical ZIP warehouse.
- **Module-specific forecasting rigor parity** across all domains; the shared contract exists, but not every module is yet backed by equally rich historical source series.

---

## 3. Dependency check (frontend)

### 3.1 Current frontend stack

| Category | Current choice | Evidence | Implication for downstream work |
|---|---|---|---|
| Framework | Next.js 14 / React 18 | `frontend/package.json` | Keep using app router/client components. |
| Styling / UI library | **Custom Tailwind CSS** | Tailwind packages are installed; global styles are Tailwind-based; no shadcn/MUI deps present. | Build the dashboard shell with Tailwind and existing design tokens/patterns. |
| Charting | **Recharts** | Already installed and used in `DemographicsPanel`. | Do **not** add Chart.js. Reuse Recharts for trend/distribution/shared charts. |
| Mapping | **Leaflet + react-leaflet** | Already installed and used by `SchoolMap`. | Do **not** add Mapbox unless requirements change. Use Leaflet for choropleth work. |
| Iconography | `lucide-react` | Already installed and used extensively. | Reuse for toolbar/actions. |
| Auth | `next-auth` credentials | Already configured. | No impact on dashboard visualization architecture. |

### 3.2 Library decision outcome for Phase 1

Because the project **already includes Recharts and Leaflet**, downstream implementation should use:
- **Tailwind + custom components** for UI shell.
- **Recharts** for `TrendChart` and `DistributionChart`.
- **Leaflet/react-leaflet** for `ChoroplethMap`.

No duplicate charting or mapping library is justified at this stage.

---

## 4. Data availability matrix by module

Legend:
- **Available** = already computed and available in current backend outputs or clearly derivable from present data path.
- **Partial** = some signals exist, but not in the requested dashboard-ready shape/granularity.
- **Missing** = not currently computed/exposed.

### 4.1 Schools module

| Dashboard need | Status | Notes |
|---|---|---|
| Catchment-wide school-age population | **Available** | Returned in `demographics`. |
| Grade/gender-scoped school-age population | **Available** | Already part of school analysis logic. |
| Families with children | **Available** | Returned in `demographics`. |
| Median household income | **Available** | Returned in `demographics`. |
| Income bracket distribution | **Available (catchment-level)** | Backend aggregates B19001-style distribution; not yet exposed as dashboard-specific bucket payload. |
| High-income household/family signal | **Available (catchment-level, partial)** | Backend computes high-income households and uses addressable-market logic, but not a dedicated NAIS-style “$200K+ families by ZIP” response. |
| Catholic/addressable market estimation | **Available** | Current scoring includes addressable market and Catholic boost outputs. |
| Private enrollment rate / pipeline ratio | **Available** | Present in demographics/scoring model. |
| Competitor schools list | **Available** | Already rendered in map/table. |
| Competitor map points | **Available** | Leaflet point map exists. |
| Benchmark/comparable markets | **Available** | Returned via analysis response. |
| Enrollment forecast | **Partial** | Shared schema exists and dashboard payloads now normalize projection views, but not all school-series inputs are source-native. |
| Historical trend series | **Partial** | Dashboard payloads now expose shared annual series, but some are derived/back-cast from existing analysis outputs rather than a dedicated historical ZIP store. |
| ZIP-level school-age population | **Partial** | Live dashboard now returns per-ZIP values, but current mapping is dashboard-oriented rather than finalized ZIP-native aggregation. |
| ZIP-level income distribution | **Partial** | Live dashboard now returns ZIP drilldown distributions, but they are not yet backed by a full ZIP-native census warehouse. |
| ZIP-level choropleth metrics | **Partial** | ZIP `FeatureCollection` and metric properties now exist via `/api/dashboard`, with cached Census geometry preferred and an explicit empty-geometry degraded state when cache coverage is missing. |
| Diversity / race-ethnicity analytics | **Missing/unclear in current UI contract** | Not visible in current `AnalysisResponse` schema used by frontend. |
| Financial gap per ZIP | **Missing** | No per-ZIP affordability gap output. |
| Competitor table with tuition/typology enrichment for dashboard shell | **Partial** | Table exists, but NAIS-style deeper competitor analytics are not yet delivered as a dedicated endpoint. |

### 4.2 Elder Care module

| Dashboard need | Status | Notes |
|---|---|---|
| Senior population (65+, 75+) | **Available** | Returned in demographics. |
| Seniors living alone / poverty proxy | **Available** | Returned in demographics and directional gravity data. |
| Senior target-population logic | **Available** | Current scoring uses mission vs market mode logic. |
| Median income | **Available** | Catchment-level median only. |
| Elder care facilities list | **Available** | CMS-derived facilities surfaced as competitor list. |
| Facility ratings / occupancy / bed counts | **Available** | Present on facility payloads where available. |
| 5-year and 10-year senior projections | **Available (module-specific)** | Simple survival-rate projections already implemented. |
| Directional senior-market breakdown | **Available** | Gravity panel data exists. |
| Historical trend series | **Partial** | Live dashboard now exposes elder-care series in the shared trend contract, but the history remains less source-rich than a dedicated elder ZIP warehouse would provide. |
| Income distribution for senior households | **Missing** | Only median income is used; no age-specific bucket distribution. |
| Medicare/Medicaid mix indicators at market level | **Missing** | Stage 2 accepts payer mix inputs, but no market analytics endpoint provides catchment payer mix distribution. |
| ZIP-level senior population | **Partial** | Live dashboard now returns per-ZIP senior metrics, but the values are still dashboard-oriented mappings rather than finalized ZIP-native aggregation. |
| ZIP-level underserved-area detection | **Partial** | Dashboard quality-gap views now exist, but they are still heuristic and not yet a dedicated underserved-area model. |
| ZIP-level facility density / quality gap map | **Partial** | ZIP choropleth metric payloads now exist for elder-care dashboards, though not yet from a full ZIP-native facility-density model. |
| Actuarial cohort projections by ZIP (65–74 / 75–84 / 85+) | **Missing/Partial** | Shared projections now exist, but they are not yet a true cohort-by-ZIP actuarial model. |

### 4.3 Low-Income Housing module

| Dashboard need | Status | Notes |
|---|---|---|
| Cost-burdened renter households | **Available** | Core market-size metric in demographics/scoring. |
| Renter households / burden ratio | **Available** | Present in demographics and directional housing view. |
| Median household income | **Available** | Catchment-level median only. |
| HUD-eligible households estimate | **Available** | Estimated from income distribution in housing module logic. |
| LIHTC / Section 202 competitor inventory | **Available** | Competitor list already assembled. |
| QCT / DDA counts | **Available** | Present in demographics/housing context. |
| Housing competitor map points | **Available** | Reuses current map/table pattern. |
| Income distribution | **Partial** | Dashboard payloads now expose housing-oriented distribution buckets and ZIP drilldown distributions, but not yet from a dedicated ZIP-native income warehouse. |
| Housing stock / unit counts | **Partial** | Competing project units are present; broader community housing stock analytics are not packaged for dashboard use. |
| Poverty-rate trend | **Missing/Partial** | Some poverty-related counts exist, but no dedicated trend endpoint. |
| Waitlists | **Missing** | No waitlist source or field present. |
| Vacancy/occupancy data | **Partial** | Limited competitor/project fields exist; not a normalized dashboard-wide resource endpoint. |
| Housing gap estimate | **Missing** | No explicit gap calculation endpoint. |
| ZIP-level need assessment | **Partial** | Live dashboard now returns per-ZIP burden / eligibility views, but the underlying model remains an additive dashboard mapping rather than finalized ZIP-native need aggregation. |
| ZIP-level choropleth of burden / poverty / stock | **Partial** | ZIP choropleth payloads now exist for housing dashboards, though still rollout-stage and geometry-cache-dependent. |
| Population growth / migration / diversity dashboard series | **Missing/Partial** | Shared trend payloads now exist, but migration/diversity-specific dashboard series are still not fully implemented. |

---

## 5. Gaps vs. requested Phase 1 shared infrastructure

### 5.1 Shared component readiness

| Requested shared component | Current equivalent? | Gap assessment |
|---|---|---|
| `ChoroplethMap` | Shared dashboard component now exists and is live-wired additively | **Addressed, rollout-stage** — live payloads now feed ZIP choropleths, and the component now handles empty-geometry states explicitly when cache coverage is missing. |
| `TrendChart` | Shared dashboard component now exists and is live-wired additively | **Addressed, rollout-stage** — shared historical/projected charting is now present, though some histories remain derived/back-cast. |
| `DistributionChart` | Shared dashboard component now exists and is live-wired additively | **Addressed, rollout-stage**. |
| `ZipDrilldownCard` | Shared dashboard component now exists and is live-wired additively | **Addressed, rollout-stage**. |
| `TabbedSubview` | Shared dashboard component now exists | **Addressed**. |
| `DashboardSidebar` | Shared dashboard component now exists | **Addressed**. |
| `ParameterBar` | Shared dashboard component now exists | **Addressed** — the live dashboard now uses a dashboard-oriented parameter bar separate from the main workflow form. |

### 5.2 Data/model readiness

| Requested capability | Current state | Gap assessment |
|---|---|---|
| ZIP boundary FeatureCollection | Partial | **Reduced gap** — additive dashboard payloads now return ZIP geometry selected by catchment/ZCTA intersection, but quality still depends on ZCTA cache availability. |
| Per-ZIP metric values | Partial | **Reduced gap** — per-ZIP maps now exist and now prefer tract-backed ZIP rollups, though the logic is still not a finalized ZIP-native aggregation layer. |
| Shared dashboard response envelope | Addressed | **Core contract now exists** via `/api/dashboard`. |
| Historical + projected time series arrays | Partial | **Reduced gap** — shared arrays now exist, but source-native depth is still uneven by module. |
| Projection confidence metadata | Partial | **Reduced gap** — confidence bands/labels are now returned, but forecasting rigor remains uneven. |
| ZIP drilldown current vs projected snapshot | Partial | **Reduced gap** — live drilldowns now exist, but they are still rollout-stage. |

---

## 6. Architectural implications and recommended implementation order

### 6.1 What should be preserved

The current assessment flow is functional and should remain intact:
1. Analysis form submission.
2. `POST /api/analyze` backend orchestration.
3. Result dashboard rendering.
4. Existing exports/history/intelligence console.

The overhaul should therefore be **additive**, not a replacement of the current `ResultsDashboard` path until new dashboards are stable.

### 6.2 Lowest-risk sequence after review

1. **Define the new dashboard API contracts first.**
   - Introduce new dashboard-specific response envelopes rather than overloading `AnalysisResponse` too aggressively.
2. **Build shared frontend analytics components against mock data.**
   - Recharts + Leaflet + Tailwind only.
3. **Add ZIP/ZCTA ingest + aggregation pipeline.**
   - This is the biggest missing data foundation for the requested design.
4. **Implement shared projection utilities.**
   - Normalize historical and projected series format across modules.
5. **Deliver one module end-to-end first (Schools is the best candidate).**
   - Schools already has the richest market, competitor, and affordability concepts.
6. **Then parallelize Elder Care and Housing dashboards.**

---

## 7. Key risks and blockers to flag before implementation

### Blocker A — ZIP boundary coverage is not yet guaranteed
The requested choropleth and drilldown experience now has a ZCTA ingest/cache path, but production behavior still depends on that cache being populated. Without it, the live dashboard now degrades to an explicit empty-geometry state rather than synthetic ZIP tiles.

### Blocker B — Current data model is tract/catchment-first, not ZIP-first
The backend’s demographic aggregation and many current score calculations still operate at catchment-wide tract aggregation. The live dashboard now maps those outputs into ZIP drilldowns, but a finalized ZIP-native analytical layer is still missing.

### Blocker C — Projection framework is generalized at the contract level, but not yet at the source-data level
The app now has a shared dashboard projection envelope and contract shape, but not every module is yet backed by equally strong historical source series or ZIP-native forecasting inputs.

### Blocker D — Reference design assets are not in this checkout
The prompt refers to `/docs/design-reference/`, but that directory was absent during audit. The four screenshots included in the task are therefore the only locally available design reference observed during this pass.

---

## 8. Bottom-line readiness assessment

### What is already strong
- The current product has a stable assessment workflow.
- The frontend stack already includes the right mapping and charting libraries.
- The backend already has catchment creation, tract aggregation, competitor retrieval, and some trend/projection hooks.

### What is not yet ready for the target dashboard
- Guaranteed cached Census ZIP boundary coverage in deployed environments.
- A true ZIP-native analytical layer underneath the new dashboard payloads.
- Fully source-native module forecasting and richer ZIP-level histories.
- Performance hardening for large/real geometry and broader Phase 3 regression coverage.

### Overall conclusion
The repo is **well-positioned for the dashboard overhaul**, but the requested experience is **not a thin frontend reskin**. It requires new shared components, new dashboard-specific API contracts, ZIP/ZCTA data ingest, and a generalized projection layer. The safest path is to keep the existing assessment flow intact and build the new dashboard experience alongside it.

---

## 9. Implementation status update (after additive live dashboard wiring)

This section records what has been addressed since the original Phase 0 audit and what still needs to happen before Phase 3 should begin.

### 9.1 Addressed so far

#### Addressed from Phase 1 / shared infrastructure
- **Shared frontend analytics components now exist** in the repo for the requested dashboard direction:
  - `ChoroplethMap`
  - `TrendChart`
  - `DistributionChart`
  - `ZipDrilldownCard`
  - `TabbedSubview`
  - `DashboardSidebar`
  - `ParameterBar`
- **Client-side download affordances exist** for PNG and CSV in the shared dashboard components.
- **Dashboard contract scaffolding exists** in `backend/docs/dashboard_api_contracts.md`.
- **A shared projection-envelope utility now exists** in `backend/services/projections.py` and returns projected bounds / confidence metadata for dashboard payloads.

#### Addressed from Phase 2 / rollout wiring
- **Module-specific dashboard preview routes now exist** for:
  - Schools
  - Elder Care
  - Low-Income Housing
- **A module gallery / preview entry point exists** under `/dashboard-preview`.
- **Module-specific sidebar/domain structures have been prototyped** using the shared component system.
- **The existing assessment flow has been preserved**; the new dashboard work remains additive rather than replacing Stage 1 / Stage 2.
- **A live dashboard endpoint now exists**:
  - `POST /api/dashboard`
  - returns ZIP lists, ZIP `FeatureCollection`, per-ZIP metric maps, drilldowns, highlight cards, projection years, freshness metadata, and geometry-source metadata
- **A Census ZCTA ingest/cache command now exists**:
  - `python -m pipeline.cli ingest-zcta`
  - stores cached ZCTA geometry for backend dashboard use
- **The full ingest workflow now includes ZCTA ingestion** so `python -m pipeline.cli ingest-all` refreshes ZIP geometry alongside the other dashboard dependencies.
- **Catchment ZIP selection is now intersection-based rather than seed-based.**
  - The dashboard prefers actual catchment/ZCTA overlap and caps the selected ZIP set with overlap-based weighting metadata.
- **The dashboard now prefers tract-backed ZIP rollups when available.**
  - When tract assignment cannot be loaded, the service falls back to area-weighted estimates rather than synthetic geometry.
- **The live module dashboard is now mounted into the post-analysis user flow** as an expandable section beneath the existing result dashboard.

### 9.2 Still not addressed

The original blockers have been reduced, but several important maturity gaps remain:

- **ZIP geometry coverage is still cache-dependent.**
  - If the ZCTA cache is not populated, the live dashboard reports `cache_unavailable`, returns no ZIP features, and shows an explicit degraded-state message in the frontend.
- **Per-ZIP metrics are improved but still not a finalized ZIP-native warehouse.**
  - The dashboard now uses tract-backed ZIP rollups when available and area-weighted fallbacks otherwise, which is materially better than seed-based mappings but still not the same as a fully durable ZIP analytical layer.
- **Historical and projected dashboard series are normalized, but not yet uniformly source-native.**
  - Some series are still derived/back-cast from current module outputs.
- **Performance work for large real-world GeoJSON payloads is still incomplete.**
  - Geometry simplification exists at cache-write time, but viewport-aware loading and broader render hardening are still pending.
- **Phase 3 regression coverage is still too light for the expanded dashboard surface.**
  - The new dashboard service has focused unit tests, but broader backend/frontend integration coverage is still needed.

### 9.3 What needs to be addressed next before moving to Phase 3

Before Phase 3 (testing/performance hardening) begins, the following Phase 1/2 implementation work still needs to be completed:

1. **Promote the current ZIP layer from rollout-grade to production-grade**
   - Ensure Census ZCTA cache population is part of normal deployment/refresh workflows.
   - Keep dependable geometry coverage as part of deployment/refresh workflows so the empty-geometry degraded state stays rare.
   - Continue tightening catchment-level ZIP selection around actual catchment/ZCTA intersection and tract assignment quality.

2. **Replace dashboard-oriented ZIP mappings with finalized ZIP-native aggregation**
   - Schools: affordability, enrollment, student-body, and competitor views backed by live data.
   - Elder Care: senior cohort, facilities, quality gaps, and 5-/10-year projections backed by live data.
   - Housing: burden, income thresholds, existing resources, and demographic trend views backed by live data.

3. **Strengthen projection integration**
   - Replace derived/back-cast series where possible with richer source-native histories.
   - Preserve confidence bands / labels so projections are never presented as fact.
   - Align module forecasting rigor more consistently across schools, elder care, and housing.

4. **Performance prerequisites**
   - Add viewport-aware loading and/or further simplification for larger ZIP geometry payloads.
   - Expand memoization / render controls for charts and drilldowns under realistic data volume.
   - Validate cache strategy for ZIP GeoJSON and dashboard responses under repeated real-user traffic.

5. **Phase 3 test-readiness**
   - Add broader backend endpoint coverage for `/api/dashboard`.
   - Add integration coverage around live dashboard rendering and degraded states.
   - Add broader regression coverage for cache-miss / empty-geometry / partial-data cases.

### 9.4 Phase gate recommendation

**Do not treat the project as fully ready for Phase 3 yet.**
The repo has now crossed an important threshold: the live dashboard endpoint, additive frontend wiring, ZCTA ingest path, and shared projection-envelope contract are in place. However, the project still needs stronger geometry coverage, truer ZIP-native aggregation, more source-native time series, and broader regression/performance hardening before it should be considered genuinely Phase 3-ready.
