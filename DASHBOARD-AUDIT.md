# Dashboard Audit — Phase 0 Reconnaissance

_Date:_ 2026-03-18

## Scope and method

This audit inventories the current React frontend, the backend API surface, core data contracts, and the data already computed for the three ministry modules (Schools, Elder Care, Low-Income Housing). It is intended to be the required Phase 0 gate before any dashboard overhaul work begins.

### Important findings up front

1. **The repo already has the baseline libraries needed for the requested dashboard direction.**
   - UI styling: **custom Tailwind CSS**, not shadcn or MUI.
   - Charting: **Recharts is already installed**.
   - Mapping: **Leaflet + react-leaflet are already installed**.
2. **The current frontend is a Stage 1/Stage 2 assessment app, not a tabbed analytics workspace.** It has a working analysis flow and several visual panels, but not the unified NAIS-style domain/subtab experience.
3. **There are no dedicated ZIP-level dashboard endpoints today.** The backend returns one analysis payload per request, centered on a catchment, with tract-level aggregation and competitor lists.
4. **There is no ZIP boundary GeoJSON pipeline today.** The backend stores/uses **tract geometry** and **catchment isochrone GeoJSON**, but not ZIP/ZCTA boundaries.
5. **Projection support is partial and module-specific.**
   - Elder care has simple survival-rate-based 5-year/10-year senior projections.
   - Schools has an enrollment forecast object in the shared schema.
   - A general time-series/projection engine for all dashboard domains does **not** exist yet.
6. **The design-reference path named in the request is missing in this checkout.** `/docs/design-reference/` was not present during audit.

---

## 1. Frontend inventory

### 1.1 Route/page inventory

| Route | File | Status | What it does | API calls |
|---|---|---|---|---|
| `/` | `frontend/src/app/page.tsx` | **Complete for current product** | Main analysis workflow: choose ministry + parameters, submit analysis, render results dashboard, local saved projects, optional DB-backed history panel. | `POST /api/analyze`; history interactions via `GET /api/history`, `DELETE /api/history/{id}`. |
| `/intelligence` | `frontend/src/app/intelligence/page.tsx` | **Complete but operational/admin-facing, not end-user dashboard** | Pipeline health, opportunities explorer, weighting methodology, portfolio candidate save actions. | `GET /api/pipeline/status`, `GET /api/opportunities`, `GET /api/scoring/weights`, `GET/PATCH /api/portfolio/workspaces/{id}`. |
| `/login` | `frontend/src/app/login/page.tsx` | **Complete** | Credentials login screen using NextAuth. | Frontend-side auth only through NextAuth route. |
| `/api/health` | `frontend/src/app/api/health/route.ts` | **Complete utility route** | Simple Next.js health JSON. | None. |
| `/api/auth/[...nextauth]` | `frontend/src/app/api/auth/[...nextauth]/route.ts` | **Complete utility route** | Credentials auth provider using `AUTH_USERS`. | None to backend API. |

### 1.2 Shared/component inventory

Below is the effective component catalog used by the current frontend.

| Component | File | Status | Current role | Data shown | Backend/API dependency |
|---|---|---|---|---|---|
| `AnalysisForm` | `frontend/src/components/AnalysisForm.tsx` | **Complete** | Main input form for schools/housing/elder-care analysis and Stage 2 inputs. | Address, ministry settings, drive time, grade/care settings, Stage 2 financial inputs, facility profile, saved projects. | `POST /api/schools/stage2/extract-audits` for audit-PDF extraction; parent submits to `POST /api/analyze`. |
| `ResultsDashboard` | `frontend/src/components/ResultsDashboard.tsx` | **Complete for current app** | Orchestrates all result cards/panels after an analysis run. | Overall score, recommendations, factor cards, map, demographics, gravity, trend, competitors, what-if, Stage 2, export actions. | Depends on `AnalysisResponse` from `POST /api/analyze`; export actions call `POST /api/export/csv`, `POST /api/export/pdf`, `POST /api/export/board-pack`. |
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
| `TrendPanel` | `frontend/src/components/TrendPanel.tsx` | **Complete but limited** | Simple trend summary card. | % change in school-age pop, income, families; no chart. | Analysis payload only (`trend`). |
| `CompetitorTable` | `frontend/src/components/CompetitorTable.tsx` | **Complete** | Sortable, paginated competitor/facility table for all modules. | Nearby schools, housing projects, or elder care facilities. | Analysis payload only (`competitor_schools`). |
| `WhatIfSimulator` | `frontend/src/components/WhatIfSimulator.tsx` | **Complete for schools only** | Financial what-if tool seeded from analysis results. | Enrollment, tuition, aid, subsidy, estimated operating surplus/deficit. | Analysis payload only. |
| `Stage2Dashboard` | `frontend/src/components/Stage2Dashboard.tsx` | **Complete but compact** | Institutional/operating health panel. | KPI component scores and readiness across schools/housing/elder care. | Analysis payload only (`feasibility_score.stage2`). |

### 1.3 Frontend completeness relative to the requested NAIS-style dashboard

#### Already implemented and reusable
- Leaflet map infrastructure.
- Recharts chart infrastructure.
- Competitor/facility tables.
- Summary score cards and modular result panels.
- Export actions for CSV/PDF/board pack.
- Empty-state handling in multiple panels.

#### Present, but only partially aligned to the target design
- **Trend handling exists, but only as a compact summary card**; there is no general-purpose multi-series trend chart with historical vs projected styling.
- **Map handling exists, but only as point/catchment mapping**; there is no choropleth ZIP/ZCTA shading, legend, metric toggle, or ZIP click drilldown.
- **Stage 2 and what-if views exist**, but they are not embedded in a tabbed analytical shell.
- **Direction/gravity panels exist**, but they are not equivalent to a ZIP drilldown or NAIS-style subtab analytics.

#### Missing from the target dashboard experience
- Shared `TabbedSubview` component.
- Shared `DashboardSidebar` with module-specific analytical domains.
- Shared `ParameterBar` matching the requested desktop analytics layout.
- Shared `TrendChart`, `DistributionChart`, `ChoroplethMap`, `ZipDrilldownCard`.
- ZIP-level analytics views, ZIP selection, ZIP comparisons.
- Download-PNG actions on every chart/card.
- Unified module dashboards for Schools / Elder Care / Housing.

---

## 2. Backend API inventory

### 2.1 Active API endpoints

| Endpoint | Method | Status | Purpose | What it returns | ZIP-level breakdown? | Time-series? | GeoJSON boundaries? |
|---|---|---:|---|---|---|---|---|
| `/api/health` | GET | Active | Basic API/service health. | Service status, version, optional DB health summary. | No. | No. | No. |
| `/api/data-health` | GET | Active | Detailed DB readiness diagnostics. | DB connectivity/readiness metadata. | No. | No. | No. |
| `/api/analyze` | POST | Active | Main ministry analysis endpoint. | `AnalysisResponse` with demographics, competitors, scores, recommendation, optional trend/gravity/forecast, catchment polygon. | **No ZIP breakdown.** Catchment-wide aggregated output. | **Partial.** Trend summary + optional forecast/projection fields only. | **Partial.** Returns `isochrone_polygon` catchment GeoJSON, not ZIP boundaries. |
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

#### What it does **not** include
- ZIP list for the catchment.
- ZIP-level metric dictionary.
- ZIP boundary `FeatureCollection`.
- Per-ZIP income distribution.
- Per-ZIP current vs projected drilldown payloads.
- Shared historical time-series arrays in `{ year, value, projected }[]` shape.
- A consistent dashboard envelope like the one requested in Phase 1D.

### 2.3 Spatial data reality check

#### Present today
- **Catchment polygon GeoJSON** for isochrone/radius mode.
- **Census tract geometry** in the database and DB queries.
- Spatial filtering/querying by tract geometry and cached isochrone polygons.

#### Missing today
- ZIP/ZCTA boundary ingestion.
- ZIP/ZCTA-level aggregation queries.
- ZIP centroid selection and click-drilldown APIs.
- Any endpoint returning a ZIP `FeatureCollection` with metric properties.

### 2.4 Time-series / projection reality check

#### Present today
- `trend` object: high-level delta summary between historical and current ACS vintages.
- `enrollment_forecast` schema exists for schools.
- Elder care module computes `seniors_projected_5yr` and `seniors_projected_10yr`.
- Historical tract table support exists in DB (`CensusTractHistory`) for trend calculations.

#### Missing today
- No generic analytics endpoint returning multi-year historical + projected series.
- No current endpoint returns per-ZIP time series.
- No generalized CAGR-based projection engine shared across modules.
- No confidence interval payload for projections.

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
| Enrollment forecast | **Partial** | Shared schema exists; current dashboard does not expose a rich trend chart workflow. |
| Historical trend series | **Partial** | Current output gives trend summary, not full annual series. |
| ZIP-level school-age population | **Missing** | Aggregation is tract/catchment-based, not ZIP-based. |
| ZIP-level income distribution | **Missing** | No ZIP aggregation/output today. |
| ZIP-level choropleth metrics | **Missing** | No ZIP boundaries or ZIP metric payload. |
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
| Historical trend series | **Missing/Partial** | No dedicated elder-care trend chart series endpoint. |
| Income distribution for senior households | **Missing** | Only median income is used; no age-specific bucket distribution. |
| Medicare/Medicaid mix indicators at market level | **Missing** | Stage 2 accepts payer mix inputs, but no market analytics endpoint provides catchment payer mix distribution. |
| ZIP-level senior population | **Missing** | No ZIP aggregation/output today. |
| ZIP-level underserved-area detection | **Missing** | Can be inferred conceptually later, but not currently computed per ZIP. |
| ZIP-level facility density / quality gap map | **Missing** | No ZIP boundaries or per-ZIP metrics. |
| Actuarial cohort projections by ZIP (65–74 / 75–84 / 85+) | **Missing** | Current projection is coarse and catchment-wide, not cohort- and ZIP-specific. |

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
| Income distribution | **Partial** | Catchment-level income distribution exists internally; no dashboard-specific bucket API or ZIP breakout. |
| Housing stock / unit counts | **Partial** | Competing project units are present; broader community housing stock analytics are not packaged for dashboard use. |
| Poverty-rate trend | **Missing/Partial** | Some poverty-related counts exist, but no dedicated trend endpoint. |
| Waitlists | **Missing** | No waitlist source or field present. |
| Vacancy/occupancy data | **Partial** | Limited competitor/project fields exist; not a normalized dashboard-wide resource endpoint. |
| Housing gap estimate | **Missing** | No explicit gap calculation endpoint. |
| ZIP-level need assessment | **Missing** | No ZIP boundaries or ZIP aggregation. |
| ZIP-level choropleth of burden / poverty / stock | **Missing** | No ZIP pipeline today. |
| Population growth / migration / diversity dashboard series | **Missing/Partial** | Only coarse trend support exists. |

---

## 5. Gaps vs. requested Phase 1 shared infrastructure

### 5.1 Shared component readiness

| Requested shared component | Current equivalent? | Gap assessment |
|---|---|---|
| `ChoroplethMap` | `SchoolMap` only | **Major gap** — current map handles points/catchment polygon, not ZIP choropleths. |
| `TrendChart` | `TrendPanel` only | **Major gap** — current trend is textual/summary, not a reusable chart. |
| `DistributionChart` | No direct equivalent | **Major gap** — only one compact demographics bar chart exists. |
| `ZipDrilldownCard` | No direct equivalent | **Major gap**. |
| `TabbedSubview` | No direct equivalent | **Major gap**. |
| `DashboardSidebar` | No direct equivalent | **Major gap**. |
| `ParameterBar` | `AnalysisForm` is functionally related | **Medium/major gap** — current form is workflow-centric, not analytics-toolbar-centric. |

### 5.2 Data/model readiness

| Requested capability | Current state | Gap assessment |
|---|---|---|
| ZIP boundary FeatureCollection | Missing | **Major gap** |
| Per-ZIP metric values | Missing | **Major gap** |
| Shared dashboard response envelope | Missing | **Major gap** |
| Historical + projected time series arrays | Partial | **Major gap** |
| Projection confidence metadata | Missing | **Major gap** |
| ZIP drilldown current vs projected snapshot | Missing | **Major gap** |

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

### Blocker A — ZIP boundary data is absent
The requested choropleth and drilldown experience depends on ZCTA/ZIP boundary geometry that is not present today.

### Blocker B — Current data model is tract/catchment-first, not ZIP-first
The backend’s demographic aggregation and many current score calculations operate at catchment-wide tract aggregation. ZIP drilldowns will require new aggregation logic and likely caching.

### Blocker C — Projection framework is not generalized
The app has isolated forecast/projection ideas, but not the shared engine needed for all requested trend views.

### Blocker D — Reference design assets are not in this checkout
The prompt refers to `/docs/design-reference/`, but that directory was absent during audit. The four screenshots included in the task are therefore the only locally available design reference observed during this pass.

---

## 8. Bottom-line readiness assessment

### What is already strong
- The current product has a stable assessment workflow.
- The frontend stack already includes the right mapping and charting libraries.
- The backend already has catchment creation, tract aggregation, competitor retrieval, and some trend/projection hooks.

### What is not yet ready for the target dashboard
- ZIP boundary geography.
- ZIP-level analytics and drilldowns.
- A shared time-series/projection contract.
- A dashboard-native frontend shell with tabs, sidebar navigation, and parameter bar.

### Overall conclusion
The repo is **well-positioned for the dashboard overhaul**, but the requested experience is **not a thin frontend reskin**. It requires new shared components, new dashboard-specific API contracts, ZIP/ZCTA data ingest, and a generalized projection layer. The safest path is to keep the existing assessment flow intact and build the new dashboard experience alongside it.
