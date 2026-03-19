# Deferred Dashboard Domains Roadmap

_Date:_ 2026-03-19

This document maps each deferred post-MVP dashboard domain to the client question it answers, the sub-tabs it contains, the data sources it needs, and whether the work can be composed from the shared component library or requires new component design.

## Design rule

> Post-MVP domains should be composed from the existing shared component library. If a domain requires a new component type not already in the shared library, design a mockup and get review before implementation.

Existing shared components referenced here:

- `ChoroplethMap`
- `TrendChart`
- `DistributionChart`
- `ZipDrilldownCard`
- `TabbedSubview`
- `CompetitorTable`

## New shared component types required

Build and design-review these shared components before implementing the domains that depend on them.

| Component | Needed by | Description | Closest existing ancestor |
| --- | --- | --- | --- |
| `ScenarioModeler` | Schools → Enrollment | Interactive dashboard-embedded modeler for enrollment, tuition, and aid assumptions, with projected revenue, margin, and break-even outputs. | `WhatIfSimulator` |
| `CatchmentComparisonView` | Schools → Student Body | Comparison view for catchment demographics versus actual enrolled student profile. | `DistributionChart` |
| `BoundaryOverlayLayer` | Housing → Existing Resources | Secondary polygon layer for QCT/DDA overlays on top of ZIP choropleths with distinct styling. | `ChoroplethMap` |
| `PartnerFacilityTable` | Elder Care → Partnership Viability | Partner-oriented facility table with ownership/operator/care-type fields and filtering presets. | `CompetitorTable` |

## Schools module

### 1. Affordability

**Priority:** High.

**Client questions**
- Can families in this area afford our tuition?
- How much financial aid will we need to budget?
- Is the market trending toward or away from our price point?
- Are there enough high-income families to sustain enrollment?

| Sub-tab | Components | Data source | Notes |
| --- | --- | --- | --- |
| Summary | Summary cards + takeaway text | `/api/dashboard` + `/api/analyze` demographics | Includes median family income, families above tuition threshold, high-income family count, and estimated financial gap. |
| Median Income | `ChoroplethMap` + `ZipDrilldownCard` | `/api/dashboard` per-ZIP metrics | ZIP choropleth shaded by median family income. |
| High Income ($200K+) | `TrendChart` + summary cards | `/api/dashboard` time series | Strongest design-reference analog for schools. |
| Distribution | `DistributionChart` + tuition reference overlay | `/api/dashboard` distribution buckets | Requires a minor `DistributionChart` extension for a `referenceLine` prop. |
| Change in Average | `TrendChart` | `/api/dashboard` time series | Lower-priority supporting trend. |

**Data requirements**
- Per-ZIP median family income.
- Per-ZIP high-income household/family count ($200K+).
- Catchment-wide income distribution aligned to ACS B19001 buckets.
- Historical high-income family trend series.

**New component types needed:** None. Minor extension: `DistributionChart.referenceLine`.

### 2. Enrollment

**Priority:** High.

**Client questions**
- Is there enough demand to fill seats at our size?
- How saturated is the market with competitors?
- What enrollment target is realistic for year 1 versus year 5?

| Sub-tab | Components | Data source | Notes |
| --- | --- | --- | --- |
| Market Size | Summary cards + `ChoroplethMap` | `/api/dashboard` per-ZIP metrics + `/api/analyze` scoring | Market depth ratio = addressable families per existing school seat. |
| Competitor Overlap | `ChoroplethMap` with competitor markers/circles | `/api/analyze` competitor payload | Extends the current competitor map rather than adding a new component. |
| Enrollment Scenarios | `ScenarioModeler` | `/api/analyze` + user inputs | Dashboard-native replacement for the standalone `WhatIfSimulator`. |

**Data requirements**
- Market depth ratio per ZIP.
- Competitor seat/enrollment counts.
- Current what-if model inputs for tuition, enrollment, and aid mix.

**New component types needed:** `ScenarioModeler`.

### 3. Student Body

**Priority:** Medium.

**Client question**
- How is the school-age population changing over time?

| Sub-tab | Components | Data source | Notes |
| --- | --- | --- | --- |
| Age Cohorts | `DistributionChart` + summary cards | `/api/dashboard` demographics | Grade-band cohort sizing (K–5 / 6–8 / 9–12). |
| Population Trend | `TrendChart` | `/api/dashboard` time series | Can reuse/refine existing market-overview population trend logic. |
| Catchment vs. Enrollment | `CatchmentComparisonView` | `/api/dashboard` demographics + client enrollment data | Requires client-provided current enrollment profile. |
| Catholic Affiliation | Summary cards + methodology note | `/api/analyze` Catholic estimate | Single-estimate context card, not a chart-first view. |

**Data requirements**
- School-age population by cohort.
- Catholic affiliation estimate.
- Current enrollment-by-grade input from the client / Stage 2.

**New component types needed:** `CatchmentComparisonView`.

## Elder Care module

### 1. Partnership Viability

**Priority:** High.

**Client question**
- We do not want to be responsible for care anymore. Who could provide care at our current location, or where could our community members go?

This is a matchmaking domain, not an underserved-market scoring view.

| Sub-tab | Components | Data source | Notes |
| --- | --- | --- | --- |
| Service Map | `ChoroplethMap` + facility markers | `/api/dashboard` per-ZIP metrics + `/api/analyze` facility list | Overlay senior density with facilities color-coded by care type. |
| Potential Partners | `PartnerFacilityTable` | `/api/analyze` facility list + enrichment | Sort/filter by ownership, operator footprint, care types, bed count, and CMS rating. |

**Data requirements**
- Ownership type enrichment.
- Multi-location operator grouping flag.
- Care-type classification passthrough.
- Existing distance, bed-count, and CMS star metrics.

**New component types needed:** `PartnerFacilityTable`.

### 2. Financial Context

**Priority:** Low.

**Client question**
- What is the revenue picture for elder care in this market?

| Sub-tab | Components | Data source | Notes |
| --- | --- | --- | --- |
| Senior Income | `DistributionChart` | ACS B19037 (new ingest) | Requires age-specific income-by-householder pipeline work. |
| Payer Context | Summary cards + caveat text | County-level CMS enrollment (not currently ingested) | Use only as indicative context; detailed payer work remains offline. |

**Data requirements**
- ACS B19037 ingest.
- County-level Medicare/Medicaid enrollment ingest.

**New component types needed:** None.

### 3. Projections (Enhanced)

**Priority:** Medium.

**Client question**
- What does the aging pipeline look like, and what does that imply for care planning?

| Sub-tab | Components | Data source | Notes |
| --- | --- | --- | --- |
| Cohort Breakdown | `DistributionChart` | Existing projection logic + ACS age data | Compare current vs 5-year vs 10-year cohort buckets (65–74 / 75–84 / 85+). |
| Care Implications | Narrative card / summary cards | Derived from cohort mix | Rule-based interpretation is sufficient initially. |

**Data requirements**
- Existing age-cohort data for 65–74, 75–84, 85+.
- Projection output by cohort rather than only total seniors.

**New component types needed:** None.

## Low-Income Housing module

### 1. Existing Resources

**Priority:** High.

**Client questions**
- What subsidized housing already exists nearby?
- Is there a gap between need and supply?
- Are LIHTC projects competing for the same population?
- Is this location in a QCT or DDA?

| Sub-tab | Components | Data source | Notes |
| --- | --- | --- | --- |
| Subsidized Housing Map | `ChoroplethMap` + project markers + `BoundaryOverlayLayer` | `/api/dashboard` per-ZIP metrics + `/api/analyze` competitors + HUD QCT/DDA boundaries | Requires a secondary polygon overlay style distinct from ZIP choropleth fills. |
| Project Table | `CompetitorTable` variant | `/api/analyze` competitor list | Housing-specific columns for project type, units, vacancy, and year built/in service. |
| Supply Gap | Summary cards | Derived calculation | Estimated eligible households minus total subsidized units, labeled as approximate. |
| Pipeline | Table/list view | New external source needed | Only ship if state/HFA pipeline data is available. |

**Data requirements**
- HUD QCT/DDA boundary GeoJSON ingest and cache.
- Subsidized housing program typing verification.
- Vacancy-rate availability review.
- Potential pipeline-project acquisition from HFA / LIHTC award sources.

**New component types needed:** `BoundaryOverlayLayer` as a `ChoroplethMap` extension.

### 2. Demographic Trends (folded into Community Profile)

**Priority:** Medium.

These items belong as sub-tabs inside Community Profile rather than as a standalone sidebar domain.

| Sub-tab | Components | Data source | Notes |
| --- | --- | --- | --- |
| Population Trend | `TrendChart` | `/api/dashboard` time series | Ensure housing payload includes total population trend. |
| Renter vs. Owner | `TrendChart` or `DistributionChart` | ACS B25003 | Tenure split and direction of the rental market. |
| Migration | Summary cards or `TrendChart` | ACS migration tables if available | Lowest-priority item; defer if not already ingested. |
| Age Distribution | `DistributionChart` | ACS age tables | Catchment age structure context. |
| Poverty Rate | `TrendChart` | ACS poverty tables | Supporting background indicator. |

**Data requirements**
- ACS B25003 tenure data.
- Migration table review.
- Minor payload additions for derived trend series.

**New component types needed:** None.

## Recommended implementation sequence

### Wave 1

| Domain | Module | Why first | New components |
| --- | --- | --- | --- |
| Affordability | Schools | Strong design reference, most data already exists, no new component type. | `DistributionChart.referenceLine` minor extension |
| Partnership Viability | Elder Care | Distinctive differentiator for elder-care clients. | `PartnerFacilityTable` |
| Demographic Trends within Community Profile | Housing | Fills out a thin module using existing primitives. | None |

### Wave 2

| Domain | Module | Why second | New components |
| --- | --- | --- | --- |
| Existing Resources | Housing | High client value, but depends on HUD boundary ingest/enrichment. | `BoundaryOverlayLayer` |
| Projections (Enhanced) | Elder Care | Straightforward extension of existing projection logic. | None |
| Student Body | Schools | Moderate value, but depends on new comparison view and client enrollment inputs. | `CatchmentComparisonView` |

### Wave 3

| Domain | Module | Why third | New components |
| --- | --- | --- | --- |
| Enrollment | Schools | Highest frontend complexity and depends on the dashboard-native scenario modeler. | `ScenarioModeler` |
| Financial Context | Elder Care | Mostly blocked by data acquisition, with lower client urgency. | None |

## Data-pipeline tasks that should start early

These items do not block Wave 1 UI composition work, but they should start in parallel.

- HUD QCT/DDA boundary ingest for housing overlays.
- ACS B19037 ingest for elder-care age-by-income analysis.
- CMS ownership/operator enrichment for partner suitability.
- ACS B25003 ingest verification for renter-versus-owner housing views.
