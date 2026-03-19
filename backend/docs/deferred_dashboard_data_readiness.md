# Deferred dashboard data readiness

_Date:_ 2026-03-19

This note captures the pipeline and enrichment work that should run in parallel with deferred dashboard-domain implementation. It focuses on the four data tasks called out in `DEFERRED_DASHBOARD_DOMAINS.md` and records what is already present in the codebase versus what still needs product or engineering follow-through.

## 1. HUD QCT/DDA boundary ingest for housing overlays

**Status:** Partially in place.

### What already exists
- The normalized HUD QCT/DDA ingest pipeline is already implemented in `backend/pipeline/ingest_housing.py` via `_ingest_hud_qct_async`, with a dedicated CLI entrypoint in `backend/pipeline/cli.py`.
- Normalized designations persist into `hud_qct_dda_designations` via the unique upsert path in `backend/pipeline/ingest_housing.py` and `backend/pipeline/ingest_hud_foundation.py`.
- Readiness/diagnostics already track `hud_qct_dda` as an optional enrichment dependency in `backend/main.py` and `backend/services/dependency_policy.py`.

### Remaining gap for the dashboard
- The dashboard still lacks a boundary-to-GeoJSON export/cache path that can feed a future `BoundaryOverlayLayer`.
- The frontend intentionally does **not** render HUD polygons yet because the roadmap requires design review before implementing that shared map-layer component.

### Operational command
```bash
python -m backend.pipeline.cli ingest-hud-qct
```

## 2. ACS B19037 ingest for elder-care age-by-income analysis

**Status:** Not implemented yet.

### What already exists
- Elder-care financial context is not backed by an age-by-income ACS ingest today.
- Current census ingest covers age, household income, poverty, and tenure primitives, but not B19037 age-by-householder income buckets.

### Required next step
- Add B19037 fields to the census ingest contract/mapping layer in `backend/pipeline/ingest_census.py`.
- Decide whether the resulting data should live on tract records, a new normalized ACS fact table, or a cached derived artifact.
- Expose only the minimum derived payload needed for the elder-care “Senior Income” distribution view.

## 3. CMS ownership/operator enrichment for partner suitability

**Status:** Ownership is present; operator grouping is incomplete.

### What already exists
- CMS and assisted-living ingestion already capture `ownership_type` and related owner/operator name columns in `backend/pipeline/ingest_elder_care.py` and `backend/competitors/cms_care_compare.py`.
- Elder-care analysis already passes ownership text through to partner-facing facility records in `backend/modules/elder_care.py`.

### Remaining gap for the dashboard
- The roadmap calls for a **multi-location operator grouping flag**. The current pipeline surfaces raw owner/operator text, but it does not persist or expose a normalized “regional operator footprint” field.
- Before expanding the live partner view further, add a deterministic grouping/enrichment step that can identify repeated owners/operators across facilities.

## 4. ACS B25003 ingest verification for renter-versus-owner housing views

**Status:** Implemented and should be treated as verified baseline input.

### What already exists
- B25003 total / owner / renter occupied fields are already requested and accumulated in `backend/api/census.py`.
- Census ingest maps the B25003 columns into tract storage in `backend/pipeline/ingest_census.py`.
- Housing/dashboard code already uses the resulting owner/renter fields in analysis and UI flows (`backend/services/dashboard_service.py`, `frontend/src/lib/dashboard-live.ts`).

### Remaining follow-up
- If the housing Community Profile gets deeper tenure-specific charts, prefer payload-level derived series over recomputing tenure shares repeatedly in the frontend.

## Recommended sequence

1. Treat **HUD QCT/DDA boundary export/cache** and **CMS operator grouping** as the next engineering-ready data tasks because the source ingestion is already mostly present.
2. Treat **B19037** as the highest net-new ingest design task because elder-care Financial Context is otherwise blocked.
3. Treat **B25003** as verified and avoid reopening that ingest path unless a specific bug is found.
