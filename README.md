# Ministry Assessment Tool

A full-stack feasibility analysis platform for mission-driven planning across three ministry tracks:

- **Schools** — Catholic K–12 market feasibility
- **Housing** — Affordable housing demand + LIHTC saturation
- **Elder Care** — Senior-market demand + CMS facility landscape

The application combines Census demographics, geospatial catchments, and module-specific competitor data to produce a feasibility score, recommendation, and exportable report.

---

## Core workflow

1. User submits an address and ministry settings from the Next.js UI.
2. Backend geocodes the address with the Census Geocoder API.
3. Backend builds a catchment using OpenRouteService isochrone (if API key is set) or a grade-level fallback radius.
4. Backend pulls ACS demographics and module-specific competitor inventory.
5. A module analyzer computes weighted metric scores and an overall recommendation.
6. UI renders dashboard cards, map, trend and gravity visualizations, competitor table, and Stage 2 panel.
7. User can optionally compare schools/housing/elder care outcomes side-by-side for transformation discernment.
8. User can export results as CSV or PDF.

---

## Setup & Deployment

Everything is managed through a single entry point:

```bash
./start.sh [command]
```

| Command | What it does |
|---------|-------------|
| `./start.sh` or `./start.sh dev` | Install deps + start backend & frontend locally (no Docker needed) |
| `./start.sh docker` | Run DB/Redis/API/workers in Docker; frontend hot-reloads locally |
| `./start.sh prod` | Full production deploy — all services + nginx in Docker |
| `./start.sh update` | `git pull` + rebuild production containers in-place |
| `./start.sh stop` | Stop dev Docker services (`--prod` for production stack) |
| `./start.sh logs [svc]` | Tail Docker logs (`--prod` for production stack) |
| `./start.sh status` | Show all running containers |
| `./start.sh diagnose [svc] [--prod]` | Print compose status, container health JSON, and recent logs (default service: `frontend`) |

---

## Local development (no Docker)

**Prerequisites:** Python 3.11+, Node.js 18+

```bash
# macOS
brew install python@3.12 node

# Ubuntu / Debian
sudo apt install python3 python3-venv nodejs npm
```

```bash
./start.sh dev
```

This creates `backend/.venv`, installs all dependencies, writes default env files, and starts both services:

| Service  | URL |
|----------|-----|
| Backend  | http://localhost:8000 |
| Frontend | http://localhost:3000 |

Default login: `admin` / `admin` (set in `frontend/.env.local` → `AUTH_USERS`)

To install dependencies without starting services:
```bash
./dev_setup_and_run.sh --install-only
```

---

## Docker dev mode

Runs the backend stack (DB, Redis, API, Celery workers) in Docker while the frontend still hot-reloads locally — best of both worlds.

**Prerequisites:** [Docker Desktop](https://docs.docker.com/get-docker/) (macOS/Windows) or Docker Engine (Linux)

```bash
./start.sh docker
```

| Container | Purpose | Port |
|-----------|---------|------|
| `db` | PostGIS 16 — persistent storage | 5432 |
| `redis` | Redis 7 — task queue + cache | 6379 |
| `api` | FastAPI backend | 8000 |
| `worker` | Celery worker — data pipeline tasks | — |
| `beat` | Celery beat — scheduled runs | — |
| frontend | Next.js dev server (local, not Docker) | 3000 |

Ctrl+C stops the frontend and tears down the Docker stack automatically.

> **Tip:** Add `ANTHROPIC_API_KEY=sk-ant-...` to `backend/.env` to enable audit PDF extraction.

---

## Production deployment

**Prerequisites:** Docker + Docker Compose on your server, repo cloned, domain or IP ready.

### 1. Create `.env.prod`

```bash
cp .env.prod.example .env.prod
```

Edit `.env.prod` and fill in every `CHANGE_ME` value:

```env
FRONTEND_URL=https://yourdomain.com   # or http://YOUR_SERVER_IP
DB_PASSWORD=a-strong-password
INTERNAL_API_KEY=<openssl rand -hex 32>
NEXTAUTH_SECRET=<openssl rand -hex 32>
AUTH_USERS=alice:password,bob:password
ANTHROPIC_API_KEY=sk-ant-...
```

### 2. Deploy

```bash
./start.sh prod
```

This builds all containers and starts:

| Container | Purpose | Port |
|-----------|---------|------|
| `db` | PostGIS 16 | internal |
| `redis` | Redis 7 | internal |
| `api` | FastAPI backend | internal |
| `worker` | Celery worker | — |
| `beat` | Celery beat | — |
| `frontend` | Next.js (production build) | internal |
| `nginx` | Reverse proxy | **80** |

### 3. Updates

```bash
./start.sh update    # git pull + rebuild containers with zero manual steps
```

### Unraid / home server

Use the [Compose Manager](https://forums.unraid.net/topic/114415-plugin-docker-compose-manager/) plugin and point it at this repo's `docker-compose.prod.yml`. See the Unraid section in the project wiki for full instructions.

---

## Manual env reference

### `backend/.env`

```env
FRONTEND_URL=http://localhost:3000
ANTHROPIC_API_KEY=sk-ant-...     # required for audit PDF extraction
ORS_API_KEY=                      # optional: drive-time isochrone catchments
CENSUS_API_KEY=                   # optional: higher Census API rate limits
USE_DB=false                      # set true to enable v2 DB features
DATABASE_URL=postgresql+asyncpg://feasibility:feasibility@localhost:5432/feasibility
REDIS_URL=redis://localhost:6379/0
```

### `frontend/.env.local`

```env
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXTAUTH_SECRET=any-random-string-for-local-dev
NEXTAUTH_URL=http://localhost:3000
AUTH_USERS=admin:admin            # comma-separated username:password pairs
```

---

## Planning and status docs

- Canonical current-state summary: `CURRENT_CODEBASE_STATUS.md`
- Improvement execution plan: `PLATFORM_IMPROVEMENT_ROADMAP.md`
- Deferred dashboard domain roadmap: `DEFERRED_DASHBOARD_DOMAINS.md`
- Historical planning references: `REVIEW_NOTES.md`, `STAGE2_BENCHMARK_TUNING_PLAN.md`, `STAGE2_SPRING_PLANNING_DISCOVERY.md`, `V2_STRATEGIC_BLUEPRINT.md`

---

## Data ingest pipelines

The revised dashboard/data flow now depends on a few ingest steps beyond the original current-vintage ACS load. In particular:

- `ingest-census-history` backfills the ACS vintages used by dashboard projections (`2013`, `2015`, `2017`, `2019`, `2021`).
- `ingest-nais` reconciles the NAIS source file against PSS so dashboard and analysis competitor tables include schools that appear in either source without double-counting overlaps.
- `ingest-zcta` remains required for the choropleth ZIP geometry cache.

### Recommended ingest order

From `backend/` run:

```bash
docker compose -f docker-compose.prod.yml --env-file .env.prod exec api python3 -m pipeline.cli init-db
docker compose -f docker-compose.prod.yml --env-file .env.prod exec api python3 -m pipeline.cli ingest-census --vintage 2022
docker compose -f docker-compose.prod.yml --env-file .env.prod exec api python3 -m pipeline.cli ingest-census-history
docker compose -f docker-compose.prod.yml --env-file .env.prod exec api python3 -m pipeline.cli python -m pipeline.cli ingest-zcta
docker compose -f docker-compose.prod.yml --env-file .env.prod exec api python3 -m pipeline.cli ingest-schools
docker compose -f docker-compose.prod.yml --env-file .env.prod exec api python3 -m pipeline.cli ingest-nais
docker compose -f docker-compose.prod.yml --env-file .env.prod exec api python3 -m pipeline.cli ingest-elder-care
docker compose -f docker-compose.prod.yml --env-file .env.prod exec api python3 -m pipeline.cli ingest-housing
docker compose -f docker-compose.prod.yml --env-file .env.prod exec api python3 -m pipeline.cli ingest-hud-section202
```

### Full refresh shortcut

For a one-command refresh, use:

```bash
docker compose -f docker-compose.prod.yml --env-file .env.prod exec api python3 -m pipeline.cli ingest-all
```

That now runs, in order, current ACS ingest, historical ACS vintages, ZCTA cache warm-up, PSS ingest, NAIS reconciliation, elder-care ingest, housing ingest, and HUD Section 202 ingest.

### Pipeline notes

- **ACS history:** projections improve materially once `ingest-census-history` has populated `CensusTractHistory` with multiple vintages instead of a single backstop point.
- **NAIS reconciliation:** `ingest-nais` reads `exsources/nais_schools.csv`, attempts to match NAIS schools to existing PSS records, and geocodes NAIS-only schools through the Census Geocoder. Because those geocoder calls are rate-limited to one request per second, this step can take noticeably longer than the other school ingest.
- **PSS + NAIS schema:** existing PSS rows default to `data_source="pss"`; NAIS-only rows are inserted as `data_source="nais"`; overlapping PSS rows are flagged with NAIS metadata instead of duplicated.

## Dashboard ZIP boundary cache warm-up

The dashboard choropleth depends on a cached Census ZCTA boundary bundle. Treat this as a standard deployment prerequisite.

### Populate the cache

From the backend directory:

```bash
python -m pipeline.cli ingest-zcta
```

or as part of the full refresh flow:

```bash
python -m pipeline.cli ingest-all
```

### Operational expectations

- Source dataset: Census TIGER/Line ZCTA shapefile (`tl_2024_us_zcta520.zip`), roughly **504 MB** downloaded from the Census site.
- Expected runtime: typically **a few minutes** in a clean environment, depending on network throughput and disk speed.
- Cache output: gzipped GeoJSON at `backend/data/zcta_boundaries.json.gz` unless `ZCTA_CACHE_PATH` is overridden.
- Expected cache footprint: usually **tens of MB**, depending on simplification settings and environment.
- Refresh cadence: refresh when the dashboard environment is rebuilt and at least **quarterly** after Census/TIGER updates or if the cache is deleted/corrupted.

### Failure mode

- If the cache file is missing or empty, `POST /api/dashboard` now returns a structured `ZCTA_CACHE_MISSING` error telling operators to run `python -m pipeline.cli ingest-zcta`.
- If the cache exists but a specific catchment resolves to no ZIPs, the frontend shows a clear empty-state message rather than a broken map.

---

## API reference

### `GET /api/health`
Returns service status and version.

### `POST /api/analyze/compare`
Runs the same location and settings across multiple ministry tracks and returns a side-by-side summary ranked by overall score.

```json
{
  "school_name": "St. Brigid Campus",
  "address": "123 Main St, Cleveland, OH",
  "ministry_types": ["schools", "housing", "elder_care"],
  "drive_minutes": 20,
  "market_context": "suburban"
}
```

Response shape:

```json
{
  "school_name": "St. Brigid Campus",
  "analysis_address": "123 Main St, Cleveland, OH",
  "compared_ministry_types": ["schools", "housing", "elder_care"],
  "results": [
    {
      "ministry_type": "schools",
      "overall_score": 74,
      "scenario_conservative": 62,
      "scenario_optimistic": 86,
      "recommendation": "Proceed to Stage 2",
      "recommendation_detail": "Strong market fundamentals with manageable risk.",
      "recommended_pathway": "continue",
      "pathway_confidence": "medium",
      "fit_band": "high",
      "capital_intensity": "medium",
      "regulatory_complexity": "medium",
      "operator_dependency": "optional",
      "time_to_launch_months_estimate": 18
    }
  ]
}
```

### `POST /api/analyze`
Main analysis endpoint. Returns demographics, competitors, feasibility scores, recommendation, and optional trend/gravity/forecast data.

```json
{
  "school_name": "string",
  "address": "string",
  "ministry_type": "schools | housing | elder_care",
  "mission_mode": false,
  "drive_minutes": 20,
  "gender": "coed | boys | girls",
  "grade_level": "k5 | k8 | high_school | k12",
  "weighting_profile": "standard_baseline | affordability_sensitive | demand_primacy",
  "market_context": "urban | suburban | rural",
  "care_level": "all | snf | assisted_living | memory_care",
  "min_mds_overall_rating": null,
  "stage2_inputs": {
    "school_audit_financials": [],
    "historical_financials": []
  },
  "facility_profile": {
    "building_square_footage": 42000,
    "accessibility_constraints": ["No elevator"],
    "current_layout_notes": "Classroom-heavy floorplan",
    "deferred_maintenance_estimate": 900000,
    "zoning_use_constraints": ["Special use permit"],
    "sponsor_operator_capacity": "medium"
  }
}
```

- `POST /api/analyze` responses now include `decision_pathway` with recommendation fields (`recommended_pathway`, confidence, risks, validations, and 12-month actions).
- When `facility_profile` is provided, pathway logic includes facility/governance constraints; partner recommendations include a `partner_assessment` object for mission alignment, governance options, and readiness checklist.
- Phase 3 API output now includes `trace_id`, `data_freshness`, `benchmark_narrative`, and `board_report_pack` for governance-ready interpretation and auditability.

### `POST /api/export/csv`
Same body as `/api/analyze`. Returns a downloadable CSV.

### `POST /api/export/pdf`
Same body as `/api/analyze`. Returns a downloadable PDF report with benchmark narrative, board-pack summary, and data-freshness sections.

### `POST /api/export/board-pack`
Same body as `/api/analyze`. Returns a board-ready JSON payload (`board_report_pack`, `benchmark_narrative`, `data_freshness`, `trace_id`).


### Phase 2 portfolio workspace endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/portfolio/workspaces` | Create a named client engagement workspace with candidate locations and scenario sets |
| `GET` | `/api/portfolio/workspaces/{workspace_id}` | Retrieve a saved workspace |
| `PATCH` | `/api/portfolio/workspaces/{workspace_id}` | Update engagement metadata, candidate locations, or scenario sets |
| `POST` | `/api/portfolio/workspaces/{workspace_id}/compare-snapshots` | Run compare and save side-by-side snapshot into workspace |

### `POST /api/schools/stage2/extract-audits`
Accepts one or more audit PDF files (`multipart/form-data`). Uses Claude to extract financial data (tuition revenue, expenses, assets, etc.) per fiscal year. Requires `ANTHROPIC_API_KEY`.

```
field: files[]  (PDF, required, one or more)
```

Returns extracted year rows with any ambiguity warnings.

### `GET /api/scoring/weights`
Returns the active scoring weight profiles.

### v2 endpoints (requires `USE_DB=true`)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/pipeline/status` | Data pipeline run status and record counts |
| `GET` | `/api/opportunities` | Opportunity discovery across stored markets |
| `GET` | `/api/history` | Saved analysis history |
| `GET` | `/api/history/{id}` | Single saved analysis |
| `DELETE` | `/api/history/{id}` | Delete a saved analysis |

---

## Module behavior

### Schools
- NCES-style competitive landscape with Catholic-market calibration.
- Supports grade level (`k5`, `k8`, `high_school`, `k12`), gender filters, drive-time catchments, weighting profiles, and market-context adjustments.
- Trend scoring, scenario ranges (conservative/optimistic), addressable-market estimation.
- Stage 2 audit PDF extraction powered by Claude Opus 4.6.

### Housing
- ACS cost-burden signals + nearby LIHTC project saturation (distance-decayed).
- Stage 2 institutional-economics schema is scaffolded (not yet fully scored).

### Elder Care
- Supports **Mission-Aligned** (vulnerability-focused) and **Market Demand** (market-size-focused) modes.
- Care-level filtering (`all`, `snf`, `assisted_living`, `memory_care`) and optional minimum CMS MDS star rating.
- CMS Care Compare competitor ingest with occupancy signals.
- Stage 2 is scaffolded and marked not-ready.

---

## Architecture

```
backend/
  main.py               FastAPI entrypoint + all routes
  api/
    analysis.py         Feasibility scoring engine
    census.py           ACS demographics + trend computation
    geocoder.py         Census Geocoder integration
    isochrone.py        OpenRouteService catchment builder
    schools.py          School competitor lookup + tiering
    school_stage2.py    Audit PDF extraction (Claude API)
    pdf_report.py       ReportLab PDF report generation
    reports.py          CSV export
    benchmarks.py       Percentile benchmarking
    hierarchical_scoring.py  Hierarchical score breakdown
    bls_workforce.py    BLS workforce data
  competitors/
    hud_lihtc.py        HUD LIHTC housing project loader
    cms_care_compare.py CMS Care Compare elder care loader
  modules/
    schools.py          Schools scoring module
    housing.py          Housing scoring module
    elder_care.py       Elder care scoring module
    frameworks.py       Shared scoring framework
  pipeline/             Celery task definitions (v2)
  db/                   SQLAlchemy models + queries (v2)
  models/schemas.py     All Pydantic request/response schemas
  validation/           Model audit harness (non-unit empirical checks)
  tests/                pytest test suite

frontend/
  src/
    app/                Next.js app router pages
    components/
      AnalysisForm.tsx       Ministry settings + audit PDF upload
      ResultsDashboard.tsx   Score cards, map, charts
      CompetitorTable.tsx    Competitor landscape table
      Stage2Dashboard.tsx    Stage 2 financial panel
    lib/api.ts          Typed API client
```

---

## Testing

```bash
pytest backend/tests -q
```

256 tests covering scoring logic, competitor tiering, Census trend computation, benchmark percentiles, and ACS variable completeness.

---

## Model validation audit

A standalone empirical validation harness runs real geocoding + ACS + scoring calls against known-outcome markets:

```bash
python backend/validation/model_audit.py
```

Outputs:
- Console summary with pass/fail discrimination checks
- `backend/validation/model_audit_report.json`
- `backend/validation/cache/backtest_cache.json` (cached to avoid repeated API calls)

To use as a library:

```python
from backend.validation.model_audit import generate_model_audit_report
import asyncio

report = asyncio.run(generate_model_audit_report(use_cache=True))
print(report["recommendations"])
```

Reference data snapshots live in `backend/validation/reference_data/`. Update `sources.json` when refreshing them.

---

## Data sources

| Source | Purpose |
|--------|---------|
| US Census Geocoder | Address resolution to lat/lon |
| US Census ACS (5-year) | Demographics and 2017→2022 trend inputs |
| OpenRouteService | Drive-time isochrone catchments (optional) |
| NCES / PSS | School competitor context |
| HUD LIHTC | Affordable housing competitor inventory (local cache) |
| CMS Care Compare | Elder care facility inventory with occupancy (local cache) |
| Anthropic Claude Opus 4.6 | Audit PDF financial data extraction |

---

## Current limitations

- Housing and elder care Stage 2 scoring are scaffolded and not yet fully scored.
- Elder care QRP quality-measure join is deferred pending dataset alignment.
- Competitor pipelines (HUD, CMS) rely on local cached CSV availability.
- Frontend ESLint may prompt for initial setup in fresh environments (`npm run lint`).
- Docker Compose (`docker-compose.yml`) does not include the frontend container — run it separately with `npm run dev`. The production compose (`docker-compose.prod.yml`) does include it.


## Troubleshooting container startup

If a service is marked `unhealthy` or fails to start, run:

```bash
./start.sh diagnose --prod frontend
./start.sh doctor --prod
./start.sh doctor [--prod]   # bracket token is accepted too
./start.sh ingest --prod      # run all pipeline ingests into DB
```

`doctor` calls API-level diagnostics (`/api/health` + `/api/pipeline/status`) and summarizes whether DB data is actually ready for analysis.
If `localhost:8000` is unavailable but `.env.prod` exists, it auto-switches to production mode.
If `python3`/`python` is unavailable on the host, `doctor` still works and prints raw JSON output.


If `doctor` reports all record counts as `0` and "has never completed successfully" for each pipeline, the database is connected but **not populated yet**. Run:

```bash
./start.sh ingest --prod
```

Then run `./start.sh doctor --prod` again to confirm `db_ready_for_analysis=True`.

This prints:
- compose service status,
- Docker health details from `docker inspect`,
- raw healthcheck log entries (exit code + output) from Docker,
- and the last 300 lines of service logs.

If the service is defined but no container exists yet, it now tells you that explicitly and prints the exact `docker compose ... up -d --build <service>` command to run next.

For dev stack debugging, omit `--prod`:

```bash
./start.sh diagnose frontend
```

### Build failures with `parent snapshot ... does not exist`

If deploy/build fails with an error like:

- `failed to prepare extraction snapshot ... parent snapshot ... does not exist: not found`

that is usually Docker BuildKit cache corruption on the host (not an app-code error).  
`./start.sh prod`, `./start.sh update`, and `./deploy.sh` now auto-detect this pattern, run a one-time `docker builder prune -f`, and retry once.

If you still hit it manually, run:

```bash
docker builder prune -f
docker compose -f docker-compose.prod.yml --env-file .env.prod up -d --build
```
