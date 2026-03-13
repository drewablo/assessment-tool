# HUD Integration Operations Runbook (Phases 4–6)

## Scope
This runbook covers backend exposure, scoring/report integration, and operational checks for:
- LIHTC property
- LIHTC tenant
- QCT/DDA designations

## Exposure path
- Query layer: `db/queries.py::get_nearby_hud_housing_context`
- Service/module layer: `modules/housing.py`
- Reporting layer: `api/reports.py` (housing CSV)

## Freshness and status checks
1. Run API status endpoint:
   - `GET /api/pipeline/status`
2. Inspect:
   - `hud_ingest` block (`lihtc_property`, `lihtc_tenant`, `qct_dda`)
   - `record_counts` for normalized HUD tables and raw snapshots
3. Run data-freshness metadata via analysis response (`data_freshness.sources`) and check HUD normalized entries.

## Annual refresh workflow
1. Acquire latest annual source files.
2. Ingest each source family using:
   - `python -m pipeline.cli ingest-hud-foundation --source-family ... --source-identifier ... --file ... --dataset-year ...`
3. Verify `status=success` in `hud_ingest_runs` and non-zero normalized row counts.
4. Validate housing analysis output includes HUD enrichment note when `USE_DB=true`.

## Schema drift review
- If contract validation fails:
  1. inspect ingest-run `error_message`
  2. compare source columns against `pipeline/hud_contracts.py`
  3. add conservative alias updates for renamed fields
  4. rerun ingest from immutable snapshot

## Reprocessing guidance
- Reprocessing is snapshot-based; rerun normalization by pointing ingest command to stored snapshot artifact file path.
- Never overwrite snapshot artifacts; each run creates immutable lineage.

## Troubleshooting
### Symptom: Housing analysis has no HUD enrichment
- Verify `USE_DB=true` and normalized HUD tables contain rows for current/prior year.
- Verify `join_confidence` values in tenant table (`>=0.7` required for aggregation).
- Confirm tract GEOIDs are normalized to 11 digits.

### Symptom: QCT/DDA flags not shown in reports
- Check `hud_property_designation_matches` rows exist for the dataset year.
- Confirm designation year aligns with property dataset year used during scoring.

### Symptom: score drift after HUD refresh
- Compare `hud_market_boost` and `hud_competition_boost` components in housing metrics descriptions.
- Ensure tenant records are not inflated by duplicate source rows.

## Known maintenance burden
- Yearly schema drift in tenant files is the highest maintenance risk.
- Designation-year alignment (QCT/DDA vs property year) must be monitored annually.
- Partial source coverage can reduce enrichment and revert to conservative fallback behavior.
