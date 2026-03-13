# HUD Backend Data Contracts (Phases 1-3)

## Source families

- `lihtc_property` (HUD LIHTC property-level)
- `lihtc_tenant` (HUD LIHTC tenant-level)
- `qct_dda` (HUD QCT/DDA annual designations)

## Required contract behavior

- Required fields are enforced through explicit contract validation.
- Optional fields are accepted when present.
- Alias mapping supports renamed columns across yearly releases.
- Validation fails on missing required fields.
- Unexpected fields are preserved in raw snapshots and ignored during normalization unless mapped.

## Refresh cadence

- LIHTC property: annual refresh.
- LIHTC tenant: annual refresh (schema can vary by year).
- QCT/DDA: annual designation refresh.

## QA checklist

1. Raw source artifact is immutable and checksumed.
2. Ingest run is recorded with status, source year/version, and errors if any.
3. Contract validation passes before normalization.
4. Normalized GEOID/FIPS fields are zero-padded and length-validated.
5. Invalid coordinates are rejected for point geometries.
6. Malformed geometry inputs are rejected with explicit errors.
7. Join pathways store method + confidence and preserve unmatched rows.
8. Normalized rows include provenance (`source_snapshot_id`, `source_version`, `dataset_year`).

## Schema variance approach

- Source-year specific aliases are resolved by canonical field names.
- Missing optional fields are set to null.
- If source-level shape changes (CSV/JSON row field names), aliases absorb known drift.
- Unknown shape changes fail fast at contract validation and are tracked in ingest run metadata.
