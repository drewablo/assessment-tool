# TODO - HUD housing dependency refactor

1. Inventory exact runtime usage of `hud_lihtc_property`, `hud_lihtc_tenant`, and `hud_qct_dda(_designations)` across analysis, readiness, and status paths.
2. Classify each use as baseline required, optional enrichment, reporting-only, or dead/legacy.
3. Refactor readiness/diagnostics logic so only HUD LIHTC property is required for baseline housing analysis; tenant and QCT/DDA must be optional and non-blocking.
4. Update status/warning copy to clearly separate required baseline housing data vs optional enrichments.
5. Add/update tests covering:
   - schools unaffected when tenant/QCT are empty
   - elder_care unaffected when tenant/QCT are empty
   - housing baseline works with property populated and tenant/QCT empty
   - diagnostics/readiness output reflects optionality correctly.
6. Run targeted pytest verification and capture evidence for final report.
