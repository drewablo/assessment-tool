# Lessons learned

- HUD LIHTC tenant workbook sources are summary matrices, not row-level records; always load with `header=None` and run explicit layout detection before normalization.
- For workbook ingestion refactors, avoid variable-name carryover from legacy paths (`raw` vs normalized rows) and run targeted persistence tests immediately after rewiring control flow.
- Per-sheet diagnostics (header rows, data start/stop, parsed row counts, warnings) should be mandatory for heterogeneous workbook formats to prevent silent zero-row success.
- Shared pre-dispatch dependencies (especially geocoding) must distinguish upstream service outages from invalid user input; mapping transport/HTTP failures to 503 with a clear error code prevents cross-module false 422 "bad address" failures and speeds root-cause diagnosis.
- When a roadmap says a new shared component requires design review first, do not fake full delivery of that domain; ship only the unblocked slices and label blockers explicitly in the UI and notes.

