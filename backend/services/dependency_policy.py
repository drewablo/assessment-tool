from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

RunMode = Literal["live_only", "db_with_fallback", "db_strict"]


@dataclass(frozen=True)
class DataDependencyRule:
    key: str
    required: bool
    baseline_blocking: bool
    affects_confidence: bool
    export_blocking_in_strict: bool
    optional_note: str | None = None


DEPENDENCY_REGISTRY: dict[str, DataDependencyRule] = {
    "census_tracts": DataDependencyRule("census_tracts", required=True, baseline_blocking=True, affects_confidence=True, export_blocking_in_strict=True),
    "competitors_schools": DataDependencyRule("competitors_schools", required=False, baseline_blocking=False, affects_confidence=True, export_blocking_in_strict=False, optional_note="School competitor intelligence unavailable; fallback path used."),
    "competitors_elder_care": DataDependencyRule("competitors_elder_care", required=False, baseline_blocking=False, affects_confidence=True, export_blocking_in_strict=False, optional_note="Elder care competitor intelligence unavailable; fallback path used."),
    "hud_lihtc_property": DataDependencyRule("hud_lihtc_property", required=False, baseline_blocking=False, affects_confidence=True, export_blocking_in_strict=True, optional_note="HUD LIHTC enrichment unavailable; baseline still available."),
    "hud_lihtc_tenant": DataDependencyRule("hud_lihtc_tenant", required=False, baseline_blocking=False, affects_confidence=True, export_blocking_in_strict=False, optional_note="HUD tenant enrichment unavailable."),
    "hud_qct_dda": DataDependencyRule("hud_qct_dda", required=False, baseline_blocking=False, affects_confidence=True, export_blocking_in_strict=False, optional_note="QCT/DDA enrichment unavailable."),
    "hud_section_202": DataDependencyRule("hud_section_202", required=True, baseline_blocking=True, affects_confidence=True, export_blocking_in_strict=True, optional_note="HUD Section 202 senior housing data unavailable; run pipeline 'hud_section_202' to ingest."),
}


def resolve_run_mode(requested: str | None, use_db: bool) -> RunMode:
    mode = (requested or "").strip().lower() or "db_with_fallback"
    if mode not in {"live_only", "db_with_fallback", "db_strict"}:
        mode = "db_with_fallback"
    if mode in {"db_with_fallback", "db_strict"} and not use_db:
        return "live_only"
    return mode  # type: ignore[return-value]


def summarize_dependencies(counts: dict[str, int] | None) -> list["DataDependencyStatus"]:
    from models.schemas import DataDependencyStatus

    counts = counts or {}
    rows: list[DataDependencyStatus] = []
    for key, rule in DEPENDENCY_REGISTRY.items():
        count = int(counts.get(key) or 0)
        rows.append(
            DataDependencyStatus(
                dataset=key,
                required=rule.required,
                baseline_blocking=rule.baseline_blocking,
                affects_confidence=rule.affects_confidence,
                export_blocking_in_strict=rule.export_blocking_in_strict,
                available=count > 0,
                row_count=count,
                note=rule.optional_note,
            )
        )
    return rows


def strict_mode_blockers(
    counts: dict[str, int] | None,
    *,
    ministry_type: str | None = None,
    housing_target_population: str | None = None,
) -> list[str]:
    """Return strict-mode blockers, optionally scoped to the active request.

    `hud_section_202` is only a hard blocker for senior-only housing analyses.
    For other ministries (or all-ages housing), missing Section 202 data should
    not block the analysis request.
    """
    counts = counts or {}
    blockers: list[str] = []

    for key, rule in DEPENDENCY_REGISTRY.items():
        if not rule.required:
            continue

        if key == "hud_section_202":
            is_senior_housing = ministry_type == "housing" and (housing_target_population or "all_ages") == "senior_only"
            if not is_senior_housing:
                continue

        if int(counts.get(key) or 0) == 0:
            blockers.append(f"Required dataset '{key}' is unavailable in db_strict mode.")

    return blockers
