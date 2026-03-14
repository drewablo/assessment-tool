"""HUD LIHTC/QCT ingestion pipeline with split ingest steps."""

import asyncio
import csv
import logging
import os
import re
import tempfile
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pandas as pd
from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.connection import async_session_factory
from db.models import HudLihtcProperty, HudLihtcTenant, HudQctDdaDesignation
from pipeline.hud_contracts import CONTRACTS, resolve_field
from pipeline.base import finish_pipeline_run, start_pipeline_run
from pipeline.celery_app import celery_app

logger = logging.getLogger("pipeline.housing")

HUD_LIHTC_PROPERTY_ZIP_URL = os.getenv(
    "HUD_LIHTC_PROPERTY_ZIP_URL",
    "https://www.huduser.gov/lihtc/lihtcpub.zip",
)
HUD_LIHTC_TENANT_XLSX_DEFAULT_URL = "https://www.huduser.gov/portal/Datasets/lihtc/2023-LIHTC-Tenant-Tables.xlsx"
HUD_QCT_DDA_XLSX_DEFAULT_URL = "https://www.huduser.gov/portal/datasets/qct/qct_data_2026.xlsx"
HUD_LIHTC_TENANT_XLSX_URL = os.getenv("HUD_LIHTC_TENANT_XLSX_URL", HUD_LIHTC_TENANT_XLSX_DEFAULT_URL)
HUD_QCT_DDA_XLSX_URL = os.getenv("HUD_QCT_DDA_XLSX_URL", HUD_QCT_DDA_XLSX_DEFAULT_URL)
HUD_DATASET_YEAR = int(os.getenv("HUD_DATASET_YEAR", str(datetime.now(timezone.utc).year)))
HUD_PROPERTY_BATCH_SIZE = int(os.getenv("HUD_PROPERTY_BATCH_SIZE", "500"))
HUD_TENANT_BATCH_SIZE = int(os.getenv("HUD_TENANT_BATCH_SIZE", "1000"))
HUD_QCT_BATCH_SIZE = int(os.getenv("HUD_QCT_BATCH_SIZE", "1000"))


def _norm_key(value: str) -> str:
    return "".join(ch for ch in str(value).strip().lower() if ch.isalnum())


def _row_with_normalized_keys(row: dict) -> dict:
    norm = dict(row)
    for key, value in row.items():
        nkey = _norm_key(key)
        if nkey and nkey not in norm:
            norm[nkey] = value
    return norm


def _resolve_value(row: dict, canonical: str, aliases: dict[str, list[str]]):
    """Resolve a contract field with case/spacing-tolerant key matching."""
    direct = resolve_field(row, canonical, aliases)
    if direct not in (None, ""):
        return direct

    candidates = [canonical, *aliases.get(canonical, [])]
    normalized = {_norm_key(k): v for k, v in row.items() if v not in (None, "")}
    for key in candidates:
        val = normalized.get(_norm_key(key))
        if val not in (None, ""):
            return val
    return None


def _to_int(value):
    if value in (None, ""):
        return None
    if isinstance(value, str):
        value = value.strip().replace(",", "")
    try:
        parsed = float(value)
        if pd.isna(parsed):
            return None
        return int(parsed)
    except (ValueError, TypeError):
        return None


def _to_float(value):
    if value in (None, ""):
        return None
    if isinstance(value, str):
        value = value.strip().replace(",", "")
    try:
        parsed = float(value)
        if pd.isna(parsed):
            return None
        return parsed
    except (ValueError, TypeError):
        return None


def _first_value(row: dict, *keys: str):
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _top_reasons(reasons: Counter, top_n: int = 5) -> str:
    return ", ".join(f"{k}={v}" for k, v in reasons.most_common(top_n)) if reasons else "none"


def _rows_from_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return [dict(r) for r in csv.DictReader(fh)]


async def _download_file(url: str, path: Path) -> None:
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, timeout=180.0, follow_redirects=True)
        resp.raise_for_status()
        path.write_bytes(resp.content)


def _select_lihtc_csv_member(member_names: list[str]) -> str:
    candidates = [m for m in member_names if m and not m.endswith("/") and Path(m).name.lower().endswith(".csv")]
    if not candidates:
        raise ValueError("No CSV files found in HUD LIHTC ZIP")

    def score(name: str) -> tuple[int, int]:
        base = Path(name).name.lower()
        if base == "lihtcpub.csv":
            return (0, len(name))
        if "lihtcpub" in base:
            return (1, len(name))
        return (2, len(name))

    return sorted(candidates, key=score)[0]


async def _fetch_lihtc_property_rows_from_zip() -> list[dict]:
    with tempfile.TemporaryDirectory(prefix="hud_lihtc_property_") as tmp:
        tmp_dir = Path(tmp)
        zip_path = tmp_dir / "LIHTCPUB.ZIP"
        logger.info("Downloading HUD LIHTC property ZIP from %s to %s", HUD_LIHTC_PROPERTY_ZIP_URL, zip_path)
        await _download_file(HUD_LIHTC_PROPERTY_ZIP_URL, zip_path)

        extracted_csv: Path | None = None
        try:
            with zipfile.ZipFile(zip_path) as zf:
                member_names = zf.namelist()
                preview = member_names[:10]
                logger.info(
                    "HUD LIHTC ZIP members (count=%s, first=%s%s)",
                    len(member_names),
                    preview,
                    "..." if len(member_names) > len(preview) else "",
                )
                target_name = _select_lihtc_csv_member(member_names)
                extracted_csv = tmp_dir / Path(target_name).name
                logger.info("HUD LIHTC ZIP selected member: %s", target_name)
                with zf.open(target_name) as src, extracted_csv.open("wb") as out:
                    out.write(src.read())

            rows = _rows_from_csv(extracted_csv)
            if rows:
                logger.info("HUD property CSV headers sample(first 30)=%s", list(rows[0].keys())[:30])
            logger.info("HUD property CSV extracted file=%s rows=%s", extracted_csv.name, len(rows))
            return rows
        finally:
            if extracted_csv is not None:
                extracted_csv.unlink(missing_ok=True)
            zip_path.unlink(missing_ok=True)


def _load_xlsx_rows(path: Path) -> tuple[list[dict], list[str]]:
    sheets = pd.read_excel(path, sheet_name=None)
    sheet_names = list(sheets.keys())
    rows: list[dict] = []
    for _, df in sheets.items():
        if df.empty:
            continue
        rows.extend(df.where(pd.notnull(df), None).to_dict(orient="records"))
    return rows, sheet_names


def _load_xlsx_frames(path: Path) -> tuple[dict[str, pd.DataFrame], list[str]]:
    sheets = pd.read_excel(path, sheet_name=None, header=None)
    return sheets, list(sheets.keys())


def _detect_header_row(df: pd.DataFrame, required_tokens: set[str]) -> int | None:
    for idx, row in df.iterrows():
        tokens = {_norm_key(str(v)) for v in row.tolist() if str(v).strip() and str(v).strip().lower() != "nan"}
        if required_tokens.issubset(tokens):
            return int(idx)
    return None


def _extract_tenant_rows_from_workbook(path: Path) -> tuple[list[dict], dict]:
    sheets, sheet_names = _load_xlsx_frames(path)
    chosen = [s for s in sheet_names if s.lower().startswith("table")]
    rows: list[dict] = []
    header_rows: dict[str, int | None] = {}
    for sheet_name in chosen:
        df = sheets[sheet_name]
        if df.empty:
            header_rows[sheet_name] = None
            continue
        year_header = None
        for idx, row in df.iterrows():
            vals = [v for v in row.tolist() if str(v).strip() and str(v).lower() != "nan"]
            years = []
            for v in vals:
                try:
                    y = int(float(v))
                except (ValueError, TypeError):
                    continue
                if 1990 <= y <= 2100:
                    years.append(y)
            if years:
                year_header = int(idx)
                break
        header_rows[sheet_name] = year_header
        if year_header is None:
            continue
        header_vals = [str(v).strip() for v in df.iloc[year_header].tolist()]
        year_cols = []
        for i, v in enumerate(header_vals):
            try:
                y = int(float(v))
            except (ValueError, TypeError):
                continue
            if 1990 <= y <= 2100:
                year_cols.append((i, y))
        if not year_cols:
            continue
        for ridx in range(year_header + 1, len(df)):
            r = df.iloc[ridx].tolist()
            label = next((r[i] for i in range(min(3, len(r))) if not pd.isna(r[i]) and str(r[i]).strip()), None)
            label = str(label).strip() if label not in (None, "", "nan") else None
            if label and label.lower().startswith("source"):
                continue
            for cidx, year in year_cols:
                if cidx >= len(r):
                    continue
                value = r[cidx]
                if pd.isna(value):
                    continue
                rows.append(
                    {
                        "REPORTING_YEAR": year,
                        "HOUSEHOLD_COUNT": value,
                        "HOUSEHOLD_TYPE": sheet_name,
                        "INCOME_BUCKET": label,
                    }
                )
    return rows, {"sheet_names": sheet_names, "chosen_sheets": chosen, "header_rows": header_rows}


def _safe_cell_str(value) -> str:
    if value is None or pd.isna(value):
        return ""
    sval = str(value).strip()
    return "" if sval.lower() == "nan" else sval


def _to_numeric(value):
    sval = _safe_cell_str(value)
    if not sval:
        return None
    sval = sval.replace(",", "")
    if sval.endswith("%"):
        sval = sval[:-1]
    try:
        return float(sval)
    except ValueError:
        return None


def _detect_lihtc_tenant_table_layout(sheet_df: pd.DataFrame, sheet_name: str) -> dict:
    scan_limit = min(len(sheet_df), 60)
    header_row = None
    row_label_col = 0
    title_rows: list[int] = []
    anchor_tokens = {"state", "properties", "units", "percent", "matched", "total", "year"}

    for ridx in range(scan_limit):
        row = sheet_df.iloc[ridx].tolist()
        non_empty = [(cidx, _safe_cell_str(v)) for cidx, v in enumerate(row) if _safe_cell_str(v)]
        if not non_empty:
            continue
        if len(non_empty) <= 2:
            title_rows.append(ridx)
        token_hits = sum(1 for _, cell in non_empty if _norm_key(cell) in anchor_tokens)
        numeric_hits = sum(1 for _, cell in non_empty if _to_numeric(cell) is not None or re.fullmatch(r"(19|20)\d{2}", cell))
        if header_row is None and len(non_empty) >= 2 and (numeric_hits >= 2 or token_hits >= 1):
            header_row = ridx
            break

    if header_row is None:
        return {"sheet_name": sheet_name, "header_row": None, "reason": "no_header_detected"}

    header_rows = [header_row]
    prev_row = max(header_row - 1, 0)
    if prev_row < header_row:
        prev_non_empty = [_safe_cell_str(v) for v in sheet_df.iloc[prev_row].tolist()]
        if sum(1 for v in prev_non_empty if v) >= 2:
            header_rows.insert(0, prev_row)

    value_cols: list[int] = []
    header_vals = [_safe_cell_str(v) for v in sheet_df.iloc[header_row].tolist()]
    for cidx, cell in enumerate(header_vals):
        if not cell:
            continue
        if cidx == row_label_col:
            continue
        value_cols.append(cidx)

    if not value_cols:
        return {"sheet_name": sheet_name, "header_row": header_row, "reason": "no_value_columns"}

    column_labels = {}
    for cidx in value_cols:
        parts = []
        for hidx in header_rows:
            cell = _safe_cell_str(sheet_df.iat[hidx, cidx] if cidx < sheet_df.shape[1] else None)
            if cell:
                parts.append(cell)
        column_labels[cidx] = " | ".join(parts) if parts else f"col_{cidx}"

    data_start_row = header_row + 1
    stop_row = len(sheet_df)
    for ridx in range(data_start_row, len(sheet_df)):
        first_cell = _safe_cell_str(sheet_df.iat[ridx, row_label_col] if row_label_col < sheet_df.shape[1] else None).lower()
        if first_cell.startswith("source") or first_cell.startswith("note"):
            stop_row = ridx
            break

    return {
        "sheet_name": sheet_name,
        "title_rows": title_rows,
        "header_rows": header_rows,
        "header_row": header_row,
        "row_label_col": row_label_col,
        "value_cols": value_cols,
        "column_labels": column_labels,
        "data_start_row": data_start_row,
        "stop_row": stop_row,
        "scan_limit": scan_limit,
    }


def _parse_lihtc_tenant_sheet(sheet_df: pd.DataFrame, layout: dict, sheet_name: str, data_year: int) -> tuple[list[dict], dict]:
    if layout.get("header_row") is None:
        return [], {"sheet_name": sheet_name, "warning": layout.get("reason", "missing_layout")}

    row_label_col = layout["row_label_col"]
    parsed_rows: list[dict] = []
    for ridx in range(layout["data_start_row"], layout["stop_row"]):
        row_label = _safe_cell_str(sheet_df.iat[ridx, row_label_col] if row_label_col < sheet_df.shape[1] else None)
        if not row_label:
            continue
        for cidx in layout["value_cols"]:
            cell = sheet_df.iat[ridx, cidx] if cidx < sheet_df.shape[1] else None
            value_raw = _safe_cell_str(cell)
            if not value_raw:
                continue
            numeric = _to_numeric(cell)
            parsed_rows.append(
                {
                    "data_year": data_year,
                    "sheet_name": sheet_name,
                    "table_id": sheet_name.lower().replace(" ", "_"),
                    "geography": None,
                    "geography_type": None,
                    "row_label": row_label,
                    "column_label": layout["column_labels"].get(cidx, f"col_{cidx}"),
                    "value_raw": value_raw,
                    "value_numeric": numeric,
                    "value_text": None if numeric is not None else value_raw,
                    "unit_of_measure": "percent" if "%" in value_raw else None,
                    "notes": None,
                }
            )
    diag = {
        "sheet_name": sheet_name,
        "title_rows": layout.get("title_rows", []),
        "header_rows": layout.get("header_rows", []),
        "data_start_row": layout.get("data_start_row"),
        "stop_row": layout.get("stop_row"),
        "value_col_count": len(layout.get("value_cols", [])),
        "matrix_rows_parsed": len({r["row_label"] for r in parsed_rows}),
        "normalized_rows": len(parsed_rows),
    }
    return parsed_rows, diag


def _normalize_lihtc_tenant_summary_rows(parsed_rows: list[dict], data_year: int) -> list[dict]:
    normalized: list[dict] = []
    for row in parsed_rows:
        numeric = row.get("value_numeric")
        numeric_int = int(numeric) if isinstance(numeric, (float, int)) and float(numeric).is_integer() else None
        year_match = re.search(r"\b((?:19|20)\d{2})\b", row.get("column_label") or "")
        reporting_year = int(year_match.group(1)) if year_match else data_year
        normalized.append(
            {
                "dataset_year": data_year,
                "reporting_year": reporting_year,
                "source_version": str(HUD_DATASET_YEAR),
                "source_snapshot_id": 0,
                "hud_id": None,
                "geoid11": None,
                "household_type": row.get("table_id"),
                "income_bucket": row.get("row_label"),
                "household_count": numeric_int,
                "average_household_income": float(numeric) if numeric is not None else None,
                "sheet_name": row.get("sheet_name"),
                "table_id": row.get("table_id"),
                "geography": row.get("geography"),
                "geography_type": row.get("geography_type"),
                "row_label": row.get("row_label"),
                "column_label": row.get("column_label"),
                "value_raw": row.get("value_raw"),
                "value_numeric": float(numeric) if numeric is not None else None,
                "value_text": row.get("value_text"),
                "unit_of_measure": row.get("unit_of_measure"),
                "notes": row.get("notes"),
                "join_method": "summary_table",
                "join_confidence": 0.0,
            }
        )
    return normalized


def _load_lihtc_tenant_workbook(path: Path, data_year: int) -> tuple[list[dict], dict]:
    sheets, sheet_names = _load_xlsx_frames(path)
    target_sheets = [s for s in sheet_names if s.lower().startswith("table")]
    all_parsed: list[dict] = []
    sheet_diags: list[dict] = []
    skipped: list[str] = []

    for sheet_name in target_sheets:
        sheet_df = sheets[sheet_name]
        layout = _detect_lihtc_tenant_table_layout(sheet_df, sheet_name)
        parsed_rows, diag = _parse_lihtc_tenant_sheet(sheet_df, layout, sheet_name, data_year)
        sheet_diags.append(diag)
        if parsed_rows:
            all_parsed.extend(parsed_rows)
        else:
            skipped.append(sheet_name)

    normalized = _normalize_lihtc_tenant_summary_rows(all_parsed, data_year)
    return normalized, {
        "sheet_names": sheet_names,
        "target_sheets": target_sheets,
        "sheet_diags": sheet_diags,
        "sheets_parsed": len([d for d in sheet_diags if d.get("normalized_rows", 0) > 0]),
        "sheets_skipped": skipped,
    }


def _extract_qct_rows_from_workbook(path: Path) -> tuple[list[dict], dict]:
    sheets, sheet_names = _load_xlsx_frames(path)
    chosen = [s for s in sheet_names if " to " in s.lower()]
    rows: list[dict] = []
    header_rows: dict[str, int | None] = {}
    for sheet_name in chosen:
        df = sheets[sheet_name]
        if df.empty:
            header_rows[sheet_name] = None
            continue
        header_idx = _detect_header_row(df, {"state", "county", "tract"})
        header_rows[sheet_name] = header_idx
        if header_idx is None:
            continue
        headers = [_norm_key(str(v)) or f"col{i}" for i, v in enumerate(df.iloc[header_idx].tolist())]
        for ridx in range(header_idx + 1, len(df)):
            values = df.iloc[ridx].tolist()
            rec = {headers[i]: values[i] for i in range(min(len(headers), len(values)))}
            if all(pd.isna(v) or str(v).strip() == "" for v in rec.values()):
                continue
            if "designation_type" not in rec:
                if "designationtype" in rec and rec.get("designationtype") not in (None, ""):
                    rec["designation_type"] = rec.get("designationtype")
                else:
                    qct_flag = str(rec.get("qct", "")).strip().upper()
                    dda_flag = str(rec.get("dda", "")).strip().upper()
                    rec["designation_type"] = "QCT" if qct_flag in {"1", "Y", "YES", "TRUE", "QCT"} else (
                        "DDA" if dda_flag in {"1", "Y", "YES", "TRUE", "DDA"} else rec.get("designation")
                    )
            rec["designation_year"] = rec.get("designation_year") or rec.get("designationyear") or HUD_DATASET_YEAR
            rec["tract"] = rec.get("tract") or rec.get("censustract")
            rows.append(rec)
    return rows, {"sheet_names": sheet_names, "chosen_sheets": chosen, "header_rows": header_rows}


async def _fetch_xlsx_rows(url: str, local_name: str) -> tuple[list[dict], list[str]]:
    if not url:
        raise ValueError(f"Source URL for {local_name} is empty")
    with tempfile.TemporaryDirectory(prefix=f"hud_{local_name}_") as tmp:
        path = Path(tmp) / local_name
        await _download_file(url, path)
        if "tenant" in local_name:
            rows, diag = _extract_tenant_rows_from_workbook(path)
            sheet_names = diag["sheet_names"]
            logger.info(
                "%s workbook sheets detected: %s | chosen=%s | header_rows=%s",
                local_name,
                sheet_names,
                diag["chosen_sheets"],
                diag["header_rows"],
            )
        elif "qct" in local_name:
            rows, diag = _extract_qct_rows_from_workbook(path)
            sheet_names = diag["sheet_names"]
            logger.info(
                "%s workbook sheets detected: %s | chosen=%s | header_rows=%s",
                local_name,
                sheet_names,
                diag["chosen_sheets"],
                diag["header_rows"],
            )
        else:
            rows, sheet_names = _load_xlsx_rows(path)
            logger.info("%s workbook sheets detected: %s", local_name, sheet_names)
        return rows, sheet_names


def _project_rejection_reason(raw: dict) -> str | None:
    raw = _row_with_normalized_keys(raw)
    hud_id = _first_value(raw, "HUD_ID", "hud_id", "nlihc_id", "nlihcid", "projectid", "project_id")
    if not hud_id:
        return "missing_project_id"
    name = str(_first_value(raw, "PROJECT", "project", "project_name", "proj_name", "projname") or "").strip()
    if not name:
        return "missing_project_name"
    lat = _first_value(raw, "LATITUDE", "latitude", "lat")
    lon = _first_value(raw, "LONGITUDE", "longitude", "lon", "lng")
    if lat in (None, "") or lon in (None, ""):
        return "missing_location"
    try:
        float(lat)
        float(lon)
    except (ValueError, TypeError):
        return "failed_numeric_parse"
    return None


def _tenant_rejection_reason(raw: dict) -> str | None:
    raw = _row_with_normalized_keys(raw)
    year = _resolve_value(raw, "REPORTING_YEAR", CONTRACTS["lihtc_tenant"].aliases)
    count = _resolve_value(raw, "HOUSEHOLD_COUNT", CONTRACTS["lihtc_tenant"].aliases)
    if year in (None, ""):
        return "missing_reporting_year"
    if count in (None, ""):
        return "missing_household_count"
    try:
        if _to_int(year) is None or _to_int(count) is None:
            raise ValueError("numeric parse failed")
    except (ValueError, TypeError):
        return "failed_numeric_parse"
    return None


def _qct_rejection_reason(raw: dict) -> str | None:
    raw = _row_with_normalized_keys(raw)
    year = _resolve_value(raw, "DESIGNATION_YEAR", CONTRACTS["qct_dda"].aliases) or HUD_DATASET_YEAR
    dtype = str(_resolve_value(raw, "DESIGNATION_TYPE", CONTRACTS["qct_dda"].aliases) or "").upper().strip()
    if dtype not in {"QCT", "DDA"}:
        return "filtered_out_by_rule"
    try:
        if _to_int(year) is None:
            raise ValueError("numeric parse failed")
    except (ValueError, TypeError):
        return "failed_numeric_parse"
    return None


def _transform_project(raw: dict) -> dict | None:
    aliases = CONTRACTS["lihtc_property"].aliases
    raw = _row_with_normalized_keys(raw)
    hud_id = _first_value(raw, "HUD_ID", "hud_id", "nlihc_id", "nlihcid", "projectid", "project_id") or resolve_field(raw, "HUD_ID", aliases)
    if not hud_id:
        return None
    name = str(_first_value(raw, "PROJECT", "project", "project_name", "proj_name", "projname") or resolve_field(raw, "PROJECT", aliases) or "").strip()
    if not name:
        return None

    lat = _first_value(raw, "LATITUDE", "latitude", "lat") or resolve_field(raw, "LATITUDE", aliases)
    lon = _first_value(raw, "LONGITUDE", "longitude", "lon", "lng") or resolve_field(raw, "LONGITUDE", aliases)
    try:
        lat = float(lat)
        lon = float(lon)
    except (ValueError, TypeError):
        return None

    def _int_or_none(v):
        try:
            return int(float(v)) if v not in (None, "") else None
        except (ValueError, TypeError):
            return None

    return {
        "hud_id": str(hud_id),
        "project_name": name,
        "lat": lat,
        "lon": lon,
        "city": (resolve_field(raw, "PROJ_CTY", aliases) or None),
        "state": str(resolve_field(raw, "PROJ_ST", aliases) or "")[:2] or None,
        "county_fips": str(resolve_field(raw, "FIPS", aliases) or "").zfill(5) or None,
        "street_address": resolve_field(raw, "ADDRESS", aliases),
        "zip_code": resolve_field(raw, "ZIP", aliases),
        "geoid11": str(resolve_field(raw, "TRACT", aliases) or "")[:11] or None,
        "total_units": _int_or_none(resolve_field(raw, "N_UNITS", aliases)),
        "low_income_units": _int_or_none(resolve_field(raw, "LI_UNITS", aliases)),
        "placed_in_service_year": _int_or_none(resolve_field(raw, "YR_PIS", aliases)),
        "compliance_end_year": _int_or_none(resolve_field(raw, "YR_COMP_END", aliases)),
        "extended_use_end_year": _int_or_none(resolve_field(raw, "YR_EXT_END", aliases)),
    }


def _transform_tenant(raw: dict) -> dict | None:
    aliases = CONTRACTS["lihtc_tenant"].aliases
    raw = _row_with_normalized_keys(raw)
    year = _resolve_value(raw, "REPORTING_YEAR", aliases)
    count = _resolve_value(raw, "HOUSEHOLD_COUNT", aliases)
    if year in (None, "") or count in (None, ""):
        return None
    parsed_year = _to_int(year)
    parsed_count = _to_int(count)
    if parsed_year is None or parsed_count is None:
        return None
    try:
        geoid = _resolve_value(raw, "TRACT", aliases)
        avg_income = _to_float(_resolve_value(raw, "AVG_HH_INCOME", aliases))
        return {
            "dataset_year": parsed_year,
            "reporting_year": parsed_year,
            "source_snapshot_id": 0,
            "source_version": str(HUD_DATASET_YEAR),
            "hud_id": str(_resolve_value(raw, "HUD_ID", aliases) or "") or None,
            "geoid11": str(geoid or "")[:11] or None,
            "household_type": _resolve_value(raw, "HOUSEHOLD_TYPE", aliases),
            "income_bucket": _resolve_value(raw, "INCOME_BUCKET", aliases),
            "household_count": parsed_count,
            "average_household_income": avg_income,
        }
    except (ValueError, TypeError):
        return None


def _transform_qct(raw: dict) -> dict | None:
    aliases = CONTRACTS["qct_dda"].aliases
    raw = _row_with_normalized_keys(raw)
    year = _resolve_value(raw, "DESIGNATION_YEAR", aliases) or HUD_DATASET_YEAR
    dtype = str(_resolve_value(raw, "DESIGNATION_TYPE", aliases) or "").upper().strip()
    if dtype not in {"QCT", "DDA"}:
        return None
    parsed_year = _to_int(year)
    if parsed_year is None:
        return None
    try:
        geoid = _resolve_value(raw, "TRACT", aliases)
        state = str(_resolve_value(raw, "STATE_FIPS", aliases) or "").zfill(2) or None
        county = str(_resolve_value(raw, "COUNTY_FIPS", aliases) or "")
        if county and len(county) <= 3 and state:
            county = f"{state}{county.zfill(3)}"
        county = county.zfill(5) if county else None
        return {
            "designation_year": parsed_year,
            "designation_type": dtype,
            "source_snapshot_id": 0,
            "geoid11": str(geoid or "")[:11] or None,
            "state_fips": state,
            "county_fips": county,
            "area_name": _resolve_value(raw, "AREA_NAME", aliases),
            "source_version": str(HUD_DATASET_YEAR),
        }
    except (ValueError, TypeError):
        return None


async def _table_count(session, model) -> int:
    return int((await session.execute(select(func.count()).select_from(model))).scalar() or 0)


def _chunk_rows(rows: list[dict], batch_size: int):
    size = max(int(batch_size or 1), 1)
    for i in range(0, len(rows), size):
        yield rows[i:i + size]


def _estimate_bind_params(row_count: int, column_count: int) -> int:
    return max(row_count, 0) * max(column_count, 0)


async def _commit_chunked_rows(
    *,
    step_label: str,
    model,
    rows: list[dict],
    batch_size: int,
    statement_builder,
) -> int:
    target_table = model.__tablename__
    if not rows:
        logger.info(
            "%s write target_table=%s batch_size=%s chunk_count=0 commit_attempted=no commit_succeeded=no rollback=no",
            step_label,
            target_table,
            batch_size,
        )
        return 0

    chunks = list(_chunk_rows(rows, batch_size))
    col_count = len(rows[0]) if rows else 0
    estimated_params = _estimate_bind_params(len(chunks[0]), col_count)

    logger.info(
        "%s write target_table=%s batch_size=%s chunk_count=%s estimated_bind_params_per_chunk=%s",
        step_label,
        target_table,
        max(int(batch_size or 1), 1),
        len(chunks),
        estimated_params,
    )

    async with async_session_factory() as session:
        before_count = await _table_count(session, model)
        commit_attempted = False
        commit_succeeded = False
        rollback_called = False
        affected = 0
        try:
            for chunk in chunks:
                stmt = statement_builder(chunk)
                result = await session.execute(stmt)
                affected += int(getattr(result, "rowcount", 0) or 0)

            commit_attempted = True
            await session.commit()
            commit_succeeded = True
        except Exception as exc:
            await session.rollback()
            rollback_called = True
            logger.error(
                "%s write target_table=%s batch_size=%s chunk_count=%s commit_attempted=%s commit_succeeded=%s rollback=%s exception=%s:%s",
                step_label,
                target_table,
                max(int(batch_size or 1), 1),
                len(chunks),
                "yes" if commit_attempted else "no",
                "yes" if commit_succeeded else "no",
                "yes" if rollback_called else "no",
                exc.__class__.__name__,
                str(exc)[:240],
            )
            raise

        after_count = await _table_count(session, model)
        logger.info(
            "%s write target_table=%s batch_size=%s chunk_count=%s commit_attempted=%s commit_succeeded=%s rollback=%s before=%s after=%s",
            step_label,
            target_table,
            max(int(batch_size or 1), 1),
            len(chunks),
            "yes" if commit_attempted else "no",
            "yes" if commit_succeeded else "no",
            "yes" if rollback_called else "no",
            before_count,
            after_count,
        )
        return max(affected, after_count - before_count)


def _property_upsert_statement(chunk: list[dict]):
    stmt = pg_insert(HudLihtcProperty).values(chunk)
    return stmt.on_conflict_do_update(
        constraint="uq_hud_lihtc_property_hudid_year",
        set_={col: stmt.excluded[col] for col in chunk[0].keys() if col not in {"hud_id", "dataset_year"}},
    )


def _qct_upsert_statement(chunk: list[dict]):
    stmt = pg_insert(HudQctDdaDesignation).values(chunk)
    return stmt.on_conflict_do_update(
        constraint="uq_hud_qct_dda_designation_year_type_geoid",
        set_={
            "source_snapshot_id": stmt.excluded.source_snapshot_id,
            "source_version": stmt.excluded.source_version,
            "state_fips": stmt.excluded.state_fips,
            "county_fips": stmt.excluded.county_fips,
            "area_name": stmt.excluded.area_name,
            "boundary": stmt.excluded.boundary,
        },
    )


async def _ingest_hud_property_async() -> dict:
    async with async_session_factory() as session:
        run = await start_pipeline_run(session, "hud_lihtc_property")
        await session.commit()
    try:
        raw = await _fetch_lihtc_property_rows_from_zip()
        logger.info("HUD property source_rows_loaded=%s", len(raw))
        reject = Counter()
        rows = []
        for r in raw:
            t = _transform_project(r)
            if t:
                rows.append(t)
            else:
                reject[_project_rejection_reason(r) or "filtered_out_by_rule"] += 1

        for rec in rows:
            rec["location"] = from_shape(Point(rec["lon"], rec["lat"]), srid=4326)
            rec["dataset_year"] = HUD_DATASET_YEAR
            rec["source_snapshot_id"] = 0
            rec["source_version"] = str(HUD_DATASET_YEAR)

        logger.info("HUD property normalized_rows_prepared=%s", len(rows))

        upserted = 0
        if rows:
            upserted = await _commit_chunked_rows(
                step_label="HUD property",
                model=HudLihtcProperty,
                rows=rows,
                batch_size=HUD_PROPERTY_BATCH_SIZE,
                statement_builder=_property_upsert_statement,
            )
        else:
            logger.warning(
                "HUD property write target_table=%s batch_size=%s chunk_count=0 commit_attempted=no commit_succeeded=no rollback=no top_rejection_reasons=%s",
                HudLihtcProperty.__tablename__,
                HUD_PROPERTY_BATCH_SIZE,
                _top_reasons(reject),
            )

        async with async_session_factory() as session:
            await finish_pipeline_run(session, run, status="success", records_processed=len(raw), records_inserted=upserted)
            await session.commit()
        return {"processed": len(raw), "upserted": upserted}
    except Exception as exc:
        async with async_session_factory() as session:
            await finish_pipeline_run(session, run, status="failed", error_message=str(exc))
            await session.commit()
        raise


async def _ingest_hud_tenant_async() -> dict:
    async with async_session_factory() as session:
        run = await start_pipeline_run(session, "hud_lihtc_tenant")
        await session.commit()
    try:
        with tempfile.TemporaryDirectory(prefix="hud_lihtc_tenant_") as tmp:
            path = Path(tmp) / "lihtc_tenant_2023.xlsx"
            logger.info("Downloading HUD LIHTC tenant workbook from %s to %s", HUD_LIHTC_TENANT_XLSX_URL, path)
            await _download_file(HUD_LIHTC_TENANT_XLSX_URL, path)
            rows, diag = _load_lihtc_tenant_workbook(path, HUD_DATASET_YEAR)

        logger.info(
            "lihtc_tenant_2023.xlsx workbook sheets detected: %s | target=%s | parsed=%s | skipped=%s",
            diag["sheet_names"],
            diag["target_sheets"],
            diag["sheets_parsed"],
            diag["sheets_skipped"],
        )
        for sheet_diag in diag["sheet_diags"]:
            logger.info(
                "HUD tenant sheet=%s title_rows=%s header_rows=%s data_start=%s stop=%s value_cols=%s matrix_rows=%s normalized_rows=%s warning=%s",
                sheet_diag.get("sheet_name"),
                sheet_diag.get("title_rows"),
                sheet_diag.get("header_rows"),
                sheet_diag.get("data_start_row"),
                sheet_diag.get("stop_row"),
                sheet_diag.get("value_col_count"),
                sheet_diag.get("matrix_rows_parsed"),
                sheet_diag.get("normalized_rows", 0),
                sheet_diag.get("warning"),
            )

        logger.info("HUD tenant source_rows_loaded=%s", len(rows))
        logger.info("HUD tenant normalized_rows_prepared=%s", len(rows))

        if diag["sheets_parsed"] == 0 or not rows:
            raise RuntimeError("HUD tenant workbook parse produced zero rows across all sheets")

        inserted = await _commit_chunked_rows(
            step_label="HUD tenant",
            model=HudLihtcTenant,
            rows=rows,
            batch_size=HUD_TENANT_BATCH_SIZE,
            statement_builder=lambda chunk: pg_insert(HudLihtcTenant).values(chunk),
        )

        async with async_session_factory() as session:
            await finish_pipeline_run(session, run, status="success", records_processed=len(rows), records_inserted=inserted)
            await session.commit()
        return {"processed": len(rows), "upserted": inserted}
    except Exception as exc:
        async with async_session_factory() as session:
            await finish_pipeline_run(session, run, status="failed", error_message=str(exc))
            await session.commit()
        raise


async def _ingest_hud_qct_async() -> dict:
    async with async_session_factory() as session:
        run = await start_pipeline_run(session, "hud_qct_dda")
        await session.commit()
    try:
        raw, _ = await _fetch_xlsx_rows(HUD_QCT_DDA_XLSX_URL, "qct_dda_2026.xlsx")
        logger.info("HUD qct source_rows_loaded=%s", len(raw))
        reject = Counter()
        rows = []
        for r in raw:
            t = _transform_qct(r)
            if t:
                rows.append(t)
            else:
                reject[_qct_rejection_reason(r) or "filtered_out_by_rule"] += 1

        rows = [{**row, "boundary": None} for row in rows]
        # Deduplicate by unique key — HUD source data may contain duplicate
        # tract entries, and PostgreSQL's ON CONFLICT DO UPDATE cannot touch
        # the same row twice in a single statement.
        seen_keys: set[tuple] = set()
        deduped: list[dict] = []
        for row in rows:
            key = (row.get("designation_year"), row.get("designation_type"), row.get("geoid11"))
            if key not in seen_keys:
                seen_keys.add(key)
                deduped.append(row)
        if len(deduped) < len(rows):
            logger.info("HUD qct deduplicated %d → %d rows", len(rows), len(deduped))
        rows = deduped
        logger.info("HUD qct normalized_rows_prepared=%s", len(rows))

        inserted = 0
        logger.info(
            "HUD qct normalization prepared=%s rejected=%s top_rejection_reasons=%s",
            len(rows),
            max(len(raw) - len(rows), 0),
            _top_reasons(reject),
        )
        if rows:
            inserted = await _commit_chunked_rows(
                step_label="HUD qct",
                model=HudQctDdaDesignation,
                rows=rows,
                batch_size=HUD_QCT_BATCH_SIZE,
                statement_builder=_qct_upsert_statement,
            )
        else:
            logger.warning(
                "HUD qct write target_table=%s batch_size=%s chunk_count=0 commit_attempted=no commit_succeeded=no rollback=no top_rejection_reasons=%s",
                HudQctDdaDesignation.__tablename__,
                HUD_QCT_BATCH_SIZE,
                _top_reasons(reject),
            )

        async with async_session_factory() as session:
            await finish_pipeline_run(session, run, status="success", records_processed=len(rows), records_inserted=inserted)
            await session.commit()
        return {"processed": len(rows), "upserted": inserted}
    except Exception as exc:
        async with async_session_factory() as session:
            await finish_pipeline_run(session, run, status="failed", error_message=str(exc))
            await session.commit()
        raise


@celery_app.task(name="pipeline.ingest_housing.ingest_hud_property", bind=True)
def ingest_hud_property(self):
    return asyncio.new_event_loop().run_until_complete(_ingest_hud_property_async())


@celery_app.task(name="pipeline.ingest_housing.ingest_hud_tenant", bind=True)
def ingest_hud_tenant(self):
    return asyncio.new_event_loop().run_until_complete(_ingest_hud_tenant_async())


@celery_app.task(name="pipeline.ingest_housing.ingest_hud_qct", bind=True)
def ingest_hud_qct(self):
    return asyncio.new_event_loop().run_until_complete(_ingest_hud_qct_async())


@celery_app.task(name="pipeline.ingest_housing.ingest_lihtc_data", bind=True)
def ingest_lihtc_data(self):
    return asyncio.new_event_loop().run_until_complete(_ingest_housing_all_async())


async def _ingest_housing_all_async() -> dict:
    summary = {}
    for step, fn in {
        "lihtc_property": _ingest_hud_property_async,
        "lihtc_tenant": _ingest_hud_tenant_async,
        "qct_dda": _ingest_hud_qct_async,
    }.items():
        try:
            summary[step] = {"status": "success", **(await fn())}
        except Exception as exc:
            summary[step] = {"status": "failed", "error": str(exc)}
    return summary
