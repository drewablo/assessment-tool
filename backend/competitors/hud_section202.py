"""CSV-based fallback loader for HUD Section 202 senior housing properties.

Mirrors the pattern in hud_lihtc.py: loads from a local CSV file when the
database (USE_DB) is unavailable.  The CSV is expected to be populated by
the pipeline.ingest_hud_section202 ingestion step or a manual export.
"""

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from utils import haversine_miles

DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "hud_section_202_properties.csv"

_S202_CACHE_DF: pd.DataFrame | None = None
_S202_CACHE_SIG: tuple[int, int] | None = None


def _pick_column(df: pd.DataFrame, aliases: list[str]) -> str | None:
    by_lower = {c.lower(): c for c in df.columns}
    for alias in aliases:
        match = by_lower.get(alias.lower())
        if match:
            return match
    return None


def _load_section202_df() -> pd.DataFrame:
    global _S202_CACHE_DF, _S202_CACHE_SIG
    if not DATA_FILE.exists():
        return pd.DataFrame()

    stat = DATA_FILE.stat()
    sig = (int(stat.st_mtime), stat.st_size)
    if _S202_CACHE_DF is not None and _S202_CACHE_SIG == sig:
        return _S202_CACHE_DF.copy()

    df = pd.read_csv(DATA_FILE)
    _S202_CACHE_DF = df
    _S202_CACHE_SIG = sig
    return df.copy()


def get_nearby_section202_projects(lat: float, lon: float, radius_miles: float) -> List[Dict[str, Any]]:
    """Return Section 202 senior housing properties within radius_miles of (lat, lon)."""
    if not DATA_FILE.exists():
        return []

    df = _load_section202_df()
    lat_col = _pick_column(df, ["lat", "latitude"])
    lon_col = _pick_column(df, ["lon", "lng", "longitude"])
    if not lat_col or not lon_col:
        return []

    # Prefer HUD's visual site-name field first; this is the canonical property
    # label for Section 202 records. Fall back to property/name only when needed.
    servicing_name_col = _pick_column(
        df,
        [
            "servicing_site_name",
            "SERVICING_SITE_NAME_TEXT",
            "servicing_site_name_text",
            "site_name",
        ],
    )
    property_name_col = _pick_column(df, ["property_name", "PROPERTY_NAME_TEXT", "property_name_text"])
    name_col = _pick_column(df, ["name", "project_name"])
    city_col = _pick_column(df, ["city", "std_city"])
    state_col = _pick_column(df, ["state", "std_st"])
    address_col = _pick_column(df, ["street_address", "std_addr"])
    zip_col = _pick_column(df, ["zip_code", "std_zip5"])
    units_col = _pick_column(df, ["total_assisted_units", "assisted_units"])
    total_units_col = _pick_column(df, ["total_units", "total_unit_count"])
    client_group_col = _pick_column(df, ["client_group_name"])
    property_category_col = _pick_column(df, ["property_category", "property_category_name"])
    financing_col = _pick_column(df, ["primary_financing_type"])
    phone_col = _pick_column(df, ["phone_number", "property_on_site_phone_number"])
    reac_col = _pick_column(df, ["reac_inspection_score", "reac_last_inspection_score"])

    df = df.rename(columns={lat_col: "lat", lon_col: "lon"})
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df = df.dropna(subset=["lat", "lon"])

    if df.empty:
        return []

    # Bounding box pre-filter for performance
    lat_buf = radius_miles / 69.0
    lon_buf = radius_miles / 50.0
    df = df[df["lat"].between(lat - lat_buf, lat + lat_buf) & df["lon"].between(lon - lon_buf, lon + lon_buf)]

    if df.empty:
        return []

    df["distance_miles"] = df.apply(lambda r: haversine_miles(lat, lon, r["lat"], r["lon"]), axis=1)
    df = df[df["distance_miles"] <= radius_miles].sort_values("distance_miles")

    def _safe_int(val):
        try:
            return int(float(val)) if pd.notna(val) else None
        except (ValueError, TypeError):
            return None

    def _safe_str(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        s = str(val).strip()
        return s if s and s.lower() not in ("none", "nan") else None

    records: list[dict] = []
    for _, r in df.iterrows():
        display_name = (
            _safe_str(r.get(servicing_name_col) if servicing_name_col else None)
            or _safe_str(r.get(property_name_col) if property_name_col else None)
            or _safe_str(r.get(name_col) if name_col else None)
            or "HUD Section 202 Property"
        )
        records.append(
            {
                "name": display_name,
                "lat": float(r["lat"]),
                "lon": float(r["lon"]),
                "distance_miles": round(float(r["distance_miles"]), 2),
                "city": _safe_str(r.get(city_col) if city_col else None),
                "state": _safe_str(r.get(state_col) if state_col else None),
                "street_address": _safe_str(r.get(address_col) if address_col else None),
                "zip_code": _safe_str(r.get(zip_col) if zip_col else None),
                "li_units": _safe_int(r.get(units_col) if units_col else None),
                "total_units": _safe_int(r.get(total_units_col) if total_units_col else None),
                "affiliation": "HUD Section 202",
                "is_catholic": False,
                "is_qct": False,
                "is_dda": False,
                "tenant_households": None,
                "client_group_name": _safe_str(r.get(client_group_col) if client_group_col else None),
                "property_category": _safe_str(r.get(property_category_col) if property_category_col else None),
                "primary_financing_type": _safe_str(r.get(financing_col) if financing_col else None),
                "phone_number": _safe_str(r.get(phone_col) if phone_col else None),
                "reac_inspection_score": _safe_int(r.get(reac_col) if reac_col else None),
                "source_type": "hud_section_202",
            }
        )
    return records
