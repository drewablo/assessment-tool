from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from utils import haversine_miles

DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "hud_lihtc_projects.csv"



_LIHTC_CACHE_DF: pd.DataFrame | None = None
_LIHTC_CACHE_SIG: tuple[int, int] | None = None


def _pick_column(df: pd.DataFrame, aliases: list[str]) -> str | None:
    by_lower = {c.lower(): c for c in df.columns}
    for alias in aliases:
        match = by_lower.get(alias.lower())
        if match:
            return match
    return None


def _load_lihtc_df() -> pd.DataFrame:
    global _LIHTC_CACHE_DF, _LIHTC_CACHE_SIG
    if not DATA_FILE.exists():
        return pd.DataFrame()

    stat = DATA_FILE.stat()
    sig = (int(stat.st_mtime), stat.st_size)
    if _LIHTC_CACHE_DF is not None and _LIHTC_CACHE_SIG == sig:
        return _LIHTC_CACHE_DF.copy()

    df = pd.read_csv(DATA_FILE)
    _LIHTC_CACHE_DF = df
    _LIHTC_CACHE_SIG = sig
    return df.copy()

def get_nearby_lihtc_projects(lat: float, lon: float, radius_miles: float) -> List[Dict[str, Any]]:
    if not DATA_FILE.exists():
        return []

    df = _load_lihtc_df()
    lat_col = _pick_column(df, ["lat", "latitude", "y", "Y"])
    lon_col = _pick_column(df, ["lon", "lng", "longitude", "x", "X"])
    if not lat_col or not lon_col:
        return []

    name_col = _pick_column(df, ["project_name", "project", "proj_add", "name"])
    city_col = _pick_column(df, ["city", "proj_cty"])
    li_units_col = _pick_column(df, ["li_units", "low_income_units", "units"])

    df = df.rename(columns={lat_col: "lat", lon_col: "lon"})

    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df = df.dropna(subset=["lat", "lon"])

    if df.empty:
        return []

    lat_buf = radius_miles / 69.0
    lon_buf = radius_miles / 50.0
    df = df[df["lat"].between(lat - lat_buf, lat + lat_buf) & df["lon"].between(lon - lon_buf, lon + lon_buf)]

    if df.empty:
        return []

    df["distance_miles"] = df.apply(lambda r: haversine_miles(lat, lon, r["lat"], r["lon"]), axis=1)
    df = df[df["distance_miles"] <= radius_miles].sort_values("distance_miles")

    records = []
    for _, r in df.iterrows():
        li_units = pd.to_numeric(r.get(li_units_col), errors="coerce") if li_units_col else None
        records.append(
            {
                "name": (r.get(name_col) if name_col else None) or "LIHTC Project",
                "lat": float(r["lat"]),
                "lon": float(r["lon"]),
                "distance_miles": round(float(r["distance_miles"]), 2),
                "city": r.get(city_col) if city_col else None,
                "li_units": int(li_units) if pd.notna(li_units) else None,
                "affiliation": "HUD LIHTC",
                "is_catholic": False,
            }
        )
    return records
