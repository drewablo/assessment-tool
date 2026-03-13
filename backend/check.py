"""
Diagnostic script: find the actual POEMPH column name in the cached PSS CSV.

Run from the backend directory:
    python3 check_pss_columns.py
"""

import pandas as pd
from pathlib import Path
import os
import platform

def find_pss_file():
    custom = os.getenv("PSS_DATA_DIR")
    if custom:
        return Path(custom).expanduser() / "pss_schools.csv"

    home = Path.home()
    system = platform.system()

    if system == "Darwin":
        return home / "Library" / "Caches" / "academy-feasibility" / "backend" / "pss_schools.csv"

    xdg = os.getenv("XDG_CACHE_HOME")
    if xdg:
        return Path(xdg) / "academy-feasibility" / "backend" / "pss_schools.csv"

    return home / ".cache" / "academy-feasibility" / "backend" / "pss_schools.csv"


pss_path = find_pss_file()
print(f"Looking for PSS file at: {pss_path}")

if not pss_path.exists():
    print("ERROR: PSS file not found. Run the app once to download it.")
    exit(1)

print(f"File size: {pss_path.stat().st_size / 1024:.0f} KB\n")

df = pd.read_csv(pss_path, encoding="latin-1", nrows=5, low_memory=False)
df.columns = df.columns.str.strip()

# Search for POEMPH-like columns
matches = [c for c in df.columns if "OEMPH" in c.upper() or "EMPH" in c.upper() or "PROGRAM" in c.upper()]
print(f"Columns matching POEMPH/EMPH/PROGRAM: {matches}")
print()

# Also check the columns we rely on
expected = ["PINST", "LATITUDE22", "LONGITUDE22", "ORIENT", "PCITY", "PZIP", "NUMSTUDS", "P335", "GRADE2", "POEMPH"]
for col in expected:
    found = col in df.columns
    if not found:
        # Look for close matches (e.g. with year suffix)
        close = [c for c in df.columns if col.rstrip("0123456789") in c.upper() or c.upper().startswith(col[:4])]
        print(f"  {col}: MISSING  -->  close matches: {close}")
    else:
        print(f"  {col}: OK")

print(f"\nTotal columns in file: {len(df.columns)}")
print(f"\nAll columns:\n{sorted(df.columns.tolist())}")
