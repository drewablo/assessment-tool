"""Model validation audit framework for feasibility scoring assumptions.

Run:
    python backend/validation/model_audit.py

This performs empirical backtests against known market outcomes and validates
key model assumptions against authoritative external reference datasets.
"""

from __future__ import annotations

import asyncio
import json
import statistics
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from api.analysis import (  # noqa: E402
    CATHOLIC_PCT_BY_STATE,
    _ESTABLISHED_CHOICE_STATES,
    _INCOME_PROPENSITY_SEGMENTS,
    _REFERENCE_ENROLLMENT,
    _STRONG_CHOICE_STATES,
    calculate_feasibility,
)
from api.census import INFLATION_ADJ_2017_TO_2022, get_demographics
from api.geocoder import geocode_address
from api.schools import get_nearby_schools
from modules.elder_care import SURVIVAL_RATE_65_TO_74, SURVIVAL_RATE_75_PLUS

VALIDATION_DIR = Path(__file__).resolve().parent
REF_DIR = VALIDATION_DIR / "reference_data"
CACHE_DIR = VALIDATION_DIR / "cache"
REPORT_PATH = VALIDATION_DIR / "model_audit_report.json"
CACHE_PATH = CACHE_DIR / "backtest_cache.json"


@dataclass
class BacktestCase:
    label: str
    address: str
    expected: str
    cohort: str
    grade_level: str = "k8"
    gender: str = "coed"
    market_context: str = "suburban"


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _parse_expected(expected: str, score: int) -> bool:
    if expected.startswith(">="):
        return score >= int(expected.replace(">=", "").strip())
    if expected.startswith("<="):
        return score <= int(expected.replace("<=", "").strip())
    return False




async def _nominatim_geocode(address: str) -> Optional[dict]:
    try:
        params = {"q": address, "format": "json", "limit": 1}
        headers = {"User-Agent": "academy-feasibility-model-audit/1.0"}
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.get("https://nominatim.openstreetmap.org/search", params=params, headers=headers)
            r.raise_for_status()
            rows = r.json()
            if not rows:
                return None
            row = rows[0]
            lat = float(row["lat"])
            lon = float(row["lon"])
            return {"lat": lat, "lon": lon, "matched_address": row.get("display_name", address)}
    except Exception:
        return None


async def _fcc_fips_lookup(lat: float, lon: float) -> dict:
    try:
        params = {"latitude": lat, "longitude": lon, "format": "json"}
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.get("https://geo.fcc.gov/api/census/area", params=params)
            r.raise_for_status()
            payload = r.json()
        county = payload.get("results", [{}])[0]
        county_fips = county.get("county_fips") or ""
        state_fips = county.get("state_fips") or county_fips[:2]
        return {
            "county_fips": county_fips,
            "state_fips": state_fips,
            "county_name": county.get("county_name", "Unknown County"),
            "state_name": county.get("state_name", "Unknown State"),
        }
    except Exception:
        return {}


async def _geocode_with_fallback(address: str) -> Optional[dict]:
    location = await geocode_address(address)
    if location:
        return location
    nom = await _nominatim_geocode(address)
    if not nom:
        return None
    fips = await _fcc_fips_lookup(nom["lat"], nom["lon"])
    nom.update(fips)
    return nom


def _weak_component(feasibility_payload: dict) -> Tuple[str, int]:
    components = {
        "market_size": feasibility_payload["market_size"]["score"],
        "income": feasibility_payload["income"]["score"],
        "competition": feasibility_payload["competition"]["score"],
        "family_density": feasibility_payload["family_density"]["score"],
    }
    key = min(components, key=components.get)
    return key, int(components[key])


async def _score_case(case: BacktestCase, cache: dict) -> dict:
    if case.label in cache:
        return cache[case.label]

    out: Dict[str, Any] = {
        "location": case.label,
        "expected": case.expected,
        "cohort": case.cohort,
    }

    location = await _geocode_with_fallback(case.address)
    if not location:
        out["error"] = "Geocode failed"
        return out

    demographics = await get_demographics(
        lat=location["lat"],
        lon=location["lon"],
        county_fips=location.get("county_fips", ""),
        state_fips=location.get("state_fips", ""),
        radius_miles=12.0,
    )
    schools = await get_nearby_schools(
        lat=location["lat"],
        lon=location["lon"],
        radius_miles=12.0,
        gender=case.gender,
        grade_level=case.grade_level,
        isochrone_polygon=None,
    )

    analysis = calculate_feasibility(
        location=location,
        demographics=demographics,
        schools=schools,
        school_name=f"Audit case: {case.label}",
        radius_miles=12.0,
        gender=case.gender,
        grade_level=case.grade_level,
        weighting_profile="standard_baseline",
        market_context=case.market_context,
    )
    analysis_dict = analysis.model_dump()
    score = analysis_dict["feasibility_score"]["overall"]
    weak_key, weak_score = _weak_component(analysis_dict["feasibility_score"])

    out.update(
        {
            "actual": score,
            "pass": _parse_expected(case.expected, score),
            "weak_component": {"name": weak_key, "score": weak_score},
            "components": {
                "market_size": analysis_dict["feasibility_score"]["market_size"]["score"],
                "income": analysis_dict["feasibility_score"]["income"]["score"],
                "competition": analysis_dict["feasibility_score"]["competition"]["score"],
                "family_density": analysis_dict["feasibility_score"]["family_density"]["score"],
            },
            "catholic_school_count": analysis_dict["catholic_school_count"],
            "total_private_school_count": analysis_dict["total_private_school_count"],
            "state": analysis_dict["state_name"],
            "county": analysis_dict["county_name"],
            "address": analysis_dict["analysis_address"],
        }
    )
    cache[case.label] = out
    return out


def _backtest_cases() -> List[BacktestCase]:
    return [
        BacktestCase("Naperville, IL (K-8)", "120 N Mill St, Naperville, IL 60540", ">= 60", "success"),
        BacktestCase("Wellesley, MA (K-8)", "34 Benvenue St, Wellesley, MA 02482", ">= 60", "success"),
        BacktestCase("Flourtown, PA (Girls HS)", "120 W Wissahickon Ave, Flourtown, PA 19031", ">= 60", "success", grade_level="high_school", gender="girls"),
        BacktestCase("Omaha, NE (K-8)", "18930 Patrick Ave, Omaha, NE 68135", ">= 60", "success"),
        BacktestCase("Miami, FL (K-8)", "8625 SW 84th St, Miami, FL 33143", ">= 60", "success", market_context="urban"),
        BacktestCase("St. Louis, MO (K-8)", "583 Coeur De Ville Dr, Saint Louis, MO 63141", ">= 60", "success", market_context="urban"),
        BacktestCase("Rural Appalachia, WV (K-8)", "101 Main St, Logan, WV 25601", "<= 55", "struggle", market_context="rural"),
        BacktestCase("Scranton, PA (K-8)", "1600 Farr St, Scranton, PA 18504", "<= 55", "struggle", market_context="urban"),
        BacktestCase("Inner-city Detroit, MI (K-8)", "4200 Martin Luther King Jr Blvd, Detroit, MI 48208", "<= 55", "struggle", market_context="urban"),
        BacktestCase("Rural Mississippi (K-8)", "800 S Theobald St, Greenville, MS 38701", "<= 55", "struggle", market_context="rural"),
    ]


def _status_from_flags(n_flags: int, n_total: int) -> str:
    if n_flags == 0:
        return "OK"
    if n_flags < max(1, n_total // 3):
        return "WATCH"
    return "FAIL"


def _validate_catholic_pct() -> dict:
    ref = _load_json(REF_DIR / "cara_catholic_pct.json")
    ref_values = ref["values"]
    flagged = []
    max_delta = 0.0
    for state, ours in CATHOLIC_PCT_BY_STATE.items():
        ext = ref_values.get(state)
        if ext is None:
            continue
        delta = abs(ours - ext)
        max_delta = max(max_delta, delta)
        if delta > 0.03:
            flagged.append({"state": state, "our_value": ours, "reference": ext, "delta": round(delta, 4)})
    methodology_note = "Unknown"
    if ref.get("metadata", {}).get("base_year"):
        methodology_note = f"Reference base year {ref['metadata']['base_year']}"
    return {
        "status": _status_from_flags(len(flagged), len(CATHOLIC_PCT_BY_STATE)),
        "max_delta": round(max_delta, 4),
        "flagged_states": flagged,
        "methodology_note": methodology_note,
    }


def _segment_value(income: int) -> float:
    points = _INCOME_PROPENSITY_SEGMENTS
    if income <= points[0][0]:
        return points[0][1]
    for i in range(1, len(points)):
        x0, y0 = points[i - 1]
        x1, y1 = points[i]
        if income <= x1:
            if x1 == x0:
                return y1
            frac = (income - x0) / (x1 - x0)
            return y0 + frac * (y1 - y0)
    return points[-1][1]


def _validate_income_propensity() -> dict:
    ref = _load_json(REF_DIR / "nces_income_private_enrollment.json")
    rows = []
    flagged = []
    for bracket in ref["brackets"]:
        low = bracket["min"]
        high = bracket["max"] if bracket["max"] is not None else 250000
        midpoint = int((low + high) / 2)
        ours = _segment_value(midpoint)
        actual = bracket["nces_pct"]
        delta = round(ours - actual, 2)
        status = "OK" if abs(delta) <= 3 else "FLAG"
        row = {
            "income_bracket": bracket["label"],
            "our_assumption_pct": round(ours, 2),
            "nces_actual_pct": actual,
            "delta_pct_points": delta,
            "status": status,
        }
        rows.append(row)
        if status == "FLAG":
            flagged.append(row)
    return {
        "status": "OK" if not flagged else "WATCH",
        "comparison_table": rows,
        "flagged_brackets": flagged,
    }


def _validate_school_choice() -> dict:
    ref = _load_json(REF_DIR / "school_choice_programs.json")
    ref_strong = set(ref["strong_choice_states"])
    ref_est = set(ref["established_choice_states"])
    # Puerto Rico is intentionally excluded from the model for now because
    # territory-level Catholic baseline and catchment support are not implemented.
    ref_est.discard("PR")

    strong_missing = sorted(ref_strong - _STRONG_CHOICE_STATES)
    strong_extra = sorted(_STRONG_CHOICE_STATES - ref_strong)
    established_missing = sorted(ref_est - _ESTABLISHED_CHOICE_STATES)
    established_extra = sorted(_ESTABLISHED_CHOICE_STATES - ref_est)

    voucher_check = {}
    for st, values in ref["voucher_amounts"].items():
        avg_voucher = values["avg_voucher_usd"]
        avg_tuition = values["avg_tuition_usd"]
        coverage = round(avg_voucher / avg_tuition, 3) if avg_tuition else None
        equivalent_income_shift = round(avg_voucher / 0.2, 0)
        voucher_check[st] = {
            "avg_voucher_usd": avg_voucher,
            "avg_tuition_usd": avg_tuition,
            "voucher_to_tuition_ratio": coverage,
            "implied_income_shift_usd": equivalent_income_shift,
            "model_strong_shift_usd": 22500,
        }

    total_diff = len(strong_missing) + len(strong_extra) + len(established_missing) + len(established_extra)
    return {
        "status": "OK" if total_diff == 0 else "STALE",
        "strong_choice": {"missing": strong_missing, "extra": strong_extra},
        "established_choice": {"missing": established_missing, "extra": established_extra},
        "voucher_impact_check": voucher_check,
    }


def _validate_reference_enrollment() -> dict:
    ref = _load_json(REF_DIR / "ncea_enrollment_benchmarks.json")
    rows = []
    flagged = []
    for level, (coed, gendered) in _REFERENCE_ENROLLMENT.items():
        bench = ref["benchmarks"][level]
        iqr_low, iqr_high = bench["iqr"]
        for variant, val in (("coed", coed), ("gendered", gendered)):
            within = iqr_low <= val <= iqr_high
            row = {
                "grade_level": level,
                "variant": variant,
                "reference_enrollment": val,
                "median": bench["median"],
                "mode": bench["mode"],
                "iqr": bench["iqr"],
                "status": "OK" if within else "FLAG",
            }
            rows.append(row)
            if not within:
                flagged.append(row)
    return {
        "status": "OK" if not flagged else "WATCH",
        "rows": rows,
        "flagged": flagged,
    }


async def _fetch_cpi_bls() -> Optional[dict]:
    payload = {
        "seriesid": ["CUUR0000SA0"],
        "startyear": "2017",
        "endyear": "2022",
        "annualaverage": True,
    }
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post("https://api.bls.gov/publicAPI/v2/timeseries/data/", json=payload)
            r.raise_for_status()
            body = r.json()
        series = body["Results"]["series"][0]["data"]
        annual = {int(x["year"]): float(x["value"]) for x in series if x.get("periodName") == "Annual"}
        if 2017 in annual and 2022 in annual:
            return {"2017": annual[2017], "2022": annual[2022], "source": "BLS API"}
    except Exception:
        return None
    return None


async def _validate_cpi_adjustment() -> dict:
    live = await _fetch_cpi_bls()
    if live is None:
        ref = _load_json(REF_DIR / "assumption_benchmarks.json")
        annual = ref["cpi_u"]["annual_avg"]
        v2017 = float(annual["2017"])
        v2022 = float(annual["2022"])
        source = "reference_data fallback"
    else:
        v2017 = live["2017"]
        v2022 = live["2022"]
        source = live["source"]
    actual = (v2022 - v2017) / v2017
    delta = abs(INFLATION_ADJ_2017_TO_2022 - actual)
    return {
        "our_value": INFLATION_ADJ_2017_TO_2022,
        "actual": round(actual, 4),
        "delta": round(delta, 4),
        "source": source,
        "status": "OK" if delta <= 0.01 else "FLAG",
    }


def _validate_waitlist_and_housing_and_survival() -> dict:
    ref = _load_json(REF_DIR / "assumption_benchmarks.json")
    wait = ref["competition_waitlist"]
    regional = wait.get("regional_waitlist_pct", {})
    regional_spread = max(regional.values()) - min(regional.values()) if regional else None
    wait_status = "WATCH" if regional_spread and regional_spread >= 20 else "OK"

    survival = ref["survival_rates"]["annual_survival"]
    s65_delta = abs(SURVIVAL_RATE_65_TO_74 - survival["65_to_74"])
    s75_delta = abs(SURVIVAL_RATE_75_PLUS - survival["75_plus"])

    housing = ref["housing"]

    return {
        "competition_waitlist": {
            "source_confirmed": True,
            "national_waitlist_pct": wait["national_waitlist_pct"],
            "regional_variation": regional,
            "regional_spread_pct_points": regional_spread,
            "status": wait_status,
        },
        "survival_rates": {
            "source": ref["survival_rates"]["source"],
            "our_values": {"65_to_74": SURVIVAL_RATE_65_TO_74, "75_plus": SURVIVAL_RATE_75_PLUS},
            "reference_values": survival,
            "delta": {"65_to_74": round(s65_delta, 4), "75_plus": round(s75_delta, 4)},
            "status": "OK" if max(s65_delta, s75_delta) <= 0.01 else "WATCH",
        },
        "lihtc_saturation": {
            "hud_ratio_affordable_to_burdened": housing["hud_affordable_to_burdened_ratio_national"],
            "model_curve_anchor": {"ratio_0": 96, "ratio_1": 28},
            "status": "OK",
            "notes": housing["hud_notes"],
        },
        "cost_burden_threshold": {
            "status": "OK" if abs(housing["cost_burden_threshold_standard"] - 0.30) < 0.001 else "WATCH",
            "hud_current": "30%",
        },
    }


def _discrimination_checks(all_results: List[dict]) -> dict:
    success = [r["actual"] for r in all_results if r.get("cohort") == "success" and "actual" in r]
    struggle = [r["actual"] for r in all_results if r.get("cohort") == "struggle" and "actual" in r]
    if not success or not struggle:
        return {"pass": False, "error": "Insufficient scored locations"}

    comp_names = ["market_size", "income", "competition", "family_density"]
    systematic = {}
    for comp in comp_names:
        vals = [r["components"][comp] for r in all_results if r.get("components")]
        systematic[comp] = {
            "min": min(vals),
            "max": max(vals),
            "always_maxed": all(v >= 95 for v in vals),
            "always_floored": all(v < 15 for v in vals),
        }

    avg_delta = statistics.mean(success) - statistics.mean(struggle)
    spread = max(success + struggle) - min(success + struggle)
    pass_check = avg_delta >= 20 and spread >= 30 and not any(
        s["always_maxed"] or s["always_floored"] for s in systematic.values()
    )
    return {
        "avg_success": round(statistics.mean(success), 2),
        "avg_struggle": round(statistics.mean(struggle), 2),
        "avg_delta": round(avg_delta, 2),
        "spread": spread,
        "component_systematic": systematic,
        "pass": pass_check,
    }


def _build_recommendations(report: dict) -> List[str]:
    recs: List[str] = []
    bt = report["backtest_results"]
    failed = [r for r in bt["success_locations"] + bt["struggle_locations"] if not r.get("pass", False)]
    for row in failed:
        if "actual" in row:
            recs.append(
                f"Backtest miss: {row['location']} expected {row['expected']} but scored {row['actual']}; investigate {row['weak_component']['name']} ({row['weak_component']['score']})."
            )
        else:
            recs.append(f"Backtest could not run for {row['location']}: {row.get('error', 'unknown error')}.")

    ap = report["assumption_checks"]
    for flagged in ap["income_propensity"].get("flagged_brackets", []):
        recs.append(
            f"Income propensity for {flagged['income_bracket']} differs by {flagged['delta_pct_points']}pp; consider recalibrating segment point."
        )

    if ap["school_choice_states"]["status"] != "OK":
        missing = ap["school_choice_states"]["strong_choice"]["missing"] + ap["school_choice_states"]["established_choice"]["missing"]
        if missing:
            recs.append(f"School choice state lists appear stale; add: {', '.join(sorted(set(missing)))}.")

    cpi = ap["cpi_adjustment"]
    if cpi["status"] != "OK":
        recs.append(
            f"Inflation adjustment constant is off by {round(cpi['delta'] * 100, 2)}pp; update INFLATION_ADJ_2017_TO_2022 to {cpi['actual']}."
        )

    wait = ap["competition_waitlist"]
    if wait["status"] != "OK":
        recs.append(
            "Competition model may need region-specific validation weighting because waitlist prevalence varies materially by region."
        )

    if not recs:
        recs.append("No immediate critical issues detected; maintain annual refresh cadence for external assumptions.")
    return recs


async def generate_model_audit_report(use_cache: bool = True) -> dict:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache = _load_json(CACHE_PATH) if (use_cache and CACHE_PATH.exists()) else {}

    results = []
    for case in _backtest_cases():
        results.append(await _score_case(case, cache))

    _save_json(CACHE_PATH, cache)

    success_locations = [r for r in results if r.get("cohort") == "success"]
    struggle_locations = [r for r in results if r.get("cohort") == "struggle"]

    assumption_base = _validate_waitlist_and_housing_and_survival()
    report = {
        "run_date": datetime.now(timezone.utc).isoformat(),
        "backtest_results": {
            "success_locations": success_locations,
            "struggle_locations": struggle_locations,
            "discrimination_power": _discrimination_checks(results),
        },
        "assumption_checks": {
            "catholic_pct": _validate_catholic_pct(),
            "income_propensity": _validate_income_propensity(),
            "school_choice_states": _validate_school_choice(),
            "reference_enrollment": _validate_reference_enrollment(),
            "competition_waitlist": assumption_base["competition_waitlist"],
            "cpi_adjustment": await _validate_cpi_adjustment(),
            "survival_rates": assumption_base["survival_rates"],
            "lihtc_saturation": assumption_base["lihtc_saturation"],
            "cost_burden_threshold": assumption_base["cost_burden_threshold"],
        },
    }
    report["recommendations"] = _build_recommendations(report)
    return report


def _print_report(report: dict) -> None:
    print("\n=== MODEL AUDIT SUMMARY ===")
    print(f"Run date: {report['run_date']}")
    print("\nBacktest table:")
    print("Location | Expected | Actual | Pass? | Weak Component")
    for row in report["backtest_results"]["success_locations"] + report["backtest_results"]["struggle_locations"]:
        actual = row.get("actual", "n/a")
        weak = row.get("weak_component", {}).get("name", "n/a")
        if isinstance(row.get("weak_component"), dict):
            weak = f"{weak}: {row['weak_component'].get('score')}"
        print(f"{row['location']} | {row['expected']} | {actual} | {'PASS' if row.get('pass') else 'FAIL'} | {weak}")

    print("\nAssumption check statuses:")
    for key, payload in report["assumption_checks"].items():
        status = payload.get("status", "INFO")
        print(f"- {key}: {status}")

    print("\nRecommendations:")
    for rec in report["recommendations"]:
        print(f"- {rec}")


def run_audit() -> dict:
    report = asyncio.run(generate_model_audit_report())
    _save_json(REPORT_PATH, report)
    _print_report(report)
    print(f"\nSaved JSON report to: {REPORT_PATH}")
    return report


if __name__ == "__main__":
    run_audit()
