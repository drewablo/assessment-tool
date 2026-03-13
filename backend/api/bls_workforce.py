"""
BLS Quarterly Census of Employment and Wages (QCEW) integration.

Provides workforce availability indicators for elder care and education
ministry feasibility analysis.

Uses the BLS QCEW Open Data API to fetch county-level employment data
for relevant NAICS industry codes:
  - 6231: Nursing Care Facilities (Skilled Nursing)
  - 6232: Residential Intellectual and Developmental Disability,
          Mental Health, and Substance Abuse Facilities
  - 6233: Continuing Care Retirement Communities and Assisted Living
  - 6216: Home Health Care Services
  - 6111: Elementary and Secondary Schools

The workforce availability index considers:
  - Total employment in elder care industries per 1,000 seniors (65+)
  - Employment concentration (location quotient) relative to national avg
  - Establishment density (number of employers competing for workers)

Data source: https://data.bls.gov/cew/data/api/
"""

import logging
from typing import Optional, Tuple

import httpx

from utils import piecewise_linear

logger = logging.getLogger(__name__)

BLS_QCEW_BASE_URL = "https://data.bls.gov/cew/data/api"
_BLS_TIMEOUT = 15.0

# NAICS codes for elder care workforce
ELDER_CARE_NAICS = ["6231", "6232", "6233", "6216"]
EDUCATION_NAICS = ["6111"]

# Workforce scoring segments
# Elder care workers per 1,000 seniors (65+) — higher is better for facility ops
# National average is roughly 25-35 per 1,000 seniors
_ELDER_CARE_WORKERS_PER_1K_SENIORS = [
    (0.0, 5),
    (5.0, 15),
    (12.0, 28),
    (20.0, 42),
    (30.0, 58),    # ~national average
    (40.0, 72),
    (55.0, 84),
    (75.0, 94),
]

# Establishment density: number of elder care employers in the county
# More employers means more competition for workers (harder to staff)
# but also indicates an established labor pool
_ESTABLISHMENT_DENSITY_SEGMENTS = [
    (0, 10),       # No employers → no labor pool
    (2, 22),
    (5, 38),
    (10, 55),      # Moderate pool
    (20, 70),
    (40, 82),
    (80, 90),      # Deep labor pool
]

# Location quotient: elder care employment concentration vs. national avg
# LQ > 1.0 means more concentrated than national average → better workforce
_LOCATION_QUOTIENT_SEGMENTS = [
    (0.0, 10),
    (0.3, 22),
    (0.6, 38),
    (0.8, 48),
    (1.0, 58),     # National average
    (1.3, 72),
    (1.6, 84),
    (2.0, 92),
]


async def fetch_county_qcew(
    county_fips: str,
    year: str = "2023",
    quarter: str = "a",  # "a" = annual average
) -> Optional[dict]:
    """
    Fetch QCEW data for a county from the BLS API.

    Args:
        county_fips: 5-digit FIPS code (state + county)
        year: Data year (default: 2023)
        quarter: Quarter or "a" for annual average

    Returns:
        Dict with parsed employment data, or None if API fails.
    """
    # BLS QCEW API endpoint for county-level data
    url = f"{BLS_QCEW_BASE_URL}/{year}/{quarter}/area/{county_fips}.csv"

    async with httpx.AsyncClient(timeout=_BLS_TIMEOUT) as client:
        try:
            response = await client.get(url)
            if response.status_code == 404:
                logger.info("No QCEW data for county %s, year %s", county_fips, year)
                return None
            response.raise_for_status()
            return _parse_qcew_csv(response.text)
        except httpx.TimeoutException:
            logger.warning("BLS QCEW timeout for county %s", county_fips)
            return None
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logger.error("BLS QCEW error for county %s: %s", county_fips, e)
            return None


def _parse_qcew_csv(csv_text: str) -> dict:
    """
    Parse the BLS QCEW CSV response into structured employment data.

    The CSV has columns:
    area_fips, own_code, industry_code, agglvl_code, size_code,
    year, qtr, disclosure_code, area_title, own_title, industry_title,
    agglvl_title, size_title, annual_avg_estabs, annual_avg_emplvl,
    total_annual_wages, taxable_annual_wages, annual_contributions,
    annual_avg_wkly_wage, avg_annual_pay, ...
    """
    lines = csv_text.strip().split("\n")
    if len(lines) < 2:
        return {}

    header = [h.strip().strip('"') for h in lines[0].split(",")]
    result = {
        "elder_care_employment": 0,
        "elder_care_establishments": 0,
        "elder_care_avg_weekly_wage": 0.0,
        "education_employment": 0,
        "education_establishments": 0,
        "total_private_employment": 0,
        "total_private_establishments": 0,
        "naics_details": [],
    }

    for line in lines[1:]:
        fields = [f.strip().strip('"') for f in line.split(",")]
        if len(fields) < len(header):
            continue

        row = dict(zip(header, fields))

        own_code = row.get("own_code", "")
        industry_code = row.get("industry_code", "").strip()
        disclosure = row.get("disclosure_code", "").strip()

        # Skip disclosed/suppressed data
        if disclosure and disclosure != "":
            continue

        # Only private ownership (own_code=5)
        if own_code != "5":
            continue

        try:
            employment = int(row.get("annual_avg_emplvl", 0) or 0)
            establishments = int(row.get("annual_avg_estabs", 0) or 0)
            avg_weekly_wage = float(row.get("annual_avg_wkly_wage", 0) or 0)
        except (ValueError, TypeError):
            continue

        # Total private sector (NAICS 10 = all industries)
        if industry_code == "10":
            result["total_private_employment"] = employment
            result["total_private_establishments"] = establishments

        # Elder care NAICS codes
        if industry_code in ELDER_CARE_NAICS:
            result["elder_care_employment"] += employment
            result["elder_care_establishments"] += establishments
            if avg_weekly_wage > 0:
                result["elder_care_avg_weekly_wage"] = max(
                    result["elder_care_avg_weekly_wage"], avg_weekly_wage
                )
            result["naics_details"].append({
                "naics": industry_code,
                "title": row.get("industry_title", ""),
                "employment": employment,
                "establishments": establishments,
                "avg_weekly_wage": avg_weekly_wage,
            })

        # Education NAICS
        if industry_code in EDUCATION_NAICS:
            result["education_employment"] += employment
            result["education_establishments"] += establishments

    return result


def score_workforce_availability(
    qcew_data: Optional[dict],
    seniors_65_plus: int,
    national_elder_care_employment_rate: float = 30.0,
) -> Tuple[float, dict]:
    """
    Compute the workforce availability index (0-100) for elder care.

    Combines:
    - Workers per 1,000 seniors (40%): measures labor pool depth relative to demand
    - Establishment density (30%): measures breadth of employer pool (training pipeline)
    - Location quotient (30%): measures regional specialization in elder care

    Args:
        qcew_data: Parsed QCEW data from fetch_county_qcew, or None
        seniors_65_plus: Senior population in the catchment area
        national_elder_care_employment_rate: National avg elder care workers per 1k seniors

    Returns:
        (score, details_dict) where details_dict contains sub-scores and raw values.
    """
    if qcew_data is None or seniors_65_plus <= 0:
        return 50.0, {
            "available": False,
            "note": "Workforce data unavailable — BLS QCEW data could not be retrieved",
        }

    elder_employment = qcew_data.get("elder_care_employment", 0)
    elder_establishments = qcew_data.get("elder_care_establishments", 0)
    total_private_employment = qcew_data.get("total_private_employment", 0)
    avg_weekly_wage = qcew_data.get("elder_care_avg_weekly_wage", 0)

    # Workers per 1,000 seniors
    workers_per_1k = (elder_employment / seniors_65_plus) * 1000
    workers_score = piecewise_linear(workers_per_1k, _ELDER_CARE_WORKERS_PER_1K_SENIORS)

    # Establishment density
    density_score = piecewise_linear(elder_establishments, _ESTABLISHMENT_DENSITY_SEGMENTS)

    # Location quotient (elder care share vs. national share)
    if total_private_employment > 0 and national_elder_care_employment_rate > 0:
        local_rate = elder_employment / total_private_employment
        # National elder care is about 3.5% of private employment
        national_rate = 0.035
        lq = local_rate / national_rate if national_rate > 0 else 1.0
    else:
        lq = 1.0
    lq_score = piecewise_linear(lq, _LOCATION_QUOTIENT_SEGMENTS)

    # Composite: workers 40%, density 30%, LQ 30%
    composite = round(workers_score * 0.40 + density_score * 0.30 + lq_score * 0.30)

    details = {
        "available": True,
        "workforce_score": composite,
        "workers_per_1k_seniors": round(workers_per_1k, 1),
        "workers_score": round(workers_score),
        "elder_care_employment": elder_employment,
        "elder_care_establishments": elder_establishments,
        "establishment_density_score": round(density_score),
        "location_quotient": round(lq, 2),
        "location_quotient_score": round(lq_score),
        "avg_weekly_wage": round(avg_weekly_wage),
        "naics_details": qcew_data.get("naics_details", []),
        "note": (
            f"Based on BLS QCEW data: {elder_employment:,} elder care workers "
            f"across {elder_establishments} establishments in this county. "
            f"{workers_per_1k:.1f} workers per 1,000 seniors (65+)."
        ),
    }

    return float(composite), details
