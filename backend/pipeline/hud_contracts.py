"""Source contracts for HUD LIHTC and QCT/DDA datasets (phases 1-3)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SourceContract:
    source_family: str
    required_fields: set[str]
    optional_fields: set[str]
    aliases: dict[str, list[str]]


CONTRACTS: dict[str, SourceContract] = {
    "lihtc_property": SourceContract(
        source_family="lihtc_property",
        required_fields={"HUD_ID", "PROJECT", "LATITUDE", "LONGITUDE"},
        optional_fields={
            "N_UNITS", "LI_UNITS", "YR_PIS", "YR_COMP_END", "YR_EXT_END",
            "PROJ_CTY", "PROJ_ST", "FIPS", "ADDRESS", "ZIP", "TRACT",
        },
        aliases={
            "HUD_ID": ["hud_id"],
            "PROJECT": ["project_name", "PROJECT_NAME"],
            "LATITUDE": ["latitude", "lat"],
            "LONGITUDE": ["longitude", "lon", "lng"],
            "N_UNITS": ["total_units"],
            "LI_UNITS": ["low_income_units"],
            "YR_PIS": ["placed_in_service_year"],
            "YR_COMP_END": ["compliance_end_year"],
            "YR_EXT_END": ["extended_use_end_year"],
            "PROJ_CTY": ["city"],
            "PROJ_ST": ["state"],
            "FIPS": ["county_fips"],
            "ADDRESS": ["street_address", "address"],
            "ZIP": ["zip", "zip_code"],
            "TRACT": ["geoid", "geoid11", "census_tract"],
        },
    ),
    "lihtc_tenant": SourceContract(
        source_family="lihtc_tenant",
        required_fields={"REPORTING_YEAR", "HOUSEHOLD_COUNT"},
        optional_fields={"HUD_ID", "TRACT", "HOUSEHOLD_TYPE", "INCOME_BUCKET", "AVG_HH_INCOME"},
        aliases={
            "REPORTING_YEAR": ["reporting_year", "year"],
            "HOUSEHOLD_COUNT": ["household_count", "households"],
            "HUD_ID": ["hud_id", "project_id"],
            "TRACT": ["TRACT_ID", "geoid", "geoid11"],
            "HOUSEHOLD_TYPE": ["household_type"],
            "INCOME_BUCKET": ["income_bucket", "income_band"],
            "AVG_HH_INCOME": ["avg_hh_income", "average_household_income"],
        },
    ),
    "qct_dda": SourceContract(
        source_family="qct_dda",
        required_fields={"DESIGNATION_YEAR", "DESIGNATION_TYPE"},
        optional_fields={"TRACT", "STATE_FIPS", "COUNTY_FIPS", "AREA_NAME", "WKT"},
        aliases={
            "DESIGNATION_YEAR": ["year", "designation_year"],
            "DESIGNATION_TYPE": ["type", "designation_type"],
            "TRACT": ["geoid", "geoid11", "tract"],
            "STATE_FIPS": ["state", "state_fips"],
            "COUNTY_FIPS": ["county", "county_fips", "fips"],
            "AREA_NAME": ["area_name", "name"],
            "WKT": ["geometry_wkt", "boundary_wkt"],
        },
    ),
}


def resolve_field(row: dict[str, Any], canonical: str, aliases: dict[str, list[str]]) -> Any:
    for key in [canonical, *aliases.get(canonical, [])]:
        if key in row:
            return row[key]
    return None


def validate_columns(source_family: str, columns: set[str]) -> list[str]:
    contract = CONTRACTS[source_family]
    errors: list[str] = []
    for required in contract.required_fields:
        valid_keys = {required, *contract.aliases.get(required, [])}
        if columns.isdisjoint(valid_keys):
            errors.append(f"Missing required field '{required}' for source family '{source_family}'")
    return errors
