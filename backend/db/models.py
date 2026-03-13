"""SQLAlchemy models for the precomputed data layer.

All spatial columns use EPSG:4326 (WGS 84) — standard lat/lon.
"""

from datetime import datetime

from geoalchemy2 import Geometry
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from db.connection import Base


# ---------------------------------------------------------------------------
# Census tract demographics
# ---------------------------------------------------------------------------

class CensusTract(Base):
    """One row per census tract with ACS 5-Year demographic variables."""

    __tablename__ = "census_tracts"

    geoid: Mapped[str] = mapped_column(String(11), primary_key=True)  # e.g. "17031010100"
    state_fips: Mapped[str] = mapped_column(String(2), index=True)
    county_fips: Mapped[str] = mapped_column(String(5), index=True)   # full 5-digit FIPS
    tract_name: Mapped[str | None] = mapped_column(String(120))

    # PostGIS geometries
    centroid: Mapped[None] = mapped_column(Geometry("POINT", srid=4326), nullable=True)
    boundary: Mapped[None] = mapped_column(Geometry("MULTIPOLYGON", srid=4326), nullable=True)
    land_area_sq_mi: Mapped[float | None] = mapped_column(Float)

    # --- Population (B01001) ---
    total_population: Mapped[int | None] = mapped_column(Integer)
    population_under_5: Mapped[int | None] = mapped_column(Integer)
    population_5_17: Mapped[int | None] = mapped_column(Integer)
    population_18_64: Mapped[int | None] = mapped_column(Integer)
    population_65_74: Mapped[int | None] = mapped_column(Integer)
    population_75_plus: Mapped[int | None] = mapped_column(Integer)
    male_total: Mapped[int | None] = mapped_column(Integer)
    female_total: Mapped[int | None] = mapped_column(Integer)

    # --- School enrollment (B14002) ---
    enrolled_nursery_preschool: Mapped[int | None] = mapped_column(Integer)
    enrolled_k_12: Mapped[int | None] = mapped_column(Integer)
    enrolled_private_k_12: Mapped[int | None] = mapped_column(Integer)

    # --- Children (B09001) ---
    population_under_18: Mapped[int | None] = mapped_column(Integer)

    # --- Income (B19001, B19013, B19125) ---
    median_household_income: Mapped[int | None] = mapped_column(Integer)
    median_family_income: Mapped[int | None] = mapped_column(Integer)
    income_bracket_under_10k: Mapped[int | None] = mapped_column(Integer)
    income_bracket_10k_15k: Mapped[int | None] = mapped_column(Integer)
    income_bracket_15k_25k: Mapped[int | None] = mapped_column(Integer)
    income_bracket_25k_35k: Mapped[int | None] = mapped_column(Integer)
    income_bracket_35k_50k: Mapped[int | None] = mapped_column(Integer)
    income_bracket_50k_75k: Mapped[int | None] = mapped_column(Integer)
    income_bracket_75k_100k: Mapped[int | None] = mapped_column(Integer)
    income_bracket_100k_150k: Mapped[int | None] = mapped_column(Integer)
    income_bracket_150k_200k: Mapped[int | None] = mapped_column(Integer)
    income_bracket_200k_plus: Mapped[int | None] = mapped_column(Integer)

    # --- Households & families (B11001, B11003, B25003) ---
    total_households: Mapped[int | None] = mapped_column(Integer)
    family_households: Mapped[int | None] = mapped_column(Integer)
    families_with_own_children: Mapped[int | None] = mapped_column(Integer)
    owner_occupied: Mapped[int | None] = mapped_column(Integer)
    renter_occupied: Mapped[int | None] = mapped_column(Integer)

    # --- Housing burden (B25070) ---
    renters_cost_burdened_30_plus_pct: Mapped[float | None] = mapped_column(Float)

    # --- Poverty (B17001) ---
    population_below_poverty: Mapped[int | None] = mapped_column(Integer)
    seniors_below_poverty: Mapped[int | None] = mapped_column(Integer)

    # --- Seniors living alone ---
    seniors_living_alone: Mapped[int | None] = mapped_column(Integer)

    # --- Margins of error (coefficient of variation) ---
    income_cv: Mapped[float | None] = mapped_column(Float)  # coefficient of variation for median income

    # --- Data vintage ---
    acs_vintage: Mapped[str] = mapped_column(String(10), default="2022")  # e.g. "2022"
    ingested_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_census_tracts_centroid", "centroid", postgresql_using="gist"),
        Index("ix_census_tracts_boundary", "boundary", postgresql_using="gist"),
        Index("ix_census_tracts_state_county", "state_fips", "county_fips"),
    )


# ---------------------------------------------------------------------------
# Historical demographics (for trend computation)
# ---------------------------------------------------------------------------

class CensusTractHistory(Base):
    """Historical demographic snapshots for trend analysis (e.g. ACS 2017)."""

    __tablename__ = "census_tracts_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    geoid: Mapped[str] = mapped_column(String(11), index=True)
    acs_vintage: Mapped[str] = mapped_column(String(10))  # "2017", "2018", etc.

    total_population: Mapped[int | None] = mapped_column(Integer)
    population_5_17: Mapped[int | None] = mapped_column(Integer)
    median_household_income: Mapped[int | None] = mapped_column(Integer)
    families_with_own_children: Mapped[int | None] = mapped_column(Integer)
    population_65_74: Mapped[int | None] = mapped_column(Integer)
    population_75_plus: Mapped[int | None] = mapped_column(Integer)
    total_households: Mapped[int | None] = mapped_column(Integer)

    ingested_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_history_geoid_vintage", "geoid", "acs_vintage", unique=True),
    )


# ---------------------------------------------------------------------------
# Competitor: Private schools (NCES PSS)
# ---------------------------------------------------------------------------

class CompetitorSchoolRecord(Base):
    """Private school from NCES Private School Survey."""

    __tablename__ = "competitors_schools"

    ppin: Mapped[str] = mapped_column(String(20), primary_key=True)  # NCES school ID
    school_name: Mapped[str] = mapped_column(String(200))
    location: Mapped[None] = mapped_column(Geometry("POINT", srid=4326))
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    city: Mapped[str | None] = mapped_column(String(100))
    state: Mapped[str | None] = mapped_column(String(2))
    county_fips: Mapped[str | None] = mapped_column(String(5))

    religious_affiliation_code: Mapped[int | None] = mapped_column(Integer)
    affiliation_label: Mapped[str | None] = mapped_column(String(80))
    is_catholic: Mapped[bool] = mapped_column(Boolean, default=False, index=True)

    enrollment: Mapped[int | None] = mapped_column(Integer)
    coeducation: Mapped[str | None] = mapped_column(String(20))  # "Co-ed", "All Boys", "All Girls"
    grade_level: Mapped[str | None] = mapped_column(String(30))  # "Elementary", "High School", etc.
    typology_code: Mapped[int | None] = mapped_column(Integer)

    competitor_tier: Mapped[str] = mapped_column(String(20), default="moderate")
    tier_weight: Mapped[float] = mapped_column(Float, default=0.4)

    pss_vintage: Mapped[str] = mapped_column(String(10), default="2021-22")
    ingested_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_schools_location", "location", postgresql_using="gist"),
        Index("ix_schools_state", "state"),
    )


# ---------------------------------------------------------------------------
# Competitor: Elder care facilities (CMS + onefact)
# ---------------------------------------------------------------------------

class CompetitorElderCare(Base):
    """Elder care facility from CMS Provider Data or onefact assisted living."""

    __tablename__ = "competitors_elder_care"

    provider_id: Mapped[str] = mapped_column(String(30), primary_key=True)
    facility_name: Mapped[str] = mapped_column(String(200))
    location: Mapped[None] = mapped_column(Geometry("POINT", srid=4326))
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    city: Mapped[str | None] = mapped_column(String(100))
    state: Mapped[str | None] = mapped_column(String(2))
    county_fips: Mapped[str | None] = mapped_column(String(5))

    care_level: Mapped[str | None] = mapped_column(String(30))  # snf, assisted_living, memory_care
    certified_beds: Mapped[int | None] = mapped_column(Integer)
    average_daily_census: Mapped[float | None] = mapped_column(Float)
    occupancy_pct: Mapped[float | None] = mapped_column(Float)
    ownership_type: Mapped[str | None] = mapped_column(String(50))
    overall_rating: Mapped[int | None] = mapped_column(Integer)  # CMS 5-star

    data_source: Mapped[str] = mapped_column(String(30), default="cms")  # "cms" or "onefact"
    ingested_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_elder_care_location", "location", postgresql_using="gist"),
        Index("ix_elder_care_state", "state"),
    )


# ---------------------------------------------------------------------------
# Competitor: Affordable housing (HUD LIHTC)
# ---------------------------------------------------------------------------

class CompetitorHousing(Base):
    """Low-Income Housing Tax Credit project from HUD LIHTC database."""

    __tablename__ = "competitors_housing"

    hud_id: Mapped[str] = mapped_column(String(30), primary_key=True)
    project_name: Mapped[str] = mapped_column(String(200))
    location: Mapped[None] = mapped_column(Geometry("POINT", srid=4326))
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    city: Mapped[str | None] = mapped_column(String(100))
    state: Mapped[str | None] = mapped_column(String(2))
    county_fips: Mapped[str | None] = mapped_column(String(5))

    total_units: Mapped[int | None] = mapped_column(Integer)
    low_income_units: Mapped[int | None] = mapped_column(Integer)
    placed_in_service_year: Mapped[int | None] = mapped_column(Integer)
    compliance_end_year: Mapped[int | None] = mapped_column(Integer)
    extended_use_end_year: Mapped[int | None] = mapped_column(Integer)

    ingested_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_housing_location", "location", postgresql_using="gist"),
        Index("ix_housing_state", "state"),
    )


# ---------------------------------------------------------------------------
# Geographic reference: Dioceses, counties
# ---------------------------------------------------------------------------

class Diocese(Base):
    """Catholic diocese boundary and metadata."""

    __tablename__ = "dioceses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(200), unique=True)
    province: Mapped[str | None] = mapped_column(String(200))
    state: Mapped[str | None] = mapped_column(String(2))
    boundary: Mapped[None] = mapped_column(Geometry("MULTIPOLYGON", srid=4326), nullable=True)

    __table_args__ = (
        Index("ix_diocese_boundary", "boundary", postgresql_using="gist"),
    )


class CountyReference(Base):
    """County boundaries and FIPS lookup."""

    __tablename__ = "counties"

    fips: Mapped[str] = mapped_column(String(5), primary_key=True)
    state_fips: Mapped[str] = mapped_column(String(2), index=True)
    county_name: Mapped[str] = mapped_column(String(100))
    state_name: Mapped[str | None] = mapped_column(String(50))
    boundary: Mapped[None] = mapped_column(Geometry("MULTIPOLYGON", srid=4326), nullable=True)
    centroid: Mapped[None] = mapped_column(Geometry("POINT", srid=4326), nullable=True)

    __table_args__ = (
        Index("ix_county_boundary", "boundary", postgresql_using="gist"),
    )


# ---------------------------------------------------------------------------
# Isochrone cache
# ---------------------------------------------------------------------------

class IsochroneCache(Base):
    """Cached drive-time isochrone polygons."""

    __tablename__ = "isochrone_cache"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    drive_minutes: Mapped[int] = mapped_column(Integer)
    location_point: Mapped[None] = mapped_column(Geometry("POINT", srid=4326))
    polygon: Mapped[None] = mapped_column(Geometry("MULTIPOLYGON", srid=4326))
    polygon_geojson: Mapped[str | None] = mapped_column(Text)  # original GeoJSON for frontend
    effective_radius_miles: Mapped[float | None] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_isochrone_location", "location_point", postgresql_using="gist"),
        Index("ix_isochrone_minutes", "drive_minutes"),
    )


# ---------------------------------------------------------------------------
# Precomputed feasibility scores (per tract, per ministry type)
# ---------------------------------------------------------------------------

class TractFeasibilityScore(Base):
    """Precomputed feasibility score for a tract + ministry type combo."""

    __tablename__ = "tract_feasibility_scores"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    geoid: Mapped[str] = mapped_column(String(11), index=True)
    ministry_type: Mapped[str] = mapped_column(String(20))  # schools, housing, elder_care
    weighting_profile: Mapped[str] = mapped_column(String(40), default="standard_baseline")

    overall_score: Mapped[int | None] = mapped_column(Integer)
    market_size_score: Mapped[int | None] = mapped_column(Integer)
    income_score: Mapped[int | None] = mapped_column(Integer)
    competition_score: Mapped[int | None] = mapped_column(Integer)
    family_density_score: Mapped[int | None] = mapped_column(Integer)
    occupancy_score: Mapped[int | None] = mapped_column(Integer)  # elder care only

    scenario_conservative: Mapped[int | None] = mapped_column(Integer)
    scenario_optimistic: Mapped[int | None] = mapped_column(Integer)

    percentile_state: Mapped[float | None] = mapped_column(Float)
    percentile_national: Mapped[float | None] = mapped_column(Float)

    factor_details: Mapped[dict | None] = mapped_column(JSONB)  # full breakdown for drill-down
    computed_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    acs_vintage: Mapped[str] = mapped_column(String(10), default="2022")

    __table_args__ = (
        Index("ix_tract_scores_geoid_ministry", "geoid", "ministry_type", "weighting_profile", unique=True),
        Index("ix_tract_scores_overall", "ministry_type", "overall_score"),
    )


# ---------------------------------------------------------------------------
# Analysis history
# ---------------------------------------------------------------------------

class AnalysisRecord(Base):
    """Persisted record of every analysis run."""

    __tablename__ = "analysis_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    school_name: Mapped[str] = mapped_column(String(200))
    address: Mapped[str] = mapped_column(String(300))
    ministry_type: Mapped[str] = mapped_column(String(20))
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    location_point: Mapped[None] = mapped_column(Geometry("POINT", srid=4326), nullable=True)

    request_params: Mapped[dict | None] = mapped_column(JSONB)
    result_summary: Mapped[dict | None] = mapped_column(JSONB)
    overall_score: Mapped[int | None] = mapped_column(Integer)

    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_analysis_location", "location_point", postgresql_using="gist"),
        Index("ix_analysis_ministry_score", "ministry_type", "overall_score"),
    )


class PortfolioWorkspaceRecord(Base):
    """Persisted portfolio workspace for phase 2 consulting workflows."""

    __tablename__ = "portfolio_workspaces"

    workspace_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    payload: Mapped[dict] = mapped_column(JSONB)
    version: Mapped[int] = mapped_column(Integer, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


# ---------------------------------------------------------------------------
# Data pipeline run log
# ---------------------------------------------------------------------------

class PipelineRun(Base):
    """Audit log for data refresh pipeline executions."""

    __tablename__ = "pipeline_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pipeline_name: Mapped[str] = mapped_column(String(80))  # e.g. "census_acs", "nces_pss"
    status: Mapped[str] = mapped_column(String(20))  # "running", "success", "failed"
    started_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    records_processed: Mapped[int | None] = mapped_column(Integer)
    records_inserted: Mapped[int | None] = mapped_column(Integer)
    records_updated: Mapped[int | None] = mapped_column(Integer)
    error_message: Mapped[str | None] = mapped_column(Text)
    metadata_json: Mapped[dict | None] = mapped_column(JSONB)


# ---------------------------------------------------------------------------
# HUD dataset foundation (phases 1-3)
# ---------------------------------------------------------------------------

class HudIngestRun(Base):
    """Ingestion lifecycle record for a HUD source family + source year."""

    __tablename__ = "hud_ingest_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_family: Mapped[str] = mapped_column(String(40), index=True)  # lihtc_property/lihtc_tenant/qct_dda
    source_identifier: Mapped[str] = mapped_column(String(120))
    dataset_year: Mapped[int] = mapped_column(Integer, index=True)
    source_version: Mapped[str | None] = mapped_column(String(80))
    status: Mapped[str] = mapped_column(String(20), index=True, default="running")
    started_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    checksum_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    snapshot_root: Mapped[str | None] = mapped_column(String(400), nullable=True)
    records_seen: Mapped[int | None] = mapped_column(Integer)
    records_loaded: Mapped[int | None] = mapped_column(Integer)
    error_message: Mapped[str | None] = mapped_column(Text)
    metadata_json: Mapped[dict | None] = mapped_column(JSONB)

    __table_args__ = (
        Index("ix_hud_ingest_runs_family_status", "source_family", "status"),
    )


class HudRawSnapshot(Base):
    """Immutable snapshot metadata for raw HUD source artifacts."""

    __tablename__ = "hud_raw_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ingest_run_id: Mapped[int] = mapped_column(Integer, index=True)
    source_family: Mapped[str] = mapped_column(String(40), index=True)
    dataset_year: Mapped[int] = mapped_column(Integer, index=True)
    source_version: Mapped[str | None] = mapped_column(String(80))
    source_uri: Mapped[str] = mapped_column(String(400))
    snapshot_path: Mapped[str] = mapped_column(String(500), unique=True)
    file_name: Mapped[str] = mapped_column(String(220))
    file_format: Mapped[str] = mapped_column(String(20))
    size_bytes: Mapped[int] = mapped_column(Integer)
    checksum_sha256: Mapped[str] = mapped_column(String(64), index=True)
    schema_version: Mapped[str | None] = mapped_column(String(60))
    validated: Mapped[bool] = mapped_column(Boolean, default=False)
    validation_errors: Mapped[dict | None] = mapped_column(JSONB)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class HudLihtcProperty(Base):
    """Normalized HUD LIHTC property-level records."""

    __tablename__ = "hud_lihtc_property"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hud_id: Mapped[str] = mapped_column(String(30), index=True)
    dataset_year: Mapped[int] = mapped_column(Integer, index=True)
    source_version: Mapped[str | None] = mapped_column(String(80))
    source_snapshot_id: Mapped[int] = mapped_column(Integer, index=True)
    project_name: Mapped[str] = mapped_column(String(220))
    street_address: Mapped[str | None] = mapped_column(String(220))
    city: Mapped[str | None] = mapped_column(String(100))
    state: Mapped[str | None] = mapped_column(String(2), index=True)
    zip_code: Mapped[str | None] = mapped_column(String(10))
    county_fips: Mapped[str | None] = mapped_column(String(5), index=True)
    geoid11: Mapped[str | None] = mapped_column(String(11), index=True)
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
    location: Mapped[None] = mapped_column(Geometry("POINT", srid=4326))
    total_units: Mapped[int | None] = mapped_column(Integer)
    low_income_units: Mapped[int | None] = mapped_column(Integer)
    placed_in_service_year: Mapped[int | None] = mapped_column(Integer)
    compliance_end_year: Mapped[int | None] = mapped_column(Integer)
    extended_use_end_year: Mapped[int | None] = mapped_column(Integer)
    normalized_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("hud_id", "dataset_year", name="uq_hud_lihtc_property_hudid_year"),
        Index("ix_hud_lihtc_property_location", "location", postgresql_using="gist"),
    )


class HudLihtcTenant(Base):
    """Normalized HUD LIHTC tenant-level records (or annual aggregates where supplied)."""

    __tablename__ = "hud_lihtc_tenant"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dataset_year: Mapped[int] = mapped_column(Integer, index=True)
    reporting_year: Mapped[int] = mapped_column(Integer, index=True)
    source_version: Mapped[str | None] = mapped_column(String(80))
    source_snapshot_id: Mapped[int] = mapped_column(Integer, index=True)
    hud_id: Mapped[str | None] = mapped_column(String(30), index=True)
    geoid11: Mapped[str | None] = mapped_column(String(11), index=True)
    household_type: Mapped[str | None] = mapped_column(String(80))
    income_bucket: Mapped[str | None] = mapped_column(String(80))
    household_count: Mapped[int | None] = mapped_column(Integer)
    average_household_income: Mapped[float | None] = mapped_column(Float)
    sheet_name: Mapped[str | None] = mapped_column(String(80), index=True)
    table_id: Mapped[str | None] = mapped_column(String(80), index=True)
    geography: Mapped[str | None] = mapped_column(String(180))
    geography_type: Mapped[str | None] = mapped_column(String(60))
    row_label: Mapped[str | None] = mapped_column(String(220))
    column_label: Mapped[str | None] = mapped_column(String(220))
    value_raw: Mapped[str | None] = mapped_column(String(120))
    value_numeric: Mapped[float | None] = mapped_column(Float)
    value_text: Mapped[str | None] = mapped_column(String(220))
    unit_of_measure: Mapped[str | None] = mapped_column(String(40))
    notes: Mapped[str | None] = mapped_column(String(500))
    join_method: Mapped[str] = mapped_column(String(30), default="unmatched")
    join_confidence: Mapped[float] = mapped_column(Float, default=0.0)
    normalized_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_hud_lihtc_tenant_hudid_year", "hud_id", "dataset_year"),
        Index("ix_hud_lihtc_tenant_geoid_year", "geoid11", "reporting_year"),
    )


class HudQctDdaDesignation(Base):
    """Normalized annual HUD QCT/DDA tract designations."""

    __tablename__ = "hud_qct_dda_designations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    designation_year: Mapped[int] = mapped_column(Integer, index=True)
    designation_type: Mapped[str] = mapped_column(String(10), index=True)  # QCT/DDA
    source_snapshot_id: Mapped[int] = mapped_column(Integer, index=True)
    source_version: Mapped[str | None] = mapped_column(String(80))
    geoid11: Mapped[str | None] = mapped_column(String(11), index=True)
    state_fips: Mapped[str | None] = mapped_column(String(2), index=True)
    county_fips: Mapped[str | None] = mapped_column(String(5), index=True)
    area_name: Mapped[str | None] = mapped_column(String(180))
    boundary: Mapped[None] = mapped_column(Geometry("MULTIPOLYGON", srid=4326), nullable=True)
    normalized_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint(
            "designation_year",
            "designation_type",
            "geoid11",
            name="uq_hud_qct_dda_designation_year_type_geoid",
        ),
        Index("ix_hud_qct_dda_boundary", "boundary", postgresql_using="gist"),
    )


class HudPropertyDesignationMatch(Base):
    """Deterministic property-to-designation join output with method + confidence."""

    __tablename__ = "hud_property_designation_matches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    property_row_id: Mapped[int] = mapped_column(Integer, index=True)
    designation_row_id: Mapped[int] = mapped_column(Integer, index=True)
    designation_year: Mapped[int] = mapped_column(Integer, index=True)
    join_method: Mapped[str] = mapped_column(String(30))
    join_confidence: Mapped[float] = mapped_column(Float)
    matched_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("property_row_id", "designation_row_id", name="uq_hud_property_designation_pair"),
    )
