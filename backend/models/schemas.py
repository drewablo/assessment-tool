from pydantic import BaseModel, ConfigDict, Field
from typing import Dict, List, Literal, Optional


class Stage2FinancialYear(BaseModel):
    year: int
    student_count: Optional[float] = None
    tuition_revenue: Optional[float] = None
    total_revenue: Optional[float] = None
    fundraising_income: Optional[float] = None
    total_income: Optional[float] = None
    payroll_expense: Optional[float] = None
    total_expenses: Optional[float] = None
    surplus_deficit: Optional[float] = None
    investment_transfers: Optional[float] = None
    one_time_income: Optional[float] = None


class SchoolAuditFinancialYear(BaseModel):
    fiscal_year: Optional[int] = None
    year_label: Optional[str] = None
    year_label_needs_confirmation: bool = False
    tuition_revenue: Optional[float] = None
    tuition_aid: Optional[float] = None
    other_revenue: Optional[float] = None
    total_expenses: Optional[float] = None
    non_operating_revenue: Optional[float] = None
    total_assets: Optional[float] = None
    enrollment: Optional[float] = None
    source_file: Optional[str] = None
    source_audit_index: Optional[int] = None
    missing_fields: List[str] = Field(default_factory=list)


class SchoolAuditExtractionResponse(BaseModel):
    years: List[SchoolAuditFinancialYear] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class SchoolAuditExtractionRequest(BaseModel):
    years: List[SchoolAuditFinancialYear] = Field(default_factory=list)
    user_confirmed: bool = False


class HousingStage2Inputs(BaseModel):
    """Operating KPIs for a Housing Stage 2 institutional-economics score."""
    occupancy_rate: Optional[float] = None           # 0.0–1.0, e.g. 0.92
    operating_cost_per_unit: Optional[float] = None  # $/unit/year
    dscr: Optional[float] = None                     # debt-service coverage ratio
    subsidy_dependency: Optional[float] = None       # fraction of units requiring subsidy
    operating_reserve_months: Optional[float] = None # months of operating reserves
    capital_reserve_per_unit: Optional[float] = None # $/unit/year capital reserve


class ElderCareStage2Inputs(BaseModel):
    """Operating KPIs for an Elder Care Stage 2 institutional-economics score."""
    occupancy_rate: Optional[float] = None                    # 0.0–1.0, e.g. 0.88
    operating_cost_per_bed: Optional[float] = None            # $/bed/year
    staffing_hours_per_resident_day: Optional[float] = None   # total HPRD (RN+CNA)
    payer_mix_private_pay: Optional[float] = None             # fraction 0.0–1.0
    payer_mix_medicaid: Optional[float] = None                # fraction 0.0–1.0
    days_cash_on_hand: Optional[float] = None                 # days


class Stage2Inputs(BaseModel):
    historical_financials: Optional[List[Stage2FinancialYear]] = None
    school_audit_financials: Optional[List[SchoolAuditFinancialYear]] = None
    school_stage2_confirmed: bool = False
    housing_financials: Optional[HousingStage2Inputs] = None
    elder_care_financials: Optional[ElderCareStage2Inputs] = None


class FacilityProfile(BaseModel):
    building_square_footage: Optional[float] = Field(default=None, ge=0)
    accessibility_constraints: List[str] = Field(default_factory=list)
    current_layout_notes: Optional[str] = None
    deferred_maintenance_estimate: Optional[float] = Field(default=None, ge=0)
    zoning_use_constraints: List[str] = Field(default_factory=list)
    sponsor_operator_capacity: Literal["low", "medium", "high"] = "medium"


class PartnerPathAssessment(BaseModel):
    mission_alignment_score: int = Field(default=0, ge=0, le=100)
    governance_model_options: List[str] = Field(default_factory=list)
    risk_transfer_profile: Literal["low", "moderate", "high"] = "moderate"
    partnership_readiness_checklist: List[str] = Field(default_factory=list)


class AnalysisRequest(BaseModel):
    school_name: str
    address: str
    ministry_type: Literal["schools", "housing", "elder_care"] = "schools"
    mission_mode: bool = False
    drive_minutes: int = Field(default=20, ge=5, le=60)
    geography_mode: Literal["catchment", "radius"] = "catchment"
    gender: Literal["coed", "boys", "girls"] = "coed"
    grade_level: Literal["k5", "k8", "high_school", "k12"] = "k12"
    weighting_profile: Literal["standard_baseline", "affordability_sensitive", "demand_primacy"] = "standard_baseline"
    market_context: Literal["urban", "suburban", "rural"] = "suburban"
    care_level: Literal["all", "snf", "assisted_living", "memory_care"] = "all"
    housing_target_population: Literal["senior_only", "all_ages"] = "all_ages"
    min_mds_overall_rating: Optional[int] = Field(default=None, ge=1, le=5)
    stage2_inputs: Optional[Stage2Inputs] = None
    facility_profile: Optional[FacilityProfile] = None
    run_mode: Optional[Literal["live_only", "db_with_fallback", "db_strict"]] = None


class CompareAnalysisRequest(BaseModel):
    school_name: str
    address: str
    ministry_types: List[Literal["schools", "housing", "elder_care"]] = Field(
        default_factory=lambda: ["schools", "housing", "elder_care"]
    )
    mission_mode: bool = False
    drive_minutes: int = Field(default=20, ge=5, le=60)
    geography_mode: Literal["catchment", "radius"] = "catchment"
    gender: Literal["coed", "boys", "girls"] = "coed"
    grade_level: Literal["k5", "k8", "high_school", "k12"] = "k12"
    weighting_profile: Literal["standard_baseline", "affordability_sensitive", "demand_primacy"] = "standard_baseline"
    market_context: Literal["urban", "suburban", "rural"] = "suburban"
    care_level: Literal["all", "snf", "assisted_living", "memory_care"] = "all"
    housing_target_population: Literal["senior_only", "all_ages"] = "all_ages"
    min_mds_overall_rating: Optional[int] = Field(default=None, ge=1, le=5)
    stage2_inputs: Optional[Stage2Inputs] = None
    facility_profile: Optional[FacilityProfile] = None


class CompareAnalysisSummary(BaseModel):
    ministry_type: Literal["schools", "housing", "elder_care"]
    overall_score: int
    scenario_conservative: int
    scenario_optimistic: int
    recommendation: str
    recommendation_detail: str
    recommended_pathway: Literal["continue", "transform", "partner", "close"]
    pathway_confidence: Literal["high", "medium", "low"] = "medium"
    fit_band: Literal["high", "medium", "low"] = "medium"
    capital_intensity: Literal["low", "medium", "high"] = "medium"
    regulatory_complexity: Literal["low", "medium", "high"] = "medium"
    operator_dependency: Literal["none", "optional", "required"] = "optional"
    time_to_launch_months_estimate: int = 18


class CompareAnalysisResponse(BaseModel):
    school_name: str
    analysis_address: str
    compared_ministry_types: List[Literal["schools", "housing", "elder_care"]]
    results: List[CompareAnalysisSummary] = Field(default_factory=list)


class Stage2Component(BaseModel):
    key: str
    label: str
    weight: int
    score: Optional[int] = None


class Stage2Score(BaseModel):
    available: bool = False
    score: Optional[int] = None
    schema_version: str = "v2"
    formula_version: str = "stage2-v2"
    computed_at_utc: Optional[str] = None
    readiness: Literal["not_ready", "partial", "ready"] = "not_ready"
    required_inputs: List[str] = Field(default_factory=list)
    provided_inputs: List[str] = Field(default_factory=list)
    missing_inputs: List[str] = Field(default_factory=list)
    components: List[Stage2Component] = Field(default_factory=list)
    note: str = "Stage 2 institutional economics score is not yet calculated in this release."


class SubIndicator(BaseModel):
    key: str
    label: str
    score: int  # 0-100
    weight: int  # Percentage weight within parent factor
    description: str = ""


class MetricScore(BaseModel):
    score: int  # 0-100
    label: str
    description: str
    weight: int  # Percentage weight in overall score
    rating: str  # "strong", "moderate", "weak", "poor"
    sub_indicators: List[SubIndicator] = Field(default_factory=list)


class BenchmarkPercentiles(BaseModel):
    percentile_state: Optional[float] = None       # 0-100
    percentile_national: Optional[float] = None     # 0-100
    percentile_msa: Optional[float] = None          # 0-100
    state_name: Optional[str] = None
    msa_name: Optional[str] = None
    sample_size_state: Optional[int] = None
    sample_size_national: Optional[int] = None
    sample_size_msa: Optional[int] = None
    comparable_markets: List[Dict] = Field(default_factory=list)  # top-N similar tracts


class HierarchicalScore(BaseModel):
    """Hierarchical composite score breakdown per Section 4.2 of blueprint."""
    market_opportunity: Optional[MetricScore] = None      # 45%
    competitive_position: Optional[MetricScore] = None     # 30%
    community_fit: Optional[MetricScore] = None            # 15%
    sustainability_risk: Optional[MetricScore] = None      # 10%


class FeasibilityScore(BaseModel):
    overall: int
    scenario_conservative: int = 0
    scenario_optimistic: int = 0
    weighting_profile: Literal["standard_baseline", "affordability_sensitive", "demand_primacy"] = "standard_baseline"
    market_size: MetricScore
    income: MetricScore
    competition: MetricScore
    family_density: MetricScore
    occupancy: Optional[MetricScore] = None
    workforce: Optional[MetricScore] = None
    stage2: Optional[Stage2Score] = None
    benchmarks: Optional[BenchmarkPercentiles] = None
    hierarchical: Optional[HierarchicalScore] = None


class DemographicData(BaseModel):
    total_population: Optional[int] = None
    population_under_18: Optional[int] = None
    school_age_population: Optional[int] = None  # scoped to grade level and gender (schools module)
    estimated_catholic_school_age: Optional[int] = None
    median_household_income: Optional[int] = None
    total_households: Optional[int] = None
    families_with_children: Optional[int] = None
    owner_occupied_pct: Optional[float] = None
    estimated_catholic_pct: Optional[float] = None
    population_under_5: Optional[int] = None
    pipeline_ratio: Optional[float] = None              # under-5 / school-age (5-17) ratio
    pipeline_score: Optional[int] = None                # 0-100 kindergarten pipeline score
    private_enrollment_rate_pct: Optional[float] = None  # % of K-12 enrolled in private schools
    private_enrollment_score: Optional[int] = None       # 0-100 private enrollment demand score
    data_geography: str = "county"  # level of Census data used
    data_confidence: Optional[str] = None  # "high", "medium", or "low"
    ministry_target_population: Optional[int] = None  # module-specific target (cost-burdened HH for housing, vulnerable seniors for elder care)
    seniors_65_plus: Optional[int] = None
    seniors_75_plus: Optional[int] = None
    seniors_living_alone: Optional[int] = None
    seniors_below_200pct_poverty: Optional[int] = None
    seniors_projected_5yr: Optional[int] = None
    seniors_projected_10yr: Optional[int] = None
    elder_care_weighted_competitor_beds: Optional[float] = None
    elder_care_bed_saturation_ratio: Optional[float] = None
    # Housing-specific demographic detail
    cost_burdened_renter_households: Optional[int] = None
    renter_households: Optional[int] = None
    hud_eligible_households: Optional[int] = None  # households below 60% AMI (estimated from B19001)
    hud_tenant_households: Optional[int] = None
    qct_designated_projects: Optional[int] = None
    dda_designated_projects: Optional[int] = None
    # Income-first addressable market (propensity-weighted enrollment-addressable families)
    total_addressable_market: Optional[int] = None
    reference_enrollment: Optional[int] = None           # NCEA modal enrollment for school type
    market_depth_ratio: Optional[float] = None           # total_addressable_market / reference_enrollment
    income_qualified_base: Optional[int] = None          # Families qualified by income propensity
    catholic_boost_contribution: Optional[int] = None    # Additional families from Catholic affiliation boost


class CompetitorSchool(BaseModel):
    name: str
    lat: float
    lon: float
    distance_miles: float
    affiliation: str
    is_catholic: bool
    city: Optional[str] = None
    state: Optional[str] = None
    street_address: Optional[str] = None
    zip_code: Optional[str] = None
    enrollment: Optional[int] = None
    gender: str = "Co-ed"          # "All Boys", "All Girls", "Co-ed", "Unknown"
    grade_level: str = "Unknown"   # "Elementary", "Middle School", "High School", "K-12", "Unknown"
    competitor_tier: str = "moderate"  # "direct", "strong", "moderate", "weak"
    tier_weight: float = 0.4          # 1.0, 0.7, 0.4, 0.15
    occupancy_pct: Optional[float] = None
    mds_overall_rating: Optional[int] = None  # CMS 5-star overall rating (1-5), elder care only
    # HUD Section 202 detail fields
    total_units: Optional[int] = None
    client_group_name: Optional[str] = None
    property_category: Optional[str] = None
    primary_financing_type: Optional[str] = None
    phone_number: Optional[str] = None
    reac_inspection_score: Optional[int] = None


class DemographicTrend(BaseModel):
    school_age_pop_pct: Optional[float] = None   # % change in school-age pop, 2017→2022
    income_real_pct: Optional[float] = None      # inflation-adjusted % change in median income
    families_pct: Optional[float] = None         # % change in family households with children
    trend_label: str = "Unknown"                 # "Growing" | "Stable" | "Declining" | "Mixed"
    period: str = "ACS 2017 → 2022 (county-level)"


class DirectionSegment(BaseModel):
    school_age_pop: int = 0
    income_qualified_pop: int = 0
    catholic_qualified_pop: int = 0  # income_qualified_pop × local Catholic % estimate
    pipeline_ratio: Optional[float] = None
    growth_signal: Optional[str] = None  # "Growing" | "Stable" | "Declining"
    # Elder care directional fields
    seniors_65_plus: Optional[int] = None
    seniors_75_plus: Optional[int] = None
    seniors_living_alone: Optional[int] = None
    seniors_below_poverty: Optional[int] = None
    isolation_ratio: Optional[float] = None  # seniors_living_alone / seniors_65_plus
    # Housing directional fields
    cost_burdened_renters: Optional[int] = None
    renter_households: Optional[int] = None
    burden_ratio: Optional[float] = None  # cost_burdened / renter_households


class PopulationGravityMap(BaseModel):
    by_direction: Dict[str, DirectionSegment] = Field(default_factory=dict)
    dominant_direction: Optional[str] = None
    gravity_weighted: bool = False


class ForecastPoint(BaseModel):
    years_out: int
    projected_enrollment: int


class EnrollmentForecast(BaseModel):
    baseline: List[ForecastPoint] = Field(default_factory=list)
    optimistic: List[ForecastPoint] = Field(default_factory=list)
    conservative: List[ForecastPoint] = Field(default_factory=list)
    minimum_viable_enrollment: int = 100
    estimated_cliff_year: Optional[int] = None
    decline_risk: Literal["low", "moderate", "high"] = "low"




class DataFreshnessSource(BaseModel):
    source_key: str
    source_label: str
    last_refreshed_utc: Optional[str] = None
    freshness_hours: Optional[float] = None
    status: Literal["fresh", "aging", "stale", "unknown"] = "unknown"
    notes: Optional[str] = None


class DataFreshnessMetadata(BaseModel):
    mode: Literal["live", "db_precomputed"] = "live"
    generated_at_utc: str
    sources: List[DataFreshnessSource] = Field(default_factory=list)


class BenchmarkNarrative(BaseModel):
    peer_cohort: str
    in_state_percentile: Optional[float] = None
    national_percentile: Optional[float] = None
    nearest_comparable_markets: List[str] = Field(default_factory=list)
    narrative_summary: str


class BoardActionRoadmap(BaseModel):
    months_12: List[str] = Field(default_factory=list)
    months_24: List[str] = Field(default_factory=list)
    months_36: List[str] = Field(default_factory=list)


class BoardReportPack(BaseModel):
    executive_summary: str
    key_risks: List[str] = Field(default_factory=list)
    strategic_options: List[str] = Field(default_factory=list)
    immediate_next_actions: List[str] = Field(default_factory=list)
    action_roadmap: BoardActionRoadmap
    methodology_assumptions: List[str] = Field(default_factory=list)
    confidence_notes: List[str] = Field(default_factory=list)


class DataDependencyStatus(BaseModel):
    dataset: str
    required: bool
    baseline_blocking: bool
    affects_confidence: bool
    export_blocking_in_strict: bool
    available: bool
    row_count: int = 0
    note: Optional[str] = None


class SectionExplanation(BaseModel):
    section: str
    inputs_used: List[str] = Field(default_factory=list)
    inputs_missing: List[str] = Field(default_factory=list)
    fallback_used: List[str] = Field(default_factory=list)
    confidence_impact: Literal["none", "low", "medium", "high"] = "none"


class ExportReadiness(BaseModel):
    ready: bool = False
    status: Literal["ready", "warning", "blocked"] = "warning"
    reasons: List[str] = Field(default_factory=list)


class ConfidenceSummary(BaseModel):
    level: Literal["high", "medium", "low"] = "medium"
    contributors: List[str] = Field(default_factory=list)


class FallbackSummary(BaseModel):
    used: bool = False
    notes: List[str] = Field(default_factory=list)

class DecisionPathwayRecommendation(BaseModel):
    recommended_pathway: Literal["continue", "transform", "partner", "close"]
    confidence: Literal["high", "medium", "low"] = "medium"
    runner_up_pathways: List[Literal["continue", "transform", "partner", "close"]] = Field(default_factory=list)
    top_risks: List[str] = Field(default_factory=list)
    required_validations: List[str] = Field(default_factory=list)
    next_12_month_actions: List[str] = Field(default_factory=list)
    partner_assessment: Optional[PartnerPathAssessment] = None


class PortfolioCandidateLocation(BaseModel):
    name: str
    address: str
    notes: Optional[str] = None


class PortfolioScenarioSet(BaseModel):
    name: str
    assumptions: Dict[str, float | int | str | bool] = Field(default_factory=dict)


class PortfolioCompareSnapshot(BaseModel):
    snapshot_id: str
    label: str
    school_name: str
    analysis_address: str
    compared_ministry_types: List[Literal["schools", "housing", "elder_care"]] = Field(default_factory=list)
    results: List[CompareAnalysisSummary] = Field(default_factory=list)


class PortfolioWorkspaceCreateRequest(BaseModel):
    engagement_name: str
    client_name: str
    candidate_locations: List[PortfolioCandidateLocation] = Field(default_factory=list)
    scenario_sets: List[PortfolioScenarioSet] = Field(default_factory=list)


class PortfolioWorkspaceUpdateRequest(BaseModel):
    engagement_name: Optional[str] = None
    client_name: Optional[str] = None
    candidate_locations: Optional[List[PortfolioCandidateLocation]] = None
    scenario_sets: Optional[List[PortfolioScenarioSet]] = None


class PortfolioWorkspaceResponse(BaseModel):
    workspace_id: str
    engagement_name: str
    client_name: str
    candidate_locations: List[PortfolioCandidateLocation] = Field(default_factory=list)
    scenario_sets: List[PortfolioScenarioSet] = Field(default_factory=list)
    compare_snapshots: List[PortfolioCompareSnapshot] = Field(default_factory=list)


class AnalysisResponse(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    school_name: str
    ministry_type: Literal["schools", "housing", "elder_care"] = "schools"
    analysis_address: str
    county_name: str
    state_name: str
    lat: float
    lon: float
    radius_miles: float           # Effective bounding radius (from isochrone or fallback)
    catchment_minutes: Optional[int] = None
    isochrone_polygon: Optional[dict] = None
    catchment_type: str = "radius"  # "isochrone" or "radius"
    gender: str = "coed"
    grade_level: str = "k12"
    demographics: DemographicData
    competitor_schools: List[CompetitorSchool]
    catholic_school_count: int
    total_private_school_count: int
    feasibility_score: FeasibilityScore
    recommendation: str
    recommendation_detail: str
    decision_pathway: Optional[DecisionPathwayRecommendation] = None
    data_notes: List[str] = []
    trend: Optional[DemographicTrend] = None
    population_gravity: Optional[PopulationGravityMap] = None
    enrollment_forecast: Optional[EnrollmentForecast] = None
    trace_id: Optional[str] = None
    run_mode: Literal["live_only", "db_with_fallback", "db_strict"] = "db_with_fallback"
    catchment_mode: Literal["isochrone", "radius"] = "radius"
    outcome: Literal["success", "degraded_success", "strict_mode_blocked", "upstream_unavailable", "export_blocked_readiness"] = "success"
    fallback_summary: FallbackSummary = Field(default_factory=FallbackSummary)
    confidence_summary: ConfidenceSummary = Field(default_factory=ConfidenceSummary)
    data_dependencies: List[DataDependencyStatus] = Field(default_factory=list)
    export_readiness: ExportReadiness = Field(default_factory=ExportReadiness)
    section_explanations: List[SectionExplanation] = Field(default_factory=list)
    data_freshness: Optional[DataFreshnessMetadata] = None
    benchmark_narrative: Optional[BenchmarkNarrative] = None
    board_report_pack: Optional[BoardReportPack] = None
