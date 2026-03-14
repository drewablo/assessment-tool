export type MinistryType = "schools" | "housing" | "elder_care";
export type CareLevel = "all" | "snf" | "assisted_living" | "memory_care";
export type HousingTargetPopulation = "senior_only" | "all_ages";

export interface Stage2FinancialYear {
  year: number;
  student_count?: number;
  tuition_revenue?: number;
  total_revenue?: number;
  fundraising_income?: number;
  total_income?: number;
  payroll_expense?: number;
  total_expenses?: number;
  investment_transfers?: number;
  one_time_income?: number;
}

export interface HousingStage2Inputs {
  occupancy_rate?: number;
  operating_cost_per_unit?: number;
  dscr?: number;
  subsidy_dependency?: number;
  operating_reserve_months?: number;
  capital_reserve_per_unit?: number;
}

export interface ElderCareStage2Inputs {
  occupancy_rate?: number;
  operating_cost_per_bed?: number;
  staffing_hours_per_resident_day?: number;
  payer_mix_private_pay?: number;
  payer_mix_medicaid?: number;
  days_cash_on_hand?: number;
}

export interface Stage2Inputs {
  historical_financials?: Stage2FinancialYear[];
  school_audit_financials?: SchoolAuditFinancialYear[];
  school_stage2_confirmed?: boolean;
  housing_financials?: HousingStage2Inputs;
  elder_care_financials?: ElderCareStage2Inputs;
}

export interface FacilityProfile {
  building_square_footage?: number;
  accessibility_constraints?: string[];
  current_layout_notes?: string;
  deferred_maintenance_estimate?: number;
  zoning_use_constraints?: string[];
  sponsor_operator_capacity?: "low" | "medium" | "high";
}

export interface SchoolAuditFinancialYear {
  fiscal_year?: number | null;
  year_label?: string | null;
  year_label_needs_confirmation?: boolean;
  tuition_revenue?: number | null;
  tuition_aid?: number | null;
  other_revenue?: number | null;
  total_expenses?: number | null;
  non_operating_revenue?: number | null;
  total_assets?: number | null;
  enrollment?: number | null;
  source_file?: string | null;
  source_audit_index?: number | null;
  missing_fields?: string[];
}

export interface SchoolAuditExtractionResponse {
  years: SchoolAuditFinancialYear[];
  warnings: string[];
}

export interface AnalysisHistoryRecord {
  id: number;
  school_name: string;
  address: string;
  ministry_type: MinistryType;
  overall_score: number | null;
  result_summary: Record<string, unknown> | null;
  request_params: AnalysisRequest | null;
  created_at: string;
}

export type RunMode = "live_only" | "db_with_fallback" | "db_strict";

export interface AnalysisRequest {
  school_name: string;
  address: string;
  ministry_type: MinistryType;
  mission_mode: boolean;
  drive_minutes: number;
  gender: "coed" | "boys" | "girls";
  grade_level: "k5" | "k8" | "high_school" | "k12";
  weighting_profile: "standard_baseline" | "affordability_sensitive" | "demand_primacy";
  market_context: "urban" | "suburban" | "rural";
  care_level: CareLevel;
  housing_target_population?: HousingTargetPopulation;
  min_mds_overall_rating?: 1 | 2 | 3 | 4 | 5;
  stage2_inputs?: Stage2Inputs;
  facility_profile?: FacilityProfile;
  run_mode?: RunMode;
}


export interface CompareAnalysisSummary {
  ministry_type: MinistryType;
  overall_score: number;
  scenario_conservative: number;
  scenario_optimistic: number;
  recommendation: string;
  recommendation_detail: string;
  recommended_pathway: "continue" | "transform" | "partner" | "close";
  pathway_confidence: "high" | "medium" | "low";
  fit_band: "high" | "medium" | "low";
  capital_intensity: "low" | "moderate" | "high";
  regulatory_complexity: "low" | "moderate" | "high";
  operator_dependency: "none" | "optional" | "required";
  time_to_launch_months_estimate: number;
}

export interface CompareAnalysisResponse {
  school_name: string;
  analysis_address: string;
  compared_ministry_types: MinistryType[];
  results: CompareAnalysisSummary[];
}

export interface SubIndicator {
  key: string;
  label: string;
  score: number;
  weight: number;
  description: string;
}

export interface MetricScore {
  score: number;
  label: string;
  description: string;
  weight: number;
  rating: "strong" | "moderate" | "weak" | "poor";
  sub_indicators?: SubIndicator[];
}

export interface Stage2Component {
  key: string;
  label: string;
  weight: number;
  score: number | null;
}

export interface Stage2Score {
  available: boolean;
  score: number | null;
  schema_version: string;
  formula_version: string;
  computed_at_utc: string | null;
  readiness: "not_ready" | "partial" | "ready";
  required_inputs: string[];
  provided_inputs: string[];
  missing_inputs: string[];
  components: Stage2Component[];
  note: string;
}

export interface ComparableMarket {
  geoid: string;
  overall_score: number;
  market_size_score: number;
  income_score: number;
  competition_score: number;
  family_density_score: number;
  percentile_state: number | null;
  percentile_national: number | null;
  similarity_distance: number | null;
}

export interface BenchmarkPercentiles {
  percentile_state: number | null;
  percentile_national: number | null;
  percentile_msa: number | null;
  state_name: string | null;
  msa_name: string | null;
  sample_size_state: number | null;
  sample_size_national: number | null;
  sample_size_msa: number | null;
  comparable_markets: ComparableMarket[];
}

export interface HierarchicalScore {
  market_opportunity: MetricScore | null;
  competitive_position: MetricScore | null;
  community_fit: MetricScore | null;
  sustainability_risk: MetricScore | null;
}

export interface FeasibilityScore {
  overall: number;
  scenario_conservative: number;
  scenario_optimistic: number;
  weighting_profile: "standard_baseline" | "affordability_sensitive" | "demand_primacy";
  market_size: MetricScore;
  income: MetricScore;
  competition: MetricScore;
  family_density: MetricScore;
  occupancy?: MetricScore | null;
  workforce?: MetricScore | null;
  stage2: Stage2Score | null;
  benchmarks?: BenchmarkPercentiles | null;
  hierarchical?: HierarchicalScore | null;
}

export interface DemographicData {
  total_population: number | null;
  population_under_18: number | null;
  school_age_population: number | null;
  estimated_catholic_school_age: number | null;
  median_household_income: number | null;
  total_households: number | null;
  families_with_children: number | null;
  owner_occupied_pct: number | null;
  estimated_catholic_pct: number | null;
  population_under_5?: number | null;
  pipeline_ratio?: number | null;
  pipeline_score?: number | null;
  private_enrollment_rate_pct?: number | null;
  private_enrollment_score?: number | null;
  data_geography: string;
  data_confidence: "high" | "medium" | "low" | null;
  ministry_target_population?: number | null;
  hud_tenant_households?: number | null;
  qct_designated_projects?: number | null;
  dda_designated_projects?: number | null;
  seniors_65_plus?: number | null;
  seniors_projected_5yr?: number | null;
  seniors_projected_10yr?: number | null;
}

export interface CompetitorSchool {
  name: string;
  lat: number;
  lon: number;
  distance_miles: number;
  affiliation: string;
  is_catholic: boolean;
  city: string | null;
  state?: string | null;
  street_address?: string | null;
  zip_code?: string | null;
  enrollment: number | null;
  gender: string;
  grade_level: string;
  occupancy_pct?: number | null;
  mds_overall_rating?: number | null;
  // HUD Section 202 detail fields
  total_units?: number | null;
  client_group_name?: string | null;
  property_category?: string | null;
  primary_financing_type?: string | null;
  phone_number?: string | null;
  reac_inspection_score?: number | null;
}

export interface DemographicTrend {
  school_age_pop_pct: number | null;
  income_real_pct: number | null;
  families_pct: number | null;
  trend_label: "Growing" | "Stable" | "Declining" | "Mixed" | "Unknown";
  period: string;
}

export interface DirectionSegment {
  school_age_pop: number;
  income_qualified_pop: number;
  catholic_qualified_pop?: number;
  pipeline_ratio: number | null;
  growth_signal: "Growing" | "Stable" | "Declining" | null;
  // Elder care directional fields
  seniors_65_plus?: number | null;
  seniors_75_plus?: number | null;
  seniors_living_alone?: number | null;
  seniors_below_poverty?: number | null;
  isolation_ratio?: number | null;
  // Housing directional fields
  cost_burdened_renters?: number | null;
  renter_households?: number | null;
  burden_ratio?: number | null;
}

export interface PopulationGravityMap {
  by_direction: Record<string, DirectionSegment>;
  dominant_direction: string | null;
  gravity_weighted: boolean;
}

export interface PartnerPathAssessment {
  mission_alignment_score: number;
  governance_model_options: string[];
  risk_transfer_profile: "low" | "moderate" | "high";
  partnership_readiness_checklist: string[];
}

export interface DecisionPathwayRecommendation {
  recommended_pathway: "continue" | "transform" | "partner" | "close";
  confidence: "high" | "medium" | "low";
  runner_up_pathways: Array<"continue" | "transform" | "partner" | "close">;
  top_risks: string[];
  required_validations: string[];
  next_12_month_actions: string[];
  partner_assessment?: PartnerPathAssessment | null;
}

export interface BenchmarkNarrative {
  peer_cohort: string;
  in_state_percentile?: number | null;
  national_percentile?: number | null;
  nearest_comparable_markets: string[];
  narrative_summary: string;
}

export interface BoardActionRoadmap {
  months_12: string[];
  months_24: string[];
  months_36: string[];
}

export interface BoardReportPack {
  executive_summary: string;
  key_risks: string[];
  strategic_options: string[];
  immediate_next_actions: string[];
  action_roadmap: BoardActionRoadmap;
  methodology_assumptions: string[];
  confidence_notes: string[];
}

export interface DataFreshnessSource {
  source_key: string;
  source_label: string;
  last_refreshed_utc?: string | null;
  freshness_hours?: number | null;
  status: "fresh" | "aging" | "stale" | "unknown";
  notes?: string | null;
}

export interface DataFreshnessMetadata {
  mode: "live" | "db_precomputed";
  generated_at_utc: string;
  sources: DataFreshnessSource[];
}

export interface PortfolioCandidateLocation {
  name: string;
  address: string;
  notes?: string;
}

export interface PortfolioScenarioSet {
  name: string;
  assumptions: Record<string, string | number | boolean>;
}

export interface PortfolioCompareSnapshot {
  snapshot_id: string;
  label: string;
  school_name: string;
  analysis_address: string;
  compared_ministry_types: MinistryType[];
  results: CompareAnalysisSummary[];
}

export interface PortfolioWorkspaceResponse {
  workspace_id: string;
  engagement_name: string;
  client_name: string;
  candidate_locations: PortfolioCandidateLocation[];
  scenario_sets: PortfolioScenarioSet[];
  compare_snapshots: PortfolioCompareSnapshot[];
}

export type PipelineFreshnessStatus = "fresh" | "aging" | "stale" | "unknown";

export interface PipelineFailureDetail {
  finished_at: string | null;
  error_message: string | null;
}

export interface PipelineStatusDetail {
  last_success: string | null;
  records_processed: number | null;
  records_inserted: number | null;
  freshness_hours: number | null;
  freshness_status: PipelineFreshnessStatus;
  last_failure: PipelineFailureDetail;
}

export interface PipelineStatusResponse {
  record_counts: Record<string, number>;
  pipelines: Record<string, PipelineStatusDetail>;
  stale_pipelines: string[];
  retry_recommended: boolean;
  hud_ingest?: Record<string, {
    status: string;
    dataset_year: number | null;
    source_version: string | null;
    started_at: string | null;
    finished_at: string | null;
    error_message: string | null;
  }>;
}

export interface OpportunityRecord {
  geoid: string;
  ministry_type: MinistryType;
  overall_score: number;
  market_size_score: number;
  income_score: number;
  competition_score: number;
  family_density_score: number;
  percentile_state: number | null;
  percentile_national: number | null;
}

export interface FactorWeightConfig {
  weight: number;
  description: string;
}

export type ScoringWeightsResponse = {
  schools: Record<string, FactorWeightConfig>;
  housing: Record<string, FactorWeightConfig>;
  elder_care: Record<string, FactorWeightConfig | Record<string, FactorWeightConfig>>;
  hierarchical?: Record<string, FactorWeightConfig>;
  hierarchical_by_ministry?: Record<MinistryType, Record<string, FactorWeightConfig>>;
  [key: string]: unknown;
};


export interface DataDependencyStatus {
  dataset: string;
  required: boolean;
  baseline_blocking: boolean;
  affects_confidence: boolean;
  export_blocking_in_strict: boolean;
  available: boolean;
  row_count: number;
  note?: string | null;
}

export interface FallbackSummary {
  used: boolean;
  notes: string[];
}

export interface ConfidenceSummary {
  level: "high" | "medium" | "low";
  contributors: string[];
}

export interface ExportReadiness {
  ready: boolean;
  status: "ready" | "warning" | "blocked";
  reasons: string[];
}

export interface SectionExplanation {
  section: string;
  inputs_used: string[];
  inputs_missing: string[];
  fallback_used: string[];
  confidence_impact: "none" | "low" | "medium" | "high";
}
export interface AnalysisResponse {
  run_mode: RunMode;
  catchment_mode: "isochrone" | "radius";
  outcome: "success" | "degraded_success" | "strict_mode_blocked" | "upstream_unavailable" | "export_blocked_readiness";
  fallback_summary: FallbackSummary;
  confidence_summary: ConfidenceSummary;
  data_dependencies: DataDependencyStatus[];
  export_readiness: ExportReadiness;
  section_explanations: SectionExplanation[];
  school_name: string;
  ministry_type: MinistryType;
  analysis_address: string;
  county_name: string;
  state_name: string;
  lat: number;
  lon: number;
  radius_miles: number;
  catchment_minutes: number | null;
  isochrone_polygon: object | null;
  catchment_type: string;
  gender: "coed" | "boys" | "girls";
  grade_level: "k5" | "k8" | "high_school" | "k12";
  demographics: DemographicData;
  competitor_schools: CompetitorSchool[];
  catholic_school_count: number;
  total_private_school_count: number;
  feasibility_score: FeasibilityScore;
  recommendation: string;
  recommendation_detail: string;
  decision_pathway?: DecisionPathwayRecommendation | null;
  data_notes: string[];
  trend: DemographicTrend | null;
  population_gravity: PopulationGravityMap | null;
  trace_id?: string | null;
  data_freshness?: DataFreshnessMetadata | null;
  benchmark_narrative?: BenchmarkNarrative | null;
  board_report_pack?: BoardReportPack | null;
}
