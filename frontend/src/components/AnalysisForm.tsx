"use client";

import { useEffect, useState } from "react";
import {
  Search,
  MapPin,
  Clock,
  Users,
  GraduationCap,
  SlidersHorizontal,
  ChevronDown,
  ChevronUp,
} from "lucide-react";
import {
  AnalysisRequest,
  Stage2FinancialYear,
  MinistryType,
  CareLevel,
  SchoolAuditFinancialYear,
  HousingStage2Inputs,
  ElderCareStage2Inputs,
  FacilityProfile,
  HousingTargetPopulation,
} from "@/lib/types";
import { extractSchoolAuditFinancials } from "@/lib/api";

interface SavedAnalysisOption {
  id: string;
  label: string;
  request: AnalysisRequest;
}

interface Props {
  onSubmit: (req: AnalysisRequest) => void;
  loading: boolean;
  initialRequest?: AnalysisRequest | null;
  savedOptions?: SavedAnalysisOption[];
  onSaveCurrent?: (req: AnalysisRequest) => { ok: boolean; message: string };
  onLoadSaved?: (req: AnalysisRequest) => void;
  onDeleteSaved?: (id: string) => void;
  saveStatus?: string | null;
}

const GENDER_OPTIONS: { value: AnalysisRequest["gender"]; label: string }[] = [
  { value: "coed", label: "Co-ed" },
  { value: "boys", label: "All Boys" },
  { value: "girls", label: "All Girls" },
];

const GRADE_OPTIONS: { value: AnalysisRequest["grade_level"]; label: string }[] = [
  { value: "k5", label: "K–5 (Elementary)" },
  { value: "k8", label: "K–8 (Elementary + Middle)" },
  { value: "high_school", label: "High School (9–12)" },
  { value: "k12", label: "K–12" },
];

const GRADE_LEVEL_DEFAULT_MINUTES: Record<AnalysisRequest["grade_level"], number> = {
  k5: 15,
  k8: 20,
  high_school: 30,
  k12: 20,
};

const MARKET_CONTEXT_OPTIONS: {
  value: AnalysisRequest["market_context"];
  label: string;
  hint: string;
}[] = [
  { value: "suburban", label: "Suburban", hint: "Default calibration." },
  { value: "urban", label: "Urban / dense city", hint: "Higher bar per tier due to alternatives." },
  { value: "rural", label: "Rural / small town", hint: "Lower bar per tier for smaller markets." },
];

const WEIGHTING_OPTIONS: {
  value: AnalysisRequest["weighting_profile"];
  label: string;
  hint: string;
}[] = [
  { value: "standard_baseline", label: "Standard baseline", hint: "35/25/25/15" },
  { value: "affordability_sensitive", label: "Affordability-sensitive", hint: "30/30/20/20" },
  { value: "demand_primacy", label: "Demand-primacy", hint: "40/20/25/15" },
];

const HOUSING_TARGET_POPULATION_OPTIONS: {
  value: HousingTargetPopulation;
  label: string;
}[] = [
  { value: "senior_only", label: "Senior-only affordable housing" },
  { value: "all_ages", label: "Affordable housing for all age groups" },
];

type Stage2YearInput = {
  year: string;
  student_count: string;
  tuition_revenue: string;
  total_revenue: string;
  fundraising_income: string;
  payroll_expense: string;
  total_expenses: string;
  investment_transfers: string;
  one_time_income: string;
};

type SchoolAuditNumericKey =
  | "fiscal_year"
  | "tuition_revenue"
  | "tuition_aid"
  | "other_revenue"
  | "total_expenses"
  | "non_operating_revenue"
  | "total_assets"
  | "enrollment";

function emptyYear(year: string): Stage2YearInput {
  return {
    year,
    student_count: "",
    tuition_revenue: "",
    total_revenue: "",
    fundraising_income: "",
    payroll_expense: "",
    total_expenses: "",
    investment_transfers: "",
    one_time_income: "",
  };
}

const DEFAULT_STAGE2_YEARS: Stage2YearInput[] = [
  emptyYear("2022"),
  emptyYear("2023"),
  emptyYear("2024"),
];

function parseNumber(value: string | undefined): number | undefined {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

/** RFC-4180 compliant CSV row parser — handles quoted fields and embedded commas. */
function parseCsvRow(line: string): string[] {
  const result: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === "," && !inQuotes) {
      result.push(current.trim());
      current = "";
    } else {
      current += ch;
    }
  }

  result.push(current.trim());
  return result;
}

function parseCsvFinancials(raw: string): Stage2YearInput[] {
  const lines = raw
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);

  if (lines.length < 2) return [];

  const headers = parseCsvRow(lines[0]).map((h) => h.toLowerCase());
  const idx = (name: string) => headers.indexOf(name);

  return lines
    .slice(1)
    .map((line) => {
      const cols = parseCsvRow(line);
      const get = (name: string) => (idx(name) >= 0 ? cols[idx(name)] ?? "" : "");

      return {
        year: get("year"),
        student_count: get("student_count"),
        tuition_revenue: get("tuition_revenue"),
        total_revenue: get("total_revenue"),
        fundraising_income: get("fundraising_income"),
        payroll_expense: get("payroll_expense"),
        total_expenses: get("total_expenses"),
        investment_transfers: get("investment_transfers"),
        one_time_income: get("one_time_income"),
      };
    })
    .filter((r) => r.year);
}

export default function AnalysisForm({
  onSubmit,
  loading,
  initialRequest = null,
  savedOptions = [],
  onSaveCurrent,
  onLoadSaved,
  onDeleteSaved,
  saveStatus = null,
}: Props) {
  const [schoolName, setSchoolName] = useState("");
  const [address, setAddress] = useState("");
  const [ministryType, setMinistryType] = useState<MinistryType>("schools");
  const [missionMode, setMissionMode] = useState(true);
  const [careLevel, setCareLevel] = useState<CareLevel>("all");
  const [minMdsOverallRating, setMinMdsOverallRating] = useState<"" | "1" | "2" | "3" | "4" | "5">("");
  const [gradeLevel, setGradeLevel] = useState<AnalysisRequest["grade_level"]>("k12");
  const [driveMinutes, setDriveMinutes] = useState(GRADE_LEVEL_DEFAULT_MINUTES.k12);
  const [gender, setGender] = useState<AnalysisRequest["gender"]>("coed");
  const [weightingProfile, setWeightingProfile] =
    useState<AnalysisRequest["weighting_profile"]>("standard_baseline");
  const [marketContext, setMarketContext] = useState<AnalysisRequest["market_context"]>("suburban");
  const [housingTargetPopulation, setHousingTargetPopulation] =
    useState<HousingTargetPopulation>("all_ages");
  const [includeStage2, setIncludeStage2] = useState(false);
  const [historicalFinancials, setHistoricalFinancials] =
    useState<Stage2YearInput[]>(DEFAULT_STAGE2_YEARS);
  const [auditInputMode, setAuditInputMode] = useState<"ml" | "manual">("ml");
  const [schoolAuditRows, setSchoolAuditRows] = useState<SchoolAuditFinancialYear[]>([]);
  const [schoolAuditWarnings, setSchoolAuditWarnings] = useState<string[]>([]);
  const [schoolStage2Confirmed, setSchoolStage2Confirmed] = useState(false);
  const [uploadingAudit, setUploadingAudit] = useState(false);
  const [housingFinancials, setHousingFinancials] =
    useState<Partial<Record<keyof HousingStage2Inputs, string>>>({});
  const [elderCareFinancials, setElderCareFinancials] =
    useState<Partial<Record<keyof ElderCareStage2Inputs, string>>>({});
  const [facilityExpanded, setFacilityExpanded] = useState(false);
  const [facilityProfile, setFacilityProfile] =
    useState<Partial<Record<keyof FacilityProfile, string>>>({
      sponsor_operator_capacity: "medium",
    });

  const showSchoolsFields = ministryType === "schools";
  const showElderFields = ministryType === "elder_care";

  function updateHistoricalYear(index: number, key: keyof Stage2YearInput, value: string) {
    setHistoricalFinancials((prev) =>
      prev.map((row, i) => (i === index ? { ...row, [key]: value } : row)),
    );
  }

  function buildHistoricalFinancials(rows: Stage2YearInput[]): Stage2FinancialYear[] {
    return rows.reduce<Stage2FinancialYear[]>((acc, row) => {
      const year = parseNumber(row.year);
      if (year == null) return acc;

      const totalRevenue = parseNumber(row.total_revenue);
      const fundraisingIncome = parseNumber(row.fundraising_income) ?? 0;
      const oneTimeIncome = parseNumber(row.one_time_income) ?? 0;

      acc.push({
        year,
        student_count: parseNumber(row.student_count),
        tuition_revenue: parseNumber(row.tuition_revenue),
        total_revenue: totalRevenue,
        fundraising_income: parseNumber(row.fundraising_income),
        total_income:
          totalRevenue != null ? totalRevenue + fundraisingIncome + oneTimeIncome : undefined,
        payroll_expense: parseNumber(row.payroll_expense),
        total_expenses: parseNumber(row.total_expenses),
        investment_transfers: parseNumber(row.investment_transfers),
        one_time_income: parseNumber(row.one_time_income),
      });

      return acc;
    }, []);
  }

  function parseList(value: string | undefined): string[] {
    if (!value) return [];
    return value
      .split(/\n|,/)
      .map((item) => item.trim())
      .filter(Boolean);
  }

  function buildFacilityProfile(): FacilityProfile | undefined {
    const footprint = parseNumber(facilityProfile.building_square_footage);
    const deferred = parseNumber(facilityProfile.deferred_maintenance_estimate);
    const accessibility = parseList(facilityProfile.accessibility_constraints);
    const zoning = parseList(facilityProfile.zoning_use_constraints);
    const layoutNotes = facilityProfile.current_layout_notes?.trim();

    const hasAny =
      footprint != null ||
      deferred != null ||
      accessibility.length > 0 ||
      zoning.length > 0 ||
      Boolean(layoutNotes) ||
      (facilityProfile.sponsor_operator_capacity &&
        facilityProfile.sponsor_operator_capacity !== "medium");

    if (!hasAny) return undefined;

    return {
      building_square_footage: footprint,
      deferred_maintenance_estimate: deferred,
      accessibility_constraints: accessibility,
      zoning_use_constraints: zoning,
      current_layout_notes: layoutNotes,
      sponsor_operator_capacity:
        (facilityProfile.sponsor_operator_capacity as FacilityProfile["sponsor_operator_capacity"]) ??
        "medium",
    };
  }

  function buildHousingFinancials(): HousingStage2Inputs {
    const p = (v: string | undefined) => (v ? parseFloat(v) : undefined);

    return {
      occupancy_rate: p(housingFinancials.occupancy_rate),
      operating_cost_per_unit: p(housingFinancials.operating_cost_per_unit),
      dscr: p(housingFinancials.dscr),
      subsidy_dependency: p(housingFinancials.subsidy_dependency),
      operating_reserve_months: p(housingFinancials.operating_reserve_months),
      capital_reserve_per_unit: p(housingFinancials.capital_reserve_per_unit),
    };
  }

  function buildElderCareFinancials(): ElderCareStage2Inputs {
    const p = (v: string | undefined) => (v ? parseFloat(v) : undefined);

    return {
      occupancy_rate: p(elderCareFinancials.occupancy_rate),
      operating_cost_per_bed: p(elderCareFinancials.operating_cost_per_bed),
      staffing_hours_per_resident_day: p(elderCareFinancials.staffing_hours_per_resident_day),
      payer_mix_private_pay: p(elderCareFinancials.payer_mix_private_pay),
      payer_mix_medicaid: p(elderCareFinancials.payer_mix_medicaid),
      days_cash_on_hand: p(elderCareFinancials.days_cash_on_hand),
    };
  }

  function buildCurrentRequest(): AnalysisRequest {
    const normalizedAuditRows = schoolAuditRows.slice(0, 3).map((row) => ({
      ...row,
      fiscal_year: row.fiscal_year != null ? Number(row.fiscal_year) : null,
      enrollment: row.enrollment != null ? Number(row.enrollment) : null,
      tuition_revenue: row.tuition_revenue != null ? Number(row.tuition_revenue) : null,
      tuition_aid: row.tuition_aid != null ? Number(row.tuition_aid) : null,
      other_revenue: row.other_revenue != null ? Number(row.other_revenue) : null,
      total_expenses: row.total_expenses != null ? Number(row.total_expenses) : null,
      non_operating_revenue:
        row.non_operating_revenue != null ? Number(row.non_operating_revenue) : null,
      total_assets: row.total_assets != null ? Number(row.total_assets) : null,
    }));

    return {
      school_name: schoolName.trim(),
      address: address.trim(),
      ministry_type: ministryType,
      mission_mode: showElderFields ? missionMode : false,
      drive_minutes: driveMinutes,
      gender,
      grade_level: gradeLevel,
      weighting_profile: weightingProfile,
      market_context: marketContext,
      care_level: showElderFields ? careLevel : "all",
      housing_target_population: ministryType === "housing" ? housingTargetPopulation : undefined,
      min_mds_overall_rating:
        showElderFields && minMdsOverallRating
          ? (Number(minMdsOverallRating) as 1 | 2 | 3 | 4 | 5)
          : undefined,
      stage2_inputs: showSchoolsFields
        ? {
            historical_financials: buildHistoricalFinancials(historicalFinancials),
            school_audit_financials: normalizedAuditRows,
            school_stage2_confirmed: schoolStage2Confirmed,
          }
        : ministryType === "housing" && includeStage2
          ? { housing_financials: buildHousingFinancials() }
          : ministryType === "elder_care" && includeStage2
            ? { elder_care_financials: buildElderCareFinancials() }
            : undefined,
      facility_profile: buildFacilityProfile(),
    };
  }

  useEffect(() => {
    if (!initialRequest) return;

    setSchoolName(initialRequest.school_name ?? "");
    setAddress(initialRequest.address ?? "");
    setMinistryType(initialRequest.ministry_type ?? "schools");
    setMissionMode(initialRequest.mission_mode ?? true);
    setCareLevel(initialRequest.care_level ?? "all");
    setMinMdsOverallRating(
      initialRequest.min_mds_overall_rating
        ? (String(initialRequest.min_mds_overall_rating) as "1" | "2" | "3" | "4" | "5")
        : "",
    );
    setGradeLevel(initialRequest.grade_level ?? "k12");
    setDriveMinutes(initialRequest.drive_minutes ?? GRADE_LEVEL_DEFAULT_MINUTES.k12);
    setGender(initialRequest.gender ?? "coed");
    setWeightingProfile(initialRequest.weighting_profile ?? "standard_baseline");
    setMarketContext(initialRequest.market_context ?? "suburban");
    setHousingTargetPopulation(initialRequest.housing_target_population ?? "all_ages");

    setFacilityProfile({
      building_square_footage:
        initialRequest.facility_profile?.building_square_footage != null
          ? String(initialRequest.facility_profile.building_square_footage)
          : "",
      deferred_maintenance_estimate:
        initialRequest.facility_profile?.deferred_maintenance_estimate != null
          ? String(initialRequest.facility_profile.deferred_maintenance_estimate)
          : "",
      accessibility_constraints: (initialRequest.facility_profile?.accessibility_constraints ?? []).join(
        ", ",
      ),
      zoning_use_constraints: (initialRequest.facility_profile?.zoning_use_constraints ?? []).join(
        ", ",
      ),
      current_layout_notes: initialRequest.facility_profile?.current_layout_notes ?? "",
      sponsor_operator_capacity:
        initialRequest.facility_profile?.sponsor_operator_capacity ?? "medium",
    });

    const rows = initialRequest.stage2_inputs?.historical_financials;
    const auditRows = initialRequest.stage2_inputs?.school_audit_financials;

    if (auditRows && auditRows.length) {
      setSchoolAuditRows(auditRows.slice(0, 3));
      setSchoolStage2Confirmed(Boolean(initialRequest.stage2_inputs?.school_stage2_confirmed));
    }

    if (rows && rows.length) {
      setIncludeStage2(true);
      setHistoricalFinancials(
        rows.slice(0, 3).map((r) => ({
          year: String(r.year ?? ""),
          student_count: String(r.student_count ?? ""),
          tuition_revenue: String(r.tuition_revenue ?? ""),
          total_revenue: String(r.total_revenue ?? ""),
          fundraising_income: String(r.fundraising_income ?? ""),
          payroll_expense: String(r.payroll_expense ?? ""),
          total_expenses: String(r.total_expenses ?? ""),
          investment_transfers: String(r.investment_transfers ?? ""),
          one_time_income: String(r.one_time_income ?? ""),
        })),
      );
    }
  }, [initialRequest]);

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!schoolName.trim() || !address.trim()) return;
    onSubmit(buildCurrentRequest());
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-5">
      <div className="rounded-lg border border-gray-200 bg-gray-50 p-3 space-y-2">
        <label className="block text-xs font-semibold text-gray-700">Saved projects</label>
        <select
          className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm bg-white"
          onChange={(e) => {
            const selected = savedOptions.find((o) => o.id === e.target.value);
            if (selected) onLoadSaved?.(selected.request);
          }}
          defaultValue=""
        >
          <option value="">Select saved analysis...</option>
          {savedOptions.map((option) => (
            <option key={option.id} value={option.id}>
              {option.label}
            </option>
          ))}
        </select>

        {savedOptions.length === 0 ? (
          <p className="text-xs text-gray-500">
            No saved projects yet. Saved items persist in this browser (local storage).
          </p>
        ) : (
          <ul className="space-y-1.5 max-h-36 overflow-y-auto">
            {savedOptions.map((option) => (
              <li
                key={option.id}
                className="flex items-center justify-between gap-2 text-xs text-gray-700 bg-white border border-gray-200 rounded px-2 py-1.5"
              >
                <span className="truncate">{option.label}</span>
                {onDeleteSaved && (
                  <button
                    type="button"
                    className="text-red-600 hover:text-red-700 font-medium"
                    onClick={() => onDeleteSaved(option.id)}
                  >
                    Delete
                  </button>
                )}
              </li>
            ))}
          </ul>
        )}

        {saveStatus && <p className="text-xs text-gray-600">{saveStatus}</p>}
      </div>

      <div>
        <label className="block text-sm font-semibold text-gray-700 mb-2">Ministry Type</label>
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
          {([
            ["schools", "Schools"],
            ["housing", "Housing"],
            ["elder_care", "Elder Care"],
          ] as [MinistryType, string][]).map(([value, label]) => (
            <button
              key={value}
              type="button"
              onClick={() => setMinistryType(value)}
              className={`rounded-lg border px-3 py-2 text-sm font-medium ${
                ministryType === value
                  ? "border-navy-700 bg-navy-50 text-navy-800"
                  : "border-gray-300 text-gray-600"
              }`}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {showElderFields && (
        <div className="rounded-lg border border-gray-200 p-3 bg-gray-50 space-y-3">
          <div>
            <p className="text-sm font-semibold text-gray-700 mb-1">Analysis Mode</p>
            <label className="block text-sm">
              <input
                type="radio"
                checked={missionMode}
                onChange={() => setMissionMode(true)}
                className="mr-2"
              />
              Mission-Aligned Analysis
            </label>
            <label className="block text-sm">
              <input
                type="radio"
                checked={!missionMode}
                onChange={() => setMissionMode(false)}
                className="mr-2"
              />
              Market Demand Analysis
            </label>
          </div>

          <div>
            <p className="text-sm font-semibold text-gray-700 mb-1">Care Level</p>
            <select
              value={careLevel}
              onChange={(e) => setCareLevel(e.target.value as CareLevel)}
              className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm bg-white"
            >
              <option value="all">All care levels</option>
              <option value="snf">Skilled nursing</option>
              <option value="assisted_living">Assisted living</option>
              <option value="memory_care">Memory care</option>
            </select>
          </div>

          <div>
            <p className="text-sm font-semibold text-gray-700 mb-1">Minimum MDS Overall Rating</p>
            <select
              value={minMdsOverallRating}
              onChange={(e) =>
                setMinMdsOverallRating(e.target.value as "" | "1" | "2" | "3" | "4" | "5")
              }
              className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm bg-white"
            >
              <option value="">Any rating</option>
              <option value="1">1 star or higher</option>
              <option value="2">2 stars or higher</option>
              <option value="3">3 stars or higher</option>
              <option value="4">4 stars or higher</option>
              <option value="5">5 stars only</option>
            </select>
          </div>
        </div>
      )}

      <div>
        <label className="block text-sm font-semibold text-gray-700 mb-1">Project Name</label>
        <input
          type="text"
          value={schoolName}
          onChange={(e) => setSchoolName(e.target.value)}
          required
          className="w-full border border-gray-300 rounded-lg px-4 py-2.5 text-sm"
        />
      </div>

      <div>
        <label className="block text-sm font-semibold text-gray-700 mb-1">
          <MapPin className="inline w-4 h-4 mr-1 text-gray-400" />
          Address
        </label>
        <input
          type="text"
          value={address}
          onChange={(e) => setAddress(e.target.value)}
          required
          className="w-full border border-gray-300 rounded-lg px-4 py-2.5 text-sm"
        />
      </div>

      {showSchoolsFields && (
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-1">
              <Users className="inline w-4 h-4 mr-1 text-gray-400" />
              Gender
            </label>
            <select
              value={gender}
              onChange={(e) => setGender(e.target.value as AnalysisRequest["gender"])}
              className="w-full border border-gray-300 rounded-lg px-3 py-2.5 text-sm bg-white"
            >
              {GENDER_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-1">
              <GraduationCap className="inline w-4 h-4 mr-1 text-gray-400" />
              Grade Level
            </label>
            <select
              value={gradeLevel}
              onChange={(e) => {
                const v = e.target.value as AnalysisRequest["grade_level"];
                setGradeLevel(v);
                setDriveMinutes(GRADE_LEVEL_DEFAULT_MINUTES[v]);
              }}
              className="w-full border border-gray-300 rounded-lg px-3 py-2.5 text-sm bg-white"
            >
              {GRADE_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </div>
        </div>
      )}

      <div>
        <label className="block text-sm font-semibold text-gray-700 mb-1">
          <Clock className="inline w-4 h-4 mr-1 text-gray-400" />
          Drive-Time Catchment: <span className="text-navy-600 font-bold">{driveMinutes} min</span>
        </label>
        <input
          type="range"
          min={5}
          max={60}
          step={5}
          value={driveMinutes}
          onChange={(e) => setDriveMinutes(Number(e.target.value))}
          className="w-full accent-navy-600"
        />
      </div>

      <div>
        <label className="block text-sm font-semibold text-gray-700 mb-1">Market Context</label>
        <select
          value={marketContext}
          onChange={(e) => setMarketContext(e.target.value as AnalysisRequest["market_context"])}
          className="w-full border border-gray-300 rounded-lg px-3 py-2.5 text-sm bg-white"
        >
          {MARKET_CONTEXT_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
      </div>

      <div>
        <label className="block text-sm font-semibold text-gray-700 mb-1">
          <SlidersHorizontal className="inline w-4 h-4 mr-1 text-gray-400" />
          Weighting Profile
        </label>
        <select
          value={weightingProfile}
          onChange={(e) =>
            setWeightingProfile(e.target.value as AnalysisRequest["weighting_profile"])
          }
          className="w-full border border-gray-300 rounded-lg px-3 py-2.5 text-sm bg-white"
        >
          {WEIGHTING_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
      </div>

      {showSchoolsFields ? (
        <div className="rounded-lg border border-gray-200 p-4 bg-gray-50 space-y-3">
          <p className="text-sm font-semibold text-gray-700">Stage 2 Institutional Financials</p>

          <div className="flex gap-2">
            <button
              type="button"
              onClick={() => {
                setAuditInputMode("ml");
                setSchoolAuditRows([]);
                setSchoolAuditWarnings([]);
                setSchoolStage2Confirmed(false);
              }}
              className={`px-3 py-1.5 rounded text-xs font-medium border transition-colors ${
                auditInputMode === "ml"
                  ? "bg-navy-700 text-white border-navy-700"
                  : "bg-white text-gray-600 border-gray-300 hover:bg-gray-50"
              }`}
            >
              Machine Learning Extraction
            </button>

            <button
              type="button"
              onClick={() => {
                setAuditInputMode("manual");
                setSchoolAuditWarnings([]);
                setSchoolStage2Confirmed(false);
              }}
              className={`px-3 py-1.5 rounded text-xs font-medium border transition-colors ${
                auditInputMode === "manual"
                  ? "bg-navy-700 text-white border-navy-700"
                  : "bg-white text-gray-600 border-gray-300 hover:bg-gray-50"
              }`}
            >
              Manual Entry
            </button>
          </div>

          {auditInputMode === "ml" ? (
            <>
              <input
                type="file"
                multiple
                accept="application/pdf,.pdf"
                onChange={async (e) => {
                  const files = Array.from(e.target.files || []);
                  if (!files.length) return;

                  setUploadingAudit(true);
                  try {
                    const extracted = await extractSchoolAuditFinancials(files);
                    setSchoolAuditRows(extracted.years.slice(0, 3));
                    setSchoolAuditWarnings(extracted.warnings || []);
                    setSchoolStage2Confirmed(false);
                  } catch (err) {
                    setSchoolAuditWarnings([
                      err instanceof Error ? err.message : "Audit extraction failed",
                    ]);
                  } finally {
                    setUploadingAudit(false);
                  }
                }}
                className="text-xs"
              />
              <p className="text-xs text-gray-600">
                Upload one or more audit PDFs. Review extracted values, confirm fiscal year labels,
                then enter enrollment by year.
              </p>
              {uploadingAudit && (
                <p className="text-xs text-gray-500">
                  Extracting financials from uploaded audits...
                </p>
              )}
            </>
          ) : (
            <>
              <p className="text-xs text-gray-600">
                Enter financial data manually for up to 3 fiscal years.
              </p>
              <button
                type="button"
                onClick={() => {
                  if (schoolAuditRows.length >= 3) return;
                  setSchoolAuditRows((prev) => [
                    ...prev,
                    {
                      fiscal_year: null,
                      year_label: null,
                      year_label_needs_confirmation: false,
                      tuition_revenue: null,
                      tuition_aid: null,
                      other_revenue: null,
                      total_expenses: null,
                      non_operating_revenue: null,
                      total_assets: null,
                      enrollment: null,
                      source_file: null,
                      source_audit_index: prev.length,
                      missing_fields: [],
                    },
                  ]);
                  setSchoolStage2Confirmed(false);
                }}
                disabled={schoolAuditRows.length >= 3}
                className="text-xs px-3 py-1.5 rounded border border-gray-300 bg-white text-gray-700 hover:bg-gray-50 disabled:opacity-40"
              >
                + Add Year
              </button>
            </>
          )}

          {schoolAuditWarnings.map((warning, idx) => (
            <p key={idx} className="text-xs text-yellow-700">
              ⚠ {warning}
            </p>
          ))}

          {schoolAuditRows.map((row, index) => (
            <div key={index} className="border border-gray-200 rounded-lg bg-white p-3">
              {auditInputMode === "manual" && (
                <div className="flex justify-end mb-2">
                  <button
                    type="button"
                    onClick={() => {
                      setSchoolAuditRows((prev) => prev.filter((_, i) => i !== index));
                      setSchoolStage2Confirmed(false);
                    }}
                    className="text-xs text-red-500 hover:text-red-700"
                  >
                    Remove
                  </button>
                </div>
              )}

              <div className="grid grid-cols-2 md:grid-cols-4 gap-2.5">
                {(
                  [
                    "fiscal_year",
                    "tuition_revenue",
                    "tuition_aid",
                    "other_revenue",
                    "total_expenses",
                    "non_operating_revenue",
                    "total_assets",
                    "enrollment",
                  ] as SchoolAuditNumericKey[]
                ).map((key) => {
                  const label =
                    key === "total_assets"
                      ? "total_assets (optional, informational)"
                      : String(key);

                  return (
                    <label key={String(key)} className="text-xs text-gray-600">
                      {label}
                      <input
                        type="number"
                        value={(row[key] as number | null | undefined) ?? ""}
                        onChange={(e) => {
                          const next = e.target.value === "" ? null : Number(e.target.value);
                          setSchoolAuditRows((prev) =>
                            prev.map((r, i) => (i === index ? { ...r, [key]: next } : r)),
                          );
                          setSchoolStage2Confirmed(false);
                        }}
                        className="mt-1 w-full border border-gray-300 rounded px-2 py-1.5 text-sm"
                      />
                    </label>
                  );
                })}
              </div>

              {!!row.missing_fields?.length && (
                <p className="mt-2 text-xs text-red-600">
                  Missing fields: {row.missing_fields.join(", ")}
                </p>
              )}
            </div>
          ))}

          <div className="space-y-2 border-t border-gray-200 pt-3">
            <p className="text-sm font-semibold text-gray-700">
              Manual historical financials (optional)
            </p>
            <p className="text-xs text-gray-500">
              These rows are sent as <code>historical_financials</code> and can supplement
              audit-derived data.
            </p>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
              {historicalFinancials.map((row, index) => (
                <div
                  key={`hist-${index}`}
                  className="bg-white border border-gray-200 rounded p-2 space-y-1.5"
                >
                  <p className="text-xs font-semibold text-gray-700">Year {index + 1}</p>
                  {(
                    ["year", "student_count", "tuition_revenue", "total_revenue", "total_expenses"] as (
                      keyof Stage2YearInput
                    )[]
                  ).map((key) => (
                    <input
                      key={key}
                      type="number"
                      placeholder={key}
                      value={row[key]}
                      onChange={(e) => updateHistoricalYear(index, key, e.target.value)}
                      className="w-full border border-gray-300 rounded px-2 py-1 text-xs"
                    />
                  ))}
                </div>
              ))}
            </div>
          </div>

          {schoolAuditRows.length > 0 && (
            <label className="flex items-center gap-2 text-sm font-semibold text-gray-700">
              <input
                type="checkbox"
                checked={schoolStage2Confirmed}
                onChange={(e) => setSchoolStage2Confirmed(e.target.checked)}
                className="rounded border-gray-300"
              />
              I confirm fiscal year labels, extracted values, and enrollment alignment for Stage 2
              scoring.
            </label>
          )}
        </div>
      ) : ministryType === "housing" ? (
        <div className="rounded-lg border border-gray-200 p-4 bg-gray-50 space-y-3">
          <div>
            <label className="block text-sm font-semibold text-gray-700 mb-1">Affordable housing type</label>
            <select
              value={housingTargetPopulation}
              onChange={(e) => setHousingTargetPopulation(e.target.value as HousingTargetPopulation)}
              className="w-full border border-gray-300 rounded-lg px-3 py-2.5 text-sm bg-white"
            >
              {HOUSING_TARGET_POPULATION_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>

          <label className="flex items-center gap-2 text-sm font-semibold text-gray-700">
            <input
              type="checkbox"
              checked={includeStage2}
              onChange={(e) => setIncludeStage2(e.target.checked)}
              className="rounded border-gray-300"
            />
            Stage 2 Operating KPIs (optional)
          </label>

          {includeStage2 && (
            <div className="space-y-2">
              <p className="text-xs text-gray-500">
                Enter current operating metrics to unlock a financial-health score alongside the
                market score.
              </p>
              <div className="grid grid-cols-2 md:grid-cols-3 gap-2.5">
                {(
                  [
                    ["occupancy_rate", "Occupancy Rate (0–1)", "e.g. 0.92"],
                    ["operating_cost_per_unit", "Op. Cost / Unit / Yr ($)", "e.g. 6500"],
                    ["dscr", "Debt Svc Coverage Ratio", "e.g. 1.25"],
                    ["subsidy_dependency", "Subsidy Dependency (0–1)", "e.g. 0.75"],
                    ["operating_reserve_months", "Operating Reserve (months)", "e.g. 4"],
                    ["capital_reserve_per_unit", "Capital Reserve / Unit ($)", "e.g. 400"],
                  ] as [keyof HousingStage2Inputs, string, string][]
                ).map(([key, label, placeholder]) => (
                  <label key={key} className="text-xs text-gray-600">
                    {label}
                    <input
                      type="number"
                      step="any"
                      placeholder={placeholder}
                      value={housingFinancials[key] ?? ""}
                      onChange={(e) =>
                        setHousingFinancials((prev) => ({ ...prev, [key]: e.target.value }))
                      }
                      className="mt-1 w-full border border-gray-300 rounded px-2 py-1.5 text-sm"
                    />
                  </label>
                ))}
              </div>
            </div>
          )}
        </div>
      ) : ministryType === "elder_care" ? (
        <div className="rounded-lg border border-gray-200 p-4 bg-gray-50 space-y-3">
          <label className="flex items-center gap-2 text-sm font-semibold text-gray-700">
            <input
              type="checkbox"
              checked={includeStage2}
              onChange={(e) => setIncludeStage2(e.target.checked)}
              className="rounded border-gray-300"
            />
            Stage 2 Operating KPIs (optional)
          </label>

          {includeStage2 && (
            <div className="space-y-2">
              <p className="text-xs text-gray-500">
                Enter current operating metrics to unlock a financial-health score alongside the
                market score.
              </p>
              <div className="grid grid-cols-2 md:grid-cols-3 gap-2.5">
                {(
                  [
                    ["occupancy_rate", "Occupancy Rate (0–1)", "e.g. 0.88"],
                    ["operating_cost_per_bed", "Op. Cost / Bed / Yr ($)", "e.g. 60000"],
                    [
                      "staffing_hours_per_resident_day",
                      "Staffing Hours / Resident Day",
                      "e.g. 3.5",
                    ],
                    ["payer_mix_private_pay", "Private Pay Mix (0–1)", "e.g. 0.30"],
                    ["payer_mix_medicaid", "Medicaid Mix (0–1)", "e.g. 0.55"],
                    ["days_cash_on_hand", "Days Cash on Hand", "e.g. 60"],
                  ] as [keyof ElderCareStage2Inputs, string, string][]
                ).map(([key, label, placeholder]) => (
                  <label key={key} className="text-xs text-gray-600">
                    {label}
                    <input
                      type="number"
                      step="any"
                      placeholder={placeholder}
                      value={elderCareFinancials[key] ?? ""}
                      onChange={(e) =>
                        setElderCareFinancials((prev) => ({ ...prev, [key]: e.target.value }))
                      }
                      className="mt-1 w-full border border-gray-300 rounded px-2 py-1.5 text-sm"
                    />
                  </label>
                ))}
              </div>
            </div>
          )}
        </div>
      ) : null}

      <div className="rounded-lg border border-gray-200 bg-gray-50">
        <button
          type="button"
          onClick={() => setFacilityExpanded((v) => !v)}
          className="w-full px-4 py-3 flex items-center justify-between text-left"
        >
          <div>
            <p className="text-sm font-semibold text-gray-700">
              Facility / Transformation Constraints
            </p>
            <p className="text-xs text-gray-500">
              Optional inputs to improve pathway realism and risk flags.
            </p>
          </div>
          {facilityExpanded ? (
            <ChevronUp className="w-4 h-4 text-gray-500" />
          ) : (
            <ChevronDown className="w-4 h-4 text-gray-500" />
          )}
        </button>

        {facilityExpanded && (
          <div className="border-t border-gray-200 p-4 grid grid-cols-1 md:grid-cols-2 gap-3">
            <label className="text-xs text-gray-600">
              Building square footage
              <input
                type="number"
                min={0}
                value={facilityProfile.building_square_footage ?? ""}
                onChange={(e) =>
                  setFacilityProfile((prev) => ({
                    ...prev,
                    building_square_footage: e.target.value,
                  }))
                }
                className="mt-1 w-full border border-gray-300 rounded px-2 py-1.5 text-sm"
              />
            </label>

            <label className="text-xs text-gray-600">
              Deferred maintenance estimate ($)
              <input
                type="number"
                min={0}
                value={facilityProfile.deferred_maintenance_estimate ?? ""}
                onChange={(e) =>
                  setFacilityProfile((prev) => ({
                    ...prev,
                    deferred_maintenance_estimate: e.target.value,
                  }))
                }
                className="mt-1 w-full border border-gray-300 rounded px-2 py-1.5 text-sm"
              />
            </label>

            <label className="text-xs text-gray-600 md:col-span-2">
              Accessibility constraints (comma-separated)
              <input
                type="text"
                value={facilityProfile.accessibility_constraints ?? ""}
                onChange={(e) =>
                  setFacilityProfile((prev) => ({
                    ...prev,
                    accessibility_constraints: e.target.value,
                  }))
                }
                className="mt-1 w-full border border-gray-300 rounded px-2 py-1.5 text-sm"
              />
            </label>

            <label className="text-xs text-gray-600 md:col-span-2">
              Zoning/use constraints (comma-separated)
              <input
                type="text"
                value={facilityProfile.zoning_use_constraints ?? ""}
                onChange={(e) =>
                  setFacilityProfile((prev) => ({
                    ...prev,
                    zoning_use_constraints: e.target.value,
                  }))
                }
                className="mt-1 w-full border border-gray-300 rounded px-2 py-1.5 text-sm"
              />
            </label>

            <label className="text-xs text-gray-600 md:col-span-2">
              Current layout notes
              <textarea
                value={facilityProfile.current_layout_notes ?? ""}
                onChange={(e) =>
                  setFacilityProfile((prev) => ({
                    ...prev,
                    current_layout_notes: e.target.value,
                  }))
                }
                rows={3}
                className="mt-1 w-full border border-gray-300 rounded px-2 py-1.5 text-sm"
              />
            </label>

            <label className="text-xs text-gray-600">
              Sponsor/operator capacity
              <select
                value={facilityProfile.sponsor_operator_capacity ?? "medium"}
                onChange={(e) =>
                  setFacilityProfile((prev) => ({
                    ...prev,
                    sponsor_operator_capacity: e.target.value,
                  }))
                }
                className="mt-1 w-full border border-gray-300 rounded px-2 py-1.5 text-sm bg-white"
              >
                <option value="low">Low</option>
                <option value="medium">Medium</option>
                <option value="high">High</option>
              </select>
            </label>
          </div>
        )}
      </div>

      <button
        type="submit"
        disabled={loading || !schoolName || !address}
        className="w-full flex items-center justify-center gap-2 bg-navy-700 hover:bg-navy-800 disabled:bg-gray-400 text-white font-semibold py-3 px-6 rounded-lg transition-colors text-sm"
      >
        {loading ? (
          <>
            <span className="animate-spin inline-block w-4 h-4 border-2 border-white border-t-transparent rounded-full" />
            Analyzing market...
          </>
        ) : (
          <>
            <Search className="w-4 h-4" />
            Run Ministry Assessment
          </>
        )}
      </button>

      {onSaveCurrent && (
        <button
          type="button"
          onClick={() => {
            const req = buildCurrentRequest();
            if (!req.school_name || !req.address) return;
            onSaveCurrent(req);
          }}
          className="w-full border border-gray-300 text-gray-700 font-semibold py-2.5 px-4 rounded-lg text-sm hover:bg-gray-50 transition-colors"
        >
          Save This Ministry Setup
        </button>
      )}
    </form>
  );
}
