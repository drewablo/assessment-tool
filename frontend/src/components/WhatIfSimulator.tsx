"use client";

import { useState } from "react";
import { AnalysisResponse } from "@/lib/types";

interface Props {
  result: AnalysisResponse;
}

function fmt$(n: number): string {
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(n);
}

// ---------------------------------------------------------------------------
// SliderRow
// ---------------------------------------------------------------------------
function SliderRow({
  label,
  value,
  min,
  max,
  step,
  onChange,
  display,
}: {
  label: string;
  value: number;
  min: number;
  max: number;
  step: number;
  onChange: (n: number) => void;
  display: string;
}) {
  return (
    <div className="space-y-1">
      <div className="flex justify-between text-xs">
        <span className="text-gray-600">{label}</span>
        <span className="font-semibold text-gray-800">{display}</span>
      </div>
      <input
        type="range"
        min={min}
        max={max}
        step={step}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="w-full accent-gray-800 cursor-pointer"
      />
      <div className="flex justify-between text-xs text-gray-400">
        <span>{min.toLocaleString()}</span>
        <span>{max.toLocaleString()}</span>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// ResultLine
// ---------------------------------------------------------------------------
function ResultLine({
  label,
  value,
  bold = false,
  positive = true,
  isTotal = false,
}: {
  label: string;
  value: number;
  bold?: boolean;
  positive?: boolean;
  isTotal?: boolean;
}) {
  const color =
    isTotal
      ? value >= 0
        ? "text-green-700"
        : "text-red-700"
      : bold
      ? "text-gray-900"
      : "text-gray-700";

  return (
    <div
      className={`flex justify-between py-1.5 ${isTotal ? "border-t border-gray-200 mt-1 pt-2.5" : ""}`}
    >
      <span className={`text-xs ${bold || isTotal ? "font-semibold" : ""} ${isTotal ? color : "text-gray-600"}`}>
        {label}
      </span>
      <span className={`text-xs font-semibold tabular-nums ${color}`}>
        {isTotal && value < 0 ? `(${fmt$(Math.abs(value))})` : fmt$(Math.abs(value))}
        {!positive && value !== 0 ? " deficit" : isTotal && value >= 0 ? " surplus" : ""}
      </span>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Per-pupil cost curve derived from local PSS enrollment data.
// Smaller schools have higher fixed-cost overhead per student.
// Breakpoints calibrated to NCES Catholic school expenditure data.
// ---------------------------------------------------------------------------
function derivePerPupilCost(medianEnrollment: number | null): { cost: number; hint: string } {
  if (medianEnrollment == null) {
    return { cost: 10_000, hint: "NCES national avg. Catholic school (~$10k; no local enrollment data)" };
  }
  // Cost curve: larger schools benefit from economies of scale
  const cost =
    medianEnrollment < 75  ? 13_500 :
    medianEnrollment < 150 ? 12_000 :
    medianEnrollment < 250 ? 10_500 :
    medianEnrollment < 400 ? 9_500  :
    medianEnrollment < 600 ? 8_750  : 8_000;
  return {
    cost,
    hint: `estimated from median local Catholic school enrollment (${medianEnrollment} students) · NCES cost curve`,
  };
}

// ---------------------------------------------------------------------------
// Defaults seeded from actual market data
// ---------------------------------------------------------------------------
function getDefaults(result: AnalysisResponse) {
  const income = result.demographics.median_household_income ?? 75000;
  const mktScore = result.feasibility_score.overall;

  const catholicWithEnrollment = result.competitor_schools.filter(
    (s) => s.is_catholic && s.enrollment != null && s.enrollment > 0
  );

  // Median local Catholic school enrollment (for per-pupil cost estimation)
  const sorted = [...catholicWithEnrollment].sort((a, b) => (a.enrollment ?? 0) - (b.enrollment ?? 0));
  const medianEnrollment =
    sorted.length > 0
      ? sorted[Math.floor(sorted.length / 2)].enrollment ?? null
      : null;

  let defaultEnrollment: number;
  if (catholicWithEnrollment.length > 0) {
    const avg =
      catholicWithEnrollment.reduce((sum, s) => sum + (s.enrollment ?? 0), 0) /
      catholicWithEnrollment.length;
    defaultEnrollment = Math.round(avg / 10) * 10;
  } else {
    defaultEnrollment =
      mktScore >= 75 ? 350 : mktScore >= 55 ? 250 : mktScore >= 35 ? 150 : 100;
  }
  defaultEnrollment = Math.min(800, Math.max(50, defaultEnrollment));

  const defaultTuition =
    income >= 120000 ? 14000 :
    income >= 90000  ? 12000 :
    income >= 65000  ? 10000 :
    income >= 45000  ? 8000  : 7000;

  const { cost: perPupilCost, hint: perPupilCostHint } = derivePerPupilCost(medianEnrollment);

  return {
    enrollment: defaultEnrollment,
    tuition: defaultTuition,
    aidPct: 20,
    parishSubsidy: 0,
    perPupilCost,
    enrollmentHint:
      catholicWithEnrollment.length > 0
        ? `avg. of ${catholicWithEnrollment.length} nearby Catholic school${catholicWithEnrollment.length > 1 ? "s" : ""}`
        : "estimated from feasibility score",
    tuitionHint: `estimated from local median income (${fmt$(income)})`,
    perPupilCostHint,
  };
}

// ---------------------------------------------------------------------------
// NumberField — labeled input used in the setup form
// ---------------------------------------------------------------------------
function NumberField({
  label,
  hint,
  value,
  onChange,
  prefix,
  suffix,
  min,
  max,
  step,
}: {
  label: string;
  hint?: string;
  value: string;
  onChange: (v: string) => void;
  prefix?: string;
  suffix?: string;
  min?: number;
  max?: number;
  step?: number;
}) {
  return (
    <div>
      <label className="block text-xs font-medium text-gray-700 mb-1">{label}</label>
      <div className="flex items-center border border-gray-300 rounded-lg overflow-hidden focus-within:ring-2 focus-within:ring-gray-400 bg-white">
        {prefix && (
          <span className="px-2.5 py-2 text-xs text-gray-500 bg-gray-50 border-r border-gray-300 select-none">
            {prefix}
          </span>
        )}
        <input
          type="number"
          min={min}
          max={max}
          step={step}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className="flex-1 px-2.5 py-2 text-sm text-gray-900 outline-none bg-white"
        />
        {suffix && (
          <span className="px-2.5 py-2 text-xs text-gray-500 bg-gray-50 border-l border-gray-300 select-none">
            {suffix}
          </span>
        )}
      </div>
      {hint && <p className="mt-0.5 text-xs text-gray-400 leading-snug">{hint}</p>}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Piecewise-linear interpolation (mirrors backend scoring approach)
// ---------------------------------------------------------------------------
function piecewise(x: number, segments: [number, number][]): number {
  if (x <= segments[0][0]) return segments[0][1];
  if (x >= segments[segments.length - 1][0]) return segments[segments.length - 1][1];
  for (let i = 0; i < segments.length - 1; i++) {
    const [x0, y0] = segments[i];
    const [x1, y1] = segments[i + 1];
    if (x >= x0 && x <= x1) return y0 + ((x - x0) / (x1 - x0)) * (y1 - y0);
  }
  return segments[segments.length - 1][1];
}

// ---------------------------------------------------------------------------
// Financial Viability Card (Option B)
// Scored from What-If inputs + NCEA 2024-2025 enrollment benchmarks.
// Penetration score (40%) + Margin score (60%) → overall viability rating.
// ---------------------------------------------------------------------------
function FinancialViabilityCard({
  enrollment,
  netTuitionPerStudent,
  perPupilCost,
  estimatedCatholicSchoolAge,
}: {
  enrollment: number;
  netTuitionPerStudent: number;
  perPupilCost: number;
  estimatedCatholicSchoolAge: number | null;
}) {
  // NCEA 2024-2025 Exhibit 6: % of Catholic schools by enrollment size
  const nceaScale =
    enrollment < 50   ? "Below 50 students (5.5% of Catholic schools)"
    : enrollment < 150 ? "50–149 students · microschool range (26.0% of schools)"
    : enrollment < 300 ? "150–299 students · modal Catholic school (38.6% of schools)"
    : enrollment < 500 ? "300–499 students · established school (18.3% of schools)"
    : enrollment < 750 ? "500–749 students · large school (6.8% of schools)"
    :                    "750+ students · very large school (4.7% of schools)";

  // Margin score — based on how large the per-student cost gap is
  const gapRatio =
    perPupilCost > 0
      ? Math.max(0, (perPupilCost - netTuitionPerStudent) / perPupilCost)
      : 0;
  const marginScore =
    netTuitionPerStudent >= perPupilCost
      ? 95
      : piecewise(gapRatio, [
          [0, 90], [0.10, 75], [0.20, 58], [0.35, 38], [0.50, 20], [0.75, 5],
        ]);

  // Penetration score — what share of the Catholic market must be captured?
  const hasPenetration =
    estimatedCatholicSchoolAge != null && estimatedCatholicSchoolAge > 0;
  const penetrationRate =
    hasPenetration ? (enrollment / estimatedCatholicSchoolAge!) * 100 : null;
  const penetrationScore =
    penetrationRate != null
      ? piecewise(penetrationRate, [
          [0, 90], [3, 85], [8, 72], [15, 55], [25, 35], [40, 15],
        ])
      : null;

  // Overall: penetration (40%) + margin (60%) when market data is available
  const viabilityScore =
    penetrationScore != null
      ? Math.round(0.4 * penetrationScore + 0.6 * marginScore)
      : Math.round(marginScore);

  const rating =
    viabilityScore >= 75
      ? { label: "Financially Strong",    bg: "bg-green-50",  border: "border-green-200",  badge: "text-green-800"  }
      : viabilityScore >= 55
      ? { label: "Financially Viable",    bg: "bg-yellow-50", border: "border-yellow-200", badge: "text-yellow-800" }
      : viabilityScore >= 35
      ? { label: "Financially Challenged", bg: "bg-orange-50", border: "border-orange-200", badge: "text-orange-800" }
      : { label: "Financial Risk",         bg: "bg-red-50",    border: "border-red-200",    badge: "text-red-800"   };

  const penetrationLabel =
    penetrationRate == null       ? ""
    : penetrationRate <= 5        ? "very achievable"
    : penetrationRate <= 10       ? "achievable"
    : penetrationRate <= 18       ? "realistic stretch"
    : penetrationRate <= 28       ? "ambitious"
    :                               "very aggressive";

  return (
    <div className={`rounded-xl border p-5 ${rating.bg} ${rating.border}`}>
      <div className="flex items-start justify-between mb-4">
        <div>
          <p className="text-sm font-bold text-gray-900">Financial Viability Assessment</p>
          <p className="text-xs text-gray-500 mt-0.5">
            Scored from your scenario · enrollment benchmarks from NCEA 2024-2025
          </p>
        </div>
        <span className={`text-xs font-bold px-3 py-1 rounded-full border ${rating.bg} ${rating.badge} ${rating.border} whitespace-nowrap ml-3`}>
          {rating.label}
        </span>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        {/* NCEA Scale */}
        <div className="bg-white rounded-lg p-3">
          <p className="text-xs font-semibold text-gray-600 mb-1">Enrollment Scale</p>
          <p className="text-sm font-bold text-gray-900">{enrollment} students</p>
          <p className="text-xs text-gray-500 mt-0.5 leading-snug">{nceaScale}</p>
        </div>

        {/* Market Penetration */}
        <div className="bg-white rounded-lg p-3">
          <p className="text-xs font-semibold text-gray-600 mb-1">Market Penetration</p>
          {penetrationRate != null ? (
            <>
              <p className="text-sm font-bold text-gray-900">{penetrationRate.toFixed(1)}%</p>
              <p className="text-xs text-gray-500 mt-0.5 leading-snug">
                of {estimatedCatholicSchoolAge!.toLocaleString()} est. Catholic school-age pop · {penetrationLabel}
              </p>
            </>
          ) : (
            <p className="text-xs text-gray-400 mt-1">Market size unavailable for this analysis</p>
          )}
        </div>

        {/* Per-Student Margin */}
        <div className="bg-white rounded-lg p-3">
          <p className="text-xs font-semibold text-gray-600 mb-1">Per-Student Margin</p>
          {netTuitionPerStudent >= perPupilCost ? (
            <>
              <p className="text-sm font-bold text-green-700">
                +{fmt$(netTuitionPerStudent - perPupilCost)}/student
              </p>
              <p className="text-xs text-gray-500 mt-0.5">Self-sustaining per enrollment</p>
            </>
          ) : (
            <>
              <p className="text-sm font-bold text-red-700">
                −{fmt$(perPupilCost - netTuitionPerStudent)}/student
              </p>
              <p className="text-xs text-gray-500 mt-0.5 leading-snug">
                {Math.round(gapRatio * 100)}% cost gap · subsidy or scale required
              </p>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------
interface FormValues {
  enrollment: string;
  tuition: string;
  aidPct: string;
  parishSubsidy: string;
  perPupilCost: string;
}

export default function WhatIfSimulator({ result }: Props) {
  const defaults = getDefaults(result);

  // Phase 1: setup form
  const [form, setForm] = useState<FormValues>({
    enrollment:    String(defaults.enrollment),
    tuition:       String(defaults.tuition),
    aidPct:        String(defaults.aidPct),
    parishSubsidy: String(defaults.parishSubsidy),
    perPupilCost:  String(defaults.perPupilCost),
  });

  // Phase 2: slider state (populated on submit)
  const [phase, setPhase] = useState<"setup" | "simulate">("setup");
  const [enrollment, setEnrollment]       = useState(defaults.enrollment);
  const [tuition, setTuition]             = useState(defaults.tuition);
  const [aidPct, setAidPct]               = useState(defaults.aidPct);
  const [parishSubsidy, setParishSubsidy] = useState(defaults.parishSubsidy);
  const [perPupilCost, setPerPupilCost]   = useState(defaults.perPupilCost);

  function field(key: keyof FormValues) {
    return (v: string) => setForm((f) => ({ ...f, [key]: v }));
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const e_ = Math.min(800, Math.max(50,    Number(form.enrollment)    || defaults.enrollment));
    const t_  = Math.min(24000, Math.max(4000, Number(form.tuition)       || defaults.tuition));
    const a_  = Math.min(60,   Math.max(0,     Number(form.aidPct)        || 0));
    const s_  = Math.min(500000, Math.max(0,   Number(form.parishSubsidy) || 0));
    const p_  = Math.min(20000, Math.max(6000, Number(form.perPupilCost)  || defaults.perPupilCost));

    setEnrollment(e_);
    setTuition(t_);
    setAidPct(a_);
    setParishSubsidy(s_);
    setPerPupilCost(p_);
    setPhase("simulate");
  }

  function handleEdit() {
    // Sync form back to current slider state so nothing is lost
    setForm({
      enrollment:    String(enrollment),
      tuition:       String(tuition),
      aidPct:        String(aidPct),
      parishSubsidy: String(parishSubsidy),
      perPupilCost:  String(perPupilCost),
    });
    setPhase("setup");
  }

  // P&L (only used in simulate phase)
  const grossTuition = enrollment * tuition;
  const aidBurden = Math.round(grossTuition * aidPct / 100);
  const netTuition = grossTuition - aidBurden;
  const totalRevenue = netTuition + parishSubsidy;
  const totalCost = enrollment * perPupilCost;
  const surplus = totalRevenue - totalCost;
  const netTuitionPerStudent = tuition * (1 - aidPct / 100);
  const breakEvenWithSubsidy =
    perPupilCost > netTuitionPerStudent && parishSubsidy > 0
      ? Math.ceil(parishSubsidy / (perPupilCost - netTuitionPerStudent))
      : null;

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6">
      {/* Header */}
      <div className="flex items-start justify-between mb-5">
        <div>
          <h2 className="text-lg font-bold text-gray-900">What-If Financial Model</h2>
          <p className="text-xs text-gray-400 mt-0.5">
            {phase === "setup"
              ? "Enter your current figures, then run the model to explore scenarios with sliders."
              : "Adjust sliders to explore scenarios · figures seeded from your inputs"}
          </p>
        </div>
        {phase === "simulate" && (
          <button
            onClick={handleEdit}
            className="text-xs text-gray-500 hover:text-gray-800 underline underline-offset-2 ml-4 shrink-0"
          >
            ← Edit inputs
          </button>
        )}
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* PHASE 1 — Setup form                                                */}
      {/* ------------------------------------------------------------------ */}
      {phase === "setup" && (
        <form onSubmit={handleSubmit} className="space-y-5">
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <NumberField
              label="Target Enrollment"
              hint={defaults.enrollmentHint}
              value={form.enrollment}
              onChange={field("enrollment")}
              suffix="students"
              min={50}
              max={800}
              step={1}
            />
            <NumberField
              label="Annual Tuition per Student"
              hint={defaults.tuitionHint}
              value={form.tuition}
              onChange={field("tuition")}
              prefix="$"
              min={4000}
              max={24000}
              step={100}
            />
            <NumberField
              label="Tuition Aid / Discount Rate"
              hint="% of gross tuition awarded as financial aid"
              value={form.aidPct}
              onChange={field("aidPct")}
              suffix="%"
              min={0}
              max={60}
              step={1}
            />
            <NumberField
              label="Annual Institutional Subsidy"
              hint="Parish, diocese, or endowment support"
              value={form.parishSubsidy}
              onChange={field("parishSubsidy")}
              prefix="$"
              min={0}
              max={500000}
              step={1000}
            />
            <NumberField
              label="Per-Pupil Operating Cost"
              hint={defaults.perPupilCostHint}
              value={form.perPupilCost}
              onChange={field("perPupilCost")}
              prefix="$"
              min={6000}
              max={20000}
              step={100}
            />
          </div>

          <div className="flex items-center gap-3 pt-1">
            <button
              type="submit"
              className="px-5 py-2 bg-gray-900 text-white text-sm font-medium rounded-lg hover:bg-gray-700 transition-colors"
            >
              Run Model →
            </button>
            <p className="text-xs text-gray-400">
              Figures are pre-filled from market data — adjust as needed
            </p>
          </div>
        </form>
      )}

      {/* ------------------------------------------------------------------ */}
      {/* PHASE 2 — Sliders + P&L                                             */}
      {/* ------------------------------------------------------------------ */}
      {phase === "simulate" && (
        <>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
          {/* Sliders */}
          <div className="space-y-5">
            <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Adjust Inputs</p>

            <SliderRow
              label="Target Enrollment"
              value={enrollment}
              min={50}
              max={800}
              step={10}
              onChange={setEnrollment}
              display={`${enrollment} students`}
            />

            <SliderRow
              label="Annual Tuition per Student"
              value={tuition}
              min={4000}
              max={24000}
              step={500}
              onChange={setTuition}
              display={fmt$(tuition)}
            />

            <SliderRow
              label="Tuition Aid / Discount Rate"
              value={aidPct}
              min={0}
              max={60}
              step={1}
              onChange={setAidPct}
              display={`${aidPct}%`}
            />

            <SliderRow
              label="Annual Institutional Subsidy"
              value={parishSubsidy}
              min={0}
              max={500000}
              step={10000}
              onChange={setParishSubsidy}
              display={fmt$(parishSubsidy)}
            />

            <SliderRow
              label="Per-Pupil Operating Cost"
              value={perPupilCost}
              min={6000}
              max={20000}
              step={500}
              onChange={setPerPupilCost}
              display={fmt$(perPupilCost)}
            />
          </div>

          {/* P&L */}
          <div>
            <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-4">
              Projected Annual P&L
            </p>

            <div className="bg-gray-50 rounded-lg p-4 divide-y divide-gray-100">
              <ResultLine label="Gross Tuition Revenue" value={grossTuition} />
              <ResultLine label="− Tuition Aid Burden" value={-aidBurden} />
              <ResultLine label="Net Tuition Revenue" value={netTuition} bold />
              {parishSubsidy > 0 && (
                <ResultLine label="+ Institutional Subsidy" value={parishSubsidy} />
              )}
              <ResultLine label="= Total Revenue" value={totalRevenue} bold />
              <ResultLine label="− Estimated Operating Costs" value={-totalCost} />
              <ResultLine
                label={surplus >= 0 ? "Annual Surplus" : "Annual Deficit"}
                value={surplus}
                isTotal
              />
            </div>

            <div className="mt-4 space-y-2">
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs font-semibold text-gray-700 mb-1">Net Tuition per Student</p>
                <p className="text-sm font-bold text-gray-900">{fmt$(netTuitionPerStudent)}</p>
                <p className="text-xs text-gray-500 mt-0.5">
                  vs. {fmt$(perPupilCost)} per-pupil cost
                  {netTuitionPerStudent >= perPupilCost
                    ? " · net positive per student"
                    : ` · ${fmt$(perPupilCost - netTuitionPerStudent)} gap per student`}
                </p>
              </div>

              {breakEvenWithSubsidy !== null && (
                <div className="bg-amber-50 border border-amber-200 rounded-lg p-3">
                  <p className="text-xs font-semibold text-amber-800">Break-Even Enrollment</p>
                  <p className="text-sm font-bold text-amber-900">~{breakEvenWithSubsidy} students</p>
                  <p className="text-xs text-amber-700 mt-0.5">
                    Minimum enrollment for subsidy to cover the per-student cost gap
                  </p>
                </div>
              )}

              {surplus < 0 && parishSubsidy === 0 && (
                <div className="bg-red-50 border border-red-200 rounded-lg p-3">
                  <p className="text-xs font-semibold text-red-800">
                    Institutional subsidy needed to break even
                  </p>
                  <p className="text-sm font-bold text-red-900">{fmt$(Math.abs(surplus))}/yr</p>
                  <p className="text-xs text-red-700 mt-0.5">
                    Or reduce per-pupil cost, raise tuition, or increase enrollment
                  </p>
                </div>
              )}
            </div>

            <p className="text-xs text-gray-400 mt-4 leading-relaxed">
              Per-pupil cost is estimated from the median enrollment of nearby Catholic schools
              using an NCES-calibrated cost curve (smaller schools have higher overhead per student).
              All figures are rough planning estimates; actual costs vary by grade span, facility
              ownership, staffing model, and diocese.
            </p>
          </div>
        </div>

        {/* Financial Viability Assessment */}
        <div className="mt-6">
          <FinancialViabilityCard
            enrollment={enrollment}
            netTuitionPerStudent={netTuitionPerStudent}
            perPupilCost={perPupilCost}
            estimatedCatholicSchoolAge={result.demographics.estimated_catholic_school_age}
          />
        </div>
        </>
      )}
    </div>
  );
}
