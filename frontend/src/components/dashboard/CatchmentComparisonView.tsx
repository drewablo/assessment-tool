"use client";

import { memo, useMemo, useState } from "react";

interface GradeInput {
  grade: string;
  enrolled: number;
}

const DEFAULT_GRADES: GradeInput[] = [
  { grade: "K", enrolled: 0 },
  { grade: "1", enrolled: 0 },
  { grade: "2", enrolled: 0 },
  { grade: "3", enrolled: 0 },
  { grade: "4", enrolled: 0 },
  { grade: "5", enrolled: 0 },
  { grade: "6", enrolled: 0 },
  { grade: "7", enrolled: 0 },
  { grade: "8", enrolled: 0 },
  { grade: "9", enrolled: 0 },
  { grade: "10", enrolled: 0 },
  { grade: "11", enrolled: 0 },
  { grade: "12", enrolled: 0 },
];

interface Props {
  schoolAgePopulation?: number | null;
  estimatedCatholicSchoolAge?: number | null;
  estimatedCatholicPct?: number | null;
}

function CatchmentComparisonView({ schoolAgePopulation, estimatedCatholicSchoolAge, estimatedCatholicPct }: Props) {
  const [grades, setGrades] = useState<GradeInput[]>(DEFAULT_GRADES);
  const [submitted, setSubmitted] = useState(false);

  const totalEnrolled = useMemo(() => grades.reduce((sum, g) => sum + g.enrolled, 0), [grades]);

  const elementaryEnrolled = useMemo(() => grades.filter((g) => ["K", "1", "2", "3", "4", "5"].includes(g.grade)).reduce((sum, g) => sum + g.enrolled, 0), [grades]);
  const middleEnrolled = useMemo(() => grades.filter((g) => ["6", "7", "8"].includes(g.grade)).reduce((sum, g) => sum + g.enrolled, 0), [grades]);
  const highEnrolled = useMemo(() => grades.filter((g) => ["9", "10", "11", "12"].includes(g.grade)).reduce((sum, g) => sum + g.enrolled, 0), [grades]);

  function handleChange(index: number, value: string) {
    setGrades((prev) => prev.map((g, i) => (i === index ? { ...g, enrolled: Math.max(0, Number(value) || 0) } : g)));
  }

  const penetrationRate = estimatedCatholicSchoolAge && estimatedCatholicSchoolAge > 0 ? (totalEnrolled / estimatedCatholicSchoolAge) * 100 : null;

  return (
    <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
      <div className="mb-4">
        <h3 className="text-xl font-semibold tracking-tight text-slate-950">Catchment vs. Enrollment Comparison</h3>
        <p className="mt-1 text-sm text-slate-500">
          Enter your current enrollment by grade to compare against the catchment&apos;s school-age population.
        </p>
      </div>

      {!submitted ? (
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-7">
            {grades.map((g, i) => (
              <div key={g.grade}>
                <label className="mb-1 block text-xs font-medium text-slate-600">Grade {g.grade}</label>
                <input
                  type="number"
                  min={0}
                  value={g.enrolled || ""}
                  onChange={(e) => handleChange(i, e.target.value)}
                  placeholder="0"
                  className="w-full rounded-lg border border-slate-200 px-2.5 py-1.5 text-sm text-slate-900 shadow-sm focus:outline-none focus:ring-1 focus:ring-indigo-400"
                />
              </div>
            ))}
          </div>

          <div className="flex items-center justify-between">
            <p className="text-sm text-slate-600">
              Total enrollment: <span className="font-semibold">{totalEnrolled}</span>
            </p>
            <button
              type="button"
              onClick={() => { if (totalEnrolled > 0) setSubmitted(true); }}
              disabled={totalEnrolled === 0}
              className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-40"
            >
              Compare →
            </button>
          </div>
        </div>
      ) : (
        <div className="space-y-4">
          <button
            type="button"
            onClick={() => setSubmitted(false)}
            className="text-xs text-slate-500 underline underline-offset-2 hover:text-slate-800"
          >
            ← Edit enrollment
          </button>

          {/* Summary cards */}
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
              <p className="text-xs font-medium text-slate-500">Your Enrollment</p>
              <p className="text-lg font-bold text-slate-900">{totalEnrolled.toLocaleString()}</p>
            </div>
            <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
              <p className="text-xs font-medium text-slate-500">Catchment School-Age</p>
              <p className="text-lg font-bold text-slate-900">{(schoolAgePopulation ?? 0).toLocaleString()}</p>
            </div>
            <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
              <p className="text-xs font-medium text-slate-500">Est. Catholic School-Age</p>
              <p className="text-lg font-bold text-slate-900">{(estimatedCatholicSchoolAge ?? 0).toLocaleString()}</p>
            </div>
            <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3">
              <p className="text-xs font-medium text-slate-500">Market Penetration</p>
              <p className="text-lg font-bold text-slate-900">
                {penetrationRate != null ? `${penetrationRate.toFixed(1)}%` : "N/A"}
              </p>
            </div>
          </div>

          {/* Grade-band breakdown */}
          <div className="rounded-xl border border-slate-200 bg-white p-4">
            <p className="mb-3 text-sm font-semibold text-slate-700">Enrollment by Grade Band</p>
            <div className="space-y-2">
              {[
                { label: "Elementary (K–5)", enrolled: elementaryEnrolled },
                { label: "Middle (6–8)", enrolled: middleEnrolled },
                { label: "High (9–12)", enrolled: highEnrolled },
              ].map((band) => (
                <div key={band.label} className="flex items-center justify-between text-sm">
                  <span className="text-slate-600">{band.label}</span>
                  <div className="flex items-center gap-3">
                    <span className="font-semibold text-slate-900">{band.enrolled}</span>
                    <div className="h-2 w-24 overflow-hidden rounded-full bg-slate-100">
                      <div
                        className="h-full rounded-full bg-indigo-500"
                        style={{ width: `${totalEnrolled > 0 ? (band.enrolled / totalEnrolled) * 100 : 0}%` }}
                      />
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <p className="text-xs text-slate-400">
            Catchment demographics from Census ACS · Catholic affiliation is a directional estimate ({estimatedCatholicPct?.toFixed(1) ?? "N/A"}% state-level share).
          </p>
        </div>
      )}
    </div>
  );
}

export default memo(CatchmentComparisonView);
