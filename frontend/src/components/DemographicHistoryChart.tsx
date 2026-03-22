"use client";

import { DemographicHistoryPoint } from "@/lib/types";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

interface Props {
  series: DemographicHistoryPoint[];
  /** Optional 2022 current-year values to append as the most recent point. */
  current?: {
    school_age_population?: number | null;
    total_population?: number | null;
    median_household_income?: number | null;
    families_with_children?: number | null;
    total_households?: number | null;
  };
}

function fmt(n: number): string {
  return n >= 1_000_000
    ? `${(n / 1_000_000).toFixed(1)}M`
    : n >= 1_000
    ? `${(n / 1_000).toFixed(1)}k`
    : String(n);
}

export default function DemographicHistoryChart({ series, current }: Props) {
  // Merge historical series with the current-year point (2022).
  const points = [...series];
  if (current && !points.find((p) => p.year === 2022)) {
    points.push({
      year: 2022,
      school_age_population: current.school_age_population ?? null,
      total_population: current.total_population ?? null,
      median_household_income: current.median_household_income ?? null,
      families_with_children: current.families_with_children ?? null,
      total_households: current.total_households ?? null,
    });
  }
  points.sort((a, b) => a.year - b.year);

  if (points.length < 2) return null;

  const hasIncome = points.some((p) => p.median_household_income != null);
  const hasSchoolAge = points.some((p) => p.school_age_population != null);
  const hasFamilies = points.some((p) => p.families_with_children != null);

  return (
    <div className="rounded-xl border border-gray-200 bg-white p-5">
      <div className="mb-4">
        <h2 className="text-sm font-bold text-gray-900">Catchment Demographics — Historical Trend</h2>
        <p className="text-xs text-gray-500 mt-0.5">
          ACS 5-year estimates aggregated across catchment tracts ·{" "}
          {points.map((p) => p.year).join(", ")}
        </p>
      </div>

      {hasSchoolAge && (
        <div className="mb-6">
          <p className="text-xs font-semibold text-gray-700 mb-2">School-Age Population (5–17) &amp; Family Households with Children</p>
          <ResponsiveContainer width="100%" height={180}>
            <LineChart data={points} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="year" tick={{ fontSize: 11 }} />
              <YAxis tickFormatter={fmt} tick={{ fontSize: 11 }} width={48} />
              <Tooltip
                formatter={(v: number, name: string) => [fmt(v), name]}
                labelFormatter={(l) => `ACS ${l}`}
              />
              <Legend iconSize={10} wrapperStyle={{ fontSize: 11 }} />
              <Line
                type="monotone"
                dataKey="school_age_population"
                name="Ages 5–17"
                stroke="#3b82f6"
                strokeWidth={2}
                dot={{ r: 3 }}
                connectNulls
              />
              {hasFamilies && (
                <Line
                  type="monotone"
                  dataKey="families_with_children"
                  name="Families w/ Children"
                  stroke="#10b981"
                  strokeWidth={2}
                  dot={{ r: 3 }}
                  connectNulls
                />
              )}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {hasIncome && (
        <div>
          <p className="text-xs font-semibold text-gray-700 mb-2">Median Household Income (nominal $)</p>
          <ResponsiveContainer width="100%" height={150}>
            <LineChart data={points} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="year" tick={{ fontSize: 11 }} />
              <YAxis
                tickFormatter={(v) => `$${fmt(v)}`}
                tick={{ fontSize: 11 }}
                width={56}
              />
              <Tooltip
                formatter={(v: number) => [`$${v.toLocaleString()}`, "Median HH Income"]}
                labelFormatter={(l) => `ACS ${l}`}
              />
              <Line
                type="monotone"
                dataKey="median_household_income"
                name="Median HH Income"
                stroke="#f59e0b"
                strokeWidth={2}
                dot={{ r: 3 }}
                connectNulls
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      <p className="text-xs text-gray-400 mt-3">
        Values are tract-level sums/weighted averages for the school catchment area. Income is nominal (not inflation-adjusted).
      </p>
    </div>
  );
}
