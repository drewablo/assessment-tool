"use client";

import { DemographicTrend } from "@/lib/types";
import { TrendingUp, TrendingDown, Minus } from "lucide-react";

interface Props {
  trend: DemographicTrend;
}

const trendColors: Record<string, { bg: string; border: string; badge: string; text: string }> = {
  Growing:  { bg: "bg-green-50",  border: "border-green-200",  badge: "bg-green-100 text-green-800",  text: "text-green-700"  },
  Stable:   { bg: "bg-blue-50",   border: "border-blue-200",   badge: "bg-blue-100 text-blue-800",    text: "text-blue-700"   },
  Declining:{ bg: "bg-red-50",    border: "border-red-200",    badge: "bg-red-100 text-red-800",      text: "text-red-700"    },
  Mixed:    { bg: "bg-yellow-50", border: "border-yellow-200", badge: "bg-yellow-100 text-yellow-800",text: "text-yellow-700" },
  Unknown:  { bg: "bg-gray-50",   border: "border-gray-200",   badge: "bg-gray-100 text-gray-600",    text: "text-gray-500"   },
};

const trendInterpretations: Record<string, string> = {
  Growing:   "Positive trajectory — growing school-age population and/or rising income levels strengthen the long-term market case.",
  Stable:    "The demographic composition of this area has remained broadly stable over the past 5 years.",
  Declining: "School-age population and/or real household income have declined — market conditions may continue to tighten.",
  Mixed:     "Signals are mixed: population and income trends point in different directions. Investigate the underlying causes.",
  Unknown:   "Trend data unavailable for this area.",
};

function TrendRow({
  label,
  value,
  positive_is_good = true,
}: {
  label: string;
  value: number | null;
  positive_is_good?: boolean;
}) {
  if (value === null) return null;

  const isPositive = value > 0;
  const isNeutral = Math.abs(value) < 1.5;
  const isGood = isNeutral ? null : positive_is_good ? isPositive : !isPositive;

  const color = isNeutral
    ? "text-gray-500"
    : isGood
    ? "text-green-600"
    : "text-red-600";

  const Icon = isNeutral ? Minus : isPositive ? TrendingUp : TrendingDown;

  return (
    <div className="flex items-center justify-between py-2 border-b border-gray-100 last:border-0">
      <span className="text-xs text-gray-600">{label}</span>
      <span className={`flex items-center gap-1 text-xs font-semibold ${color}`}>
        <Icon className="w-3 h-3" />
        {value > 0 ? "+" : ""}
        {value.toFixed(1)}%
      </span>
    </div>
  );
}

export default function TrendPanel({ trend }: Props) {
  const colors = trendColors[trend.trend_label] ?? trendColors.Unknown;
  const interpretation = trendInterpretations[trend.trend_label] ?? trendInterpretations.Unknown;

  return (
    <div className={`rounded-xl border p-5 ${colors.bg} ${colors.border}`}>
      <div className="flex items-start justify-between mb-3">
        <div>
          <h2 className="text-sm font-bold text-gray-900">Demographic Trends</h2>
          <p className="text-xs text-gray-500 mt-0.5">{trend.period}</p>
        </div>
        <span className={`text-xs font-semibold px-2.5 py-1 rounded-full ${colors.badge}`}>
          {trend.trend_label}
        </span>
      </div>

      <p className={`text-xs mb-4 leading-relaxed ${colors.text}`}>{interpretation}</p>

      <div className="bg-white/70 rounded-lg px-3 divide-y divide-gray-100">
        <TrendRow label="School-Age Population (ages 5–17)" value={trend.school_age_pop_pct} />
        <TrendRow label="Median Household Income (real, inflation-adjusted)" value={trend.income_real_pct} />
        <TrendRow label="Family Households with Children" value={trend.families_pct} />
      </div>

      <p className="text-xs text-gray-400 mt-3">
        Compares ACS 2017 5-year and ACS 2022 5-year county-level estimates. Income change
        adjusted for ~19% cumulative CPI-U inflation (2017 → 2022).
      </p>
    </div>
  );
}
