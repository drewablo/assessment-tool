"use client";

import { HierarchicalScore, MetricScore, MinistryType, SubIndicator } from "@/lib/types";
import { Layers } from "lucide-react";

interface Props {
  hierarchical: HierarchicalScore;
  ministryType: MinistryType;
}

const PANEL_COPY: Record<MinistryType, { title: string; description: string }> = {
  schools: {
    title: "Hierarchical School Feasibility Breakdown",
    description:
      "Multi-level school-market decomposition showing pipeline demand, validation pressure, mission fit, and sustainability risk.",
  },
  housing: {
    title: "Hierarchical Housing Feasibility Breakdown",
    description:
      "Multi-level housing decomposition showing renter need, affordability pressure, supply pressure, neighborhood fit, and development viability.",
  },
  elder_care: {
    title: "Hierarchical Elder-Care Feasibility Breakdown",
    description:
      "Multi-level elder-care decomposition showing senior demand, care-market competition, community aging profile, and staffing/operational viability.",
  },
};

const ratingColors: Record<string, string> = {
  strong: "text-green-700 bg-green-50 border-green-200",
  moderate: "text-yellow-700 bg-yellow-50 border-yellow-200",
  weak: "text-orange-700 bg-orange-50 border-orange-200",
  poor: "text-red-700 bg-red-50 border-red-200",
};

const barColors: Record<string, string> = {
  strong: "bg-green-500",
  moderate: "bg-yellow-500",
  weak: "bg-orange-500",
  poor: "bg-red-500",
};

function SubIndicatorRow({ sub }: { sub: SubIndicator }) {
  const barColor =
    sub.score >= 75 ? "bg-green-400" :
    sub.score >= 55 ? "bg-yellow-400" :
    sub.score >= 35 ? "bg-orange-400" :
    "bg-red-400";

  return (
    <div className="flex items-center gap-3 py-1">
      <span className="text-[11px] text-gray-500 w-40 flex-shrink-0 truncate" title={sub.label}>
        {sub.label}
      </span>
      <div className="flex-1 h-1.5 bg-gray-100 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full ${barColor}`}
          style={{ width: `${sub.score}%` }}
        />
      </div>
      <span className="text-[11px] font-semibold text-gray-700 w-8 text-right">{sub.score}</span>
    </div>
  );
}

function IndexCard({ metric }: { metric: MetricScore }) {
  const colorClass = ratingColors[metric.rating] ?? ratingColors.weak;
  const barColor = barColors[metric.rating] ?? barColors.weak;

  return (
    <div className={`rounded-lg border p-4 ${colorClass}`}>
      <div className="flex items-center justify-between mb-2">
        <div>
          <h4 className="text-sm font-semibold">{metric.label}</h4>
          <p className="text-[10px] opacity-75">{metric.weight}% of overall</p>
        </div>
        <div className="text-2xl font-bold">{metric.score}</div>
      </div>
      <div className="h-2 bg-white/50 rounded-full overflow-hidden mb-3">
        <div
          className={`h-full rounded-full ${barColor}`}
          style={{ width: `${metric.score}%` }}
        />
      </div>
      {metric.sub_indicators && metric.sub_indicators.length > 0 && (
        <div className="space-y-0.5 border-t border-current/10 pt-2">
          {metric.sub_indicators.map((sub) => (
            <SubIndicatorRow key={sub.key} sub={sub} />
          ))}
        </div>
      )}
    </div>
  );
}

export default function HierarchicalScorePanel({ hierarchical, ministryType }: Props) {
  const indices = [
    hierarchical.market_opportunity,
    hierarchical.competitive_position,
    hierarchical.community_fit,
    hierarchical.sustainability_risk,
  ].filter((m): m is MetricScore => m !== null);

  if (indices.length === 0) return null;

  // Compute hierarchical overall
  const hOverall = Math.round(
    (hierarchical.market_opportunity?.score ?? 50) * 0.45
    + (hierarchical.competitive_position?.score ?? 50) * 0.30
    + (hierarchical.community_fit?.score ?? 50) * 0.15
    + (hierarchical.sustainability_risk?.score ?? 50) * 0.10
  );

  const panelCopy = PANEL_COPY[ministryType] ?? PANEL_COPY.schools;

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-5">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <Layers className="w-4 h-4 text-purple-500" />
          <h3 className="font-semibold text-gray-900">{panelCopy.title}</h3>
        </div>
        <div className="text-sm text-gray-500">
          Composite: <span className="font-bold text-gray-900">{hOverall}</span>/100
        </div>
      </div>

      <p className="text-xs text-gray-500 mb-4">
        {panelCopy.description}
      </p>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        {indices.map((metric) => (
          <IndexCard key={metric.label} metric={metric} />
        ))}
      </div>
    </div>
  );
}
