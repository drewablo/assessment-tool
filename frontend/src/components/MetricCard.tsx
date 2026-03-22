"use client";

import { useState, useEffect, useRef } from "react";
import { Info } from "lucide-react";
import { MetricScore } from "@/lib/types";

interface Props {
  metric: MetricScore;
}

const ratingStyles: Record<string, { bar: string; badge: string; text: string }> = {
  strong: { bar: "bg-green-500", badge: "bg-green-100 text-green-800", text: "Strong" },
  moderate: { bar: "bg-yellow-500", badge: "bg-yellow-100 text-yellow-800", text: "Moderate" },
  weak: { bar: "bg-orange-500", badge: "bg-orange-100 text-orange-800", text: "Weak" },
  poor: { bar: "bg-red-500", badge: "bg-red-100 text-red-800", text: "Poor" },
};

const metricExplanations: Record<string, string> = {
  // Schools
  "Market Size": "The estimated number of Catholic school-age children within the catchment area, calibrated for urban, suburban, or rural markets. A higher score reflects a larger addressable student population and stronger enrollment potential.",
  "Income Level": "Median household income in the area, weighted by the share of households earning $100k or more. Higher incomes generally indicate greater tuition-paying capacity. State school choice programs (vouchers/ESAs) can reduce effective tuition burden and boost this score.",
  "Competition": "The number and proximity of existing Catholic and private schools. Lower nearby competitive pressure generally means less enrollment fragmentation and stronger sustainability conditions for continued school operation. Higher score = less competitive pressure.",
  "Family Density": "The share of households with children under 18. A family-dense community is more likely to generate demand for Catholic education. Higher density means a larger pool of families who are potential tuition-paying households.",

  // Elder care — market_size label is dynamic based on mission mode
  "Mission-Aligned Target Population": "The estimated number of vulnerable seniors (those living alone or below 200% of the poverty line) within the catchment area. In mission mode, this population is the primary measure of community need.",
  "Market Demand Target Population": "The estimated population of adults age 75+ within the catchment area. This age group has the highest likelihood of needing elder care services and represents the core addressable market.",
  "Bed Saturation": "The ratio of weighted competitor beds to the target senior population. A low saturation ratio means the market is underserved relative to demand. Higher score = less saturated, more opportunity.",
  "Isolation Signal": "The share of seniors living alone, used as a proxy for gaps in family support networks. Higher isolation rates indicate greater unmet need for professional care services.",
  "Income Fit": "Household income relative to the cost of elder care services. In market mode, higher incomes indicate stronger self-pay capacity. In mission mode, this is reversed — lower incomes signal greater need for affordable or subsidized care.",
  "Market Occupancy": "The weighted average occupancy rate across nearby elder care facilities. Higher occupancy among competitors suggests strong, proven demand. Lower occupancy may indicate market softness or oversaturation.",

  // Housing
  "Cost-Burdened Households": "The number of renter households spending more than 30% of their income on housing — the standard HUD threshold for being 'cost burdened.' A higher count signals greater unmet affordable housing need in the area.",
  "Income Need": "Median household income assessed for HUD eligibility. Scored inversely: lower incomes indicate higher need and yield a higher score, reflecting the housing ministry's focus on underserved communities.",
  "LIHTC Saturation": "The ratio of existing Low-Income Housing Tax Credit (LIHTC) units to cost-burdened households. Lower saturation means the affordable housing supply is insufficient relative to need. Higher score = more opportunity to fill the gap.",
  "Renter Burden Intensity": "The share of all renter households that are cost burdened. Higher intensity means a larger proportion of renters are struggling with housing costs, indicating stronger community-wide need.",
};

function InfoTooltip({ label }: { label: string }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const explanation = metricExplanations[label];

  useEffect(() => {
    if (!open) return;
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [open]);

  if (!explanation) return null;

  return (
    <div ref={ref} className="relative inline-flex items-center">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="ml-1 text-slate-400 hover:text-slate-600 transition-colors focus:outline-none"
        aria-label={`Info about ${label}`}
      >
        <Info size={13} />
      </button>
      {open && (
        <div className="absolute z-50 left-1/2 -translate-x-1/2 top-6 w-60 bg-slate-950 text-white text-xs rounded-lg shadow-lg p-3 leading-relaxed">
          <div className="absolute -top-1.5 left-1/2 -translate-x-1/2 w-3 h-3 bg-slate-950 rotate-45" />
          {explanation}
        </div>
      )}
    </div>
  );
}

export default function MetricCard({ metric }: Props) {
  const styles = ratingStyles[metric.rating] ?? ratingStyles.weak;

  return (
    <div className="bg-white rounded-[28px] border border-slate-200 p-5 shadow-sm">
      <div className="flex items-start justify-between mb-3">
        <div>
          <div className="flex items-center">
            <h3 className="font-semibold text-slate-950">{metric.label}</h3>
            <InfoTooltip label={metric.label} />
          </div>
          <p className="mt-0.5 text-xs text-slate-500">{metric.weight}% of overall score</p>
        </div>
        <span className={`text-xs font-semibold px-2 py-1 rounded-full ${styles.badge}`}>
          {styles.text}
        </span>
      </div>

      <div className="mb-3">
        <div className="flex justify-between text-xs text-slate-400 mb-1">
          <span>Score</span>
          <span className="font-bold text-slate-800">{metric.score}/100</span>
        </div>
        <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
          <div
            className={`h-full rounded-full transition-all duration-700 ${styles.bar}`}
            style={{ width: `${metric.score}%` }}
          />
        </div>
      </div>

      <p className="text-xs leading-relaxed text-slate-500">{metric.description}</p>
    </div>
  );
}
