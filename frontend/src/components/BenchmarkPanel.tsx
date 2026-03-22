"use client";

import { BenchmarkPercentiles } from "@/lib/types";
import { BarChart3, TrendingUp } from "lucide-react";

interface Props {
  benchmarks: BenchmarkPercentiles;
  overallScore: number;
}

function PercentileBar({ label, percentile, sampleSize }: { label: string; percentile: number | null; sampleSize: number | null }) {
  if (percentile === null) return null;

  const color =
    percentile >= 75 ? "bg-green-500" :
    percentile >= 50 ? "bg-yellow-500" :
    percentile >= 25 ? "bg-orange-500" :
    "bg-red-500";

  return (
    <div className="space-y-1">
      <div className="flex justify-between items-baseline">
        <span className="text-xs font-medium text-slate-800">{label}</span>
        <span className="text-sm font-bold text-slate-950">{percentile.toFixed(0)}th</span>
      </div>
      <div className="h-2.5 bg-slate-100 rounded-full overflow-hidden relative">
        <div
          className={`h-full rounded-full transition-all duration-700 ${color}`}
          style={{ width: `${percentile}%` }}
        />
        {/* Median marker */}
        <div className="absolute top-0 bottom-0 w-px bg-slate-400" style={{ left: "50%" }} />
      </div>
      {sampleSize !== null && (
        <p className="text-[10px] text-slate-400">vs. {sampleSize.toLocaleString()} markets</p>
      )}
    </div>
  );
}

export default function BenchmarkPanel({ benchmarks, overallScore }: Props) {
  const hasAnyPercentile = benchmarks.percentile_national !== null || benchmarks.percentile_state !== null;

  if (!hasAnyPercentile) return null;

  return (
    <div className="bg-white rounded-[28px] border border-slate-200 shadow-sm p-6">
      <div className="flex items-center gap-2 mb-4">
        <BarChart3 className="w-4 h-4 text-indigo-500" />
        <h3 className="font-semibold text-slate-950">Benchmark Percentile Rankings</h3>
      </div>

      <p className="text-sm text-slate-500 mb-4">
        How this location&apos;s score of <strong>{overallScore}</strong> compares to other markets analyzed for the same ministry type.
      </p>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 mb-4">
        <PercentileBar
          label={benchmarks.state_name ? `State (${benchmarks.state_name})` : "State"}
          percentile={benchmarks.percentile_state}
          sampleSize={benchmarks.sample_size_state}
        />
        <PercentileBar
          label="National"
          percentile={benchmarks.percentile_national}
          sampleSize={benchmarks.sample_size_national}
        />
        <PercentileBar
          label={benchmarks.msa_name ? `Metro (${benchmarks.msa_name})` : "Metro Area"}
          percentile={benchmarks.percentile_msa}
          sampleSize={benchmarks.sample_size_msa}
        />
      </div>

      {/* Comparable Markets */}
      {benchmarks.comparable_markets.length > 0 && (
        <div className="mt-4 border-t border-slate-100 pt-4">
          <div className="flex items-center gap-2 mb-3">
            <TrendingUp className="w-3.5 h-3.5 text-slate-400" />
            <h4 className="text-xs font-semibold text-slate-800">Similar Market Profiles</h4>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-slate-500 border-b border-slate-100">
                  <th className="text-left py-1.5 pr-3 font-medium">Tract</th>
                  <th className="text-right py-1.5 px-2 font-medium">Score</th>
                  <th className="text-right py-1.5 px-2 font-medium">Market</th>
                  <th className="text-right py-1.5 px-2 font-medium">Income</th>
                  <th className="text-right py-1.5 px-2 font-medium">Comp</th>
                  <th className="text-right py-1.5 pl-2 font-medium">Family</th>
                </tr>
              </thead>
              <tbody>
                {benchmarks.comparable_markets.slice(0, 5).map((m) => (
                  <tr key={m.geoid} className="border-b border-slate-100 hover:bg-slate-50">
                    <td className="py-1.5 pr-3 text-slate-500 font-mono">{m.geoid}</td>
                    <td className="py-1.5 px-2 text-right font-bold text-slate-800">{m.overall_score}</td>
                    <td className="py-1.5 px-2 text-right text-slate-500">{m.market_size_score}</td>
                    <td className="py-1.5 px-2 text-right text-slate-500">{m.income_score}</td>
                    <td className="py-1.5 px-2 text-right text-slate-500">{m.competition_score}</td>
                    <td className="py-1.5 pl-2 text-right text-slate-500">{m.family_density_score}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="text-[10px] text-slate-400 mt-2">
            Markets with the most similar demographic and competitive profiles, ranked by factor-score similarity.
          </p>
        </div>
      )}
    </div>
  );
}
