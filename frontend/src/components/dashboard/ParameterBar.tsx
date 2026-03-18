"use client";

import { Search } from "lucide-react";
import { ParameterBarField } from "@/lib/dashboard";

interface Props {
  driveTimeMinutes: number;
  address: string;
  primaryLabel: string;
  primaryValue: string;
  zipCount: number;
  secondaryLabel?: string;
  secondaryValue?: string;
  onRun?: () => void;
}

function StatChip({ label, value }: ParameterBarField) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white px-3 py-2 shadow-sm">
      <p className="text-[11px] uppercase tracking-[0.18em] text-slate-400">{label}</p>
      <p className="mt-1 text-sm font-semibold text-slate-900">{value}</p>
    </div>
  );
}

export default function ParameterBar({
  driveTimeMinutes,
  address,
  primaryLabel,
  primaryValue,
  zipCount,
  secondaryLabel,
  secondaryValue,
  onRun,
}: Props) {
  return (
    <section className="rounded-[28px] border border-slate-200 bg-white/95 p-6 shadow-sm backdrop-blur">
      <div className="flex flex-col gap-5 xl:flex-row xl:items-center xl:justify-between">
        <div className="space-y-4">
          <div className="flex flex-wrap items-center gap-3 text-[30px] font-semibold tracking-tight text-slate-900">
            <span>Show me data within a</span>
            <span className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-2 text-xl font-medium text-slate-700">
              {driveTimeMinutes}-minute
            </span>
            <span>drive of</span>
          </div>
          <div className="flex flex-wrap items-center gap-3 text-xl text-slate-800">
            <span className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 font-medium shadow-sm">
              {address}
            </span>
            <span>at a</span>
            <span className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 font-medium shadow-sm">
              {primaryValue}
            </span>
            <span>{primaryLabel}.</span>
          </div>
        </div>

        <button
          type="button"
          onClick={onRun}
          className="inline-flex items-center justify-center gap-2 rounded-xl bg-indigo-600 px-5 py-3 text-sm font-semibold text-white shadow-sm transition hover:bg-indigo-500"
        >
          <Search className="w-4 h-4" />
          Show me the trends
        </button>
      </div>

      <div className="mt-6 flex flex-wrap items-center gap-3 border-t border-slate-100 pt-5">
        <StatChip label="Drive time" value={`${driveTimeMinutes} minutes`} />
        <StatChip label="Metric" value={primaryValue} />
        <StatChip label="ZIPs in view" value={zipCount} />
        {secondaryLabel && secondaryValue ? <StatChip label={secondaryLabel} value={secondaryValue} /> : null}
      </div>
    </section>
  );
}
