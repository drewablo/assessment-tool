"use client";

import { ParameterBarField } from "@/lib/dashboard";

interface Props {
  driveTimeMinutes: number;
  address: string;
  primaryLabel: string;
  primaryValue: string;
  zipCount: number;
  secondaryLabel?: string;
  secondaryValue?: string;
  parameterFields?: ParameterBarField[];
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
  parameterFields = [],
}: Props) {
  const summaryFields: ParameterBarField[] = [
    { label: "Drive time", value: `${driveTimeMinutes} minutes` },
    { label: primaryLabel, value: primaryValue },
    { label: "ZIPs", value: `${zipCount} total ZIPs` },
    ...(secondaryLabel && secondaryValue ? [{ label: secondaryLabel, value: secondaryValue }] : []),
    ...parameterFields,
  ];

  return (
    <section className="rounded-[28px] border border-slate-200 bg-white/95 p-6 shadow-sm backdrop-blur">
      <div className="flex flex-col gap-5 xl:flex-row xl:items-start xl:justify-between">
        <div className="space-y-4">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-indigo-600">Current analysis</p>
          <div className="flex flex-wrap items-center gap-3 text-[30px] font-semibold tracking-tight text-slate-900">
            <span>Market view within a</span>
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
          <p className="text-sm text-slate-500">
            Dashboard settings are read-only here. Return to the analysis form to change assumptions and rerun.
          </p>
        </div>
      </div>

      <div className="mt-6 flex flex-wrap items-center gap-3 border-t border-slate-100 pt-5">
        {summaryFields.map((field) => (
          <StatChip key={`${field.label}-${field.value}`} label={field.label} value={field.value} />
        ))}
      </div>
    </section>
  );
}
