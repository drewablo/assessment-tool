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
  const allFields: ParameterBarField[] = [
    { label: "Address", value: address },
    { label: "Drive time", value: `${driveTimeMinutes} min` },
    { label: primaryLabel, value: primaryValue },
    { label: "ZIPs", value: String(zipCount) },
    ...(secondaryLabel && secondaryValue ? [{ label: secondaryLabel, value: secondaryValue }] : []),
    ...parameterFields,
  ];

  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-2 rounded-2xl border border-slate-200 bg-white px-4 py-2.5 shadow-sm">
      {allFields.map((field) => (
        <div key={`${field.label}-${field.value}`} className="flex items-center gap-1.5 text-sm">
          <span className="text-slate-400">{field.label}</span>
          <span className="font-semibold text-slate-800">{field.value}</span>
        </div>
      ))}
    </div>
  );
}
