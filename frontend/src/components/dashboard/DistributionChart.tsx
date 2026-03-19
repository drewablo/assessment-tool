"use client";

import { memo, useRef } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import ChartActionBar from "./ChartActionBar";
import { DashboardDistributionBucket, DashboardReferenceLine, downloadCsv, downloadElementAsPng, formatDashboardValue } from "@/lib/dashboard";

interface Props {
  title: string;
  subtitle?: string;
  data: DashboardDistributionBucket[];
  primaryLabel?: string;
  comparisonLabel?: string;
  primaryColor?: string;
  comparisonColor?: string;
  referenceLine?: DashboardReferenceLine;
  fileBaseName?: string;
}

function DistributionChart({
  title,
  subtitle,
  data,
  primaryLabel = "Current",
  comparisonLabel = "Projected",
  primaryColor = "#1d4ed8",
  comparisonColor = "#16a34a",
  referenceLine,
  fileBaseName = "distribution-chart",
}: Props) {
  const ref = useRef<HTMLDivElement>(null);

  return (
    <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
      <div className="mb-6 flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <h3 className="text-2xl font-semibold tracking-tight text-slate-950">{title}</h3>
          {subtitle ? <p className="mt-1 text-sm text-slate-500">{subtitle}</p> : null}
        </div>
        <ChartActionBar
          onDownloadPng={() => downloadElementAsPng(`${fileBaseName}.png`, ref.current)}
          onDownloadCsv={() => downloadCsv(`${fileBaseName}.csv`, data)}
        />
      </div>

      <div ref={ref} className="h-[360px] w-full">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} layout="vertical" margin={{ top: 8, right: 16, left: 20, bottom: 0 }}>
            <CartesianGrid stroke="#e2e8f0" horizontal={false} />
            <XAxis type="number" tick={{ fill: "#64748b", fontSize: 12 }} stroke="#cbd5e1" />
            <YAxis type="category" dataKey="bucket" width={110} tick={{ fill: "#64748b", fontSize: 12 }} stroke="#cbd5e1" />
            <Tooltip formatter={(value: number) => formatDashboardValue(value, "number")} />
            <Legend />
            {referenceLine ? (
              <ReferenceLine
                x={referenceLine.value}
                stroke={referenceLine.color ?? "#f59e0b"}
                strokeDasharray="4 4"
                label={{
                  value: referenceLine.label,
                  position: "insideTopRight",
                  fill: referenceLine.color ?? "#b45309",
                  fontSize: 12,
                }}
              />
            ) : null}
            <Bar dataKey="primary" name={primaryLabel} fill={primaryColor} radius={[0, 8, 8, 0]} />
            <Bar dataKey="comparison" name={comparisonLabel} fill={comparisonColor} radius={[0, 8, 8, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

export default memo(DistributionChart);
