"use client";

import { memo, useMemo, useRef, useState } from "react";
import { ChevronDown, ChevronUp } from "lucide-react";
import ChartActionBar from "./ChartActionBar";
import {
  ZipDrilldownData,
  downloadCsv,
  downloadElementAsPng,
  formatDashboardValue,
} from "@/lib/dashboard";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

interface Props {
  data: ZipDrilldownData;
  defaultOpen?: boolean;
}

function changeBadge(current: number, projected: number, invertChange = false) {
  if (current === 0) return { text: "N/A", className: "bg-slate-100 text-slate-600" };
  const pct = ((projected - current) / current) * 100;
  const positive = invertChange ? pct <= 0 : pct >= 0;
  return {
    text: `${pct >= 0 ? "+" : ""}${pct.toFixed(1)}%`,
    className: positive ? "bg-emerald-100 text-emerald-700" : "bg-rose-100 text-rose-700",
  };
}

function ZipDrilldownCard({ data, defaultOpen = false }: Props) {
  const [open, setOpen] = useState(defaultOpen);
  const ref = useRef<HTMLDivElement>(null);

  const csvRows = useMemo(() => {
    return data.metrics.map((metric) => ({
      metric: metric.label,
      current_year: data.currentYear,
      current_value: metric.current,
      projected_year: data.projectedYear,
      projected_value: metric.projected,
    }));
  }, [data]);

  return (
    <div className="rounded-[28px] border border-slate-200 bg-white shadow-sm">
      <button
        type="button"
        onClick={() => setOpen((value) => !value)}
        className="flex w-full items-center justify-between gap-4 px-6 py-5 text-left"
      >
        <div>
          <p className="text-2xl font-semibold text-indigo-900">{data.zipCode}{data.placeLabel ? ` ${data.placeLabel}` : ""}</p>
          <p className="mt-1 text-sm text-slate-500">{data.summary}</p>
        </div>
        <span className="rounded-full bg-slate-100 p-2 text-slate-500">
          {open ? <ChevronUp className="w-5 h-5" /> : <ChevronDown className="w-5 h-5" />}
        </span>
      </button>

      {open ? (
        <div className="border-t border-slate-100 px-6 py-6">
          <div className="mb-6 flex flex-col gap-3">
            <div className="grid gap-3 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4">
              {data.metrics.map((metric) => {
                const badge = changeBadge(metric.current, metric.projected, metric.invertChange);
                return (
                  <div key={metric.label} className="rounded-2xl bg-slate-50 p-4">
                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">{metric.label}</p>
                    <div className="mt-3 flex items-end justify-between gap-3">
                      <div>
                        <p className="text-xs text-slate-400">{data.currentYear}</p>
                        <p className="text-lg font-semibold text-slate-900">
                          {formatDashboardValue(metric.current, metric.format)}
                        </p>
                      </div>
                      <div>
                        <p className="text-xs text-slate-400">{data.projectedYear}</p>
                        <p className="text-lg font-semibold text-slate-900">
                          {formatDashboardValue(metric.projected, metric.format)}
                        </p>
                      </div>
                    </div>
                    <span className={`mt-3 inline-flex rounded-full px-2 py-1 text-xs font-semibold ${badge.className}`}>
                      {badge.text}
                    </span>
                  </div>
                );
              })}
            </div>
            <div className="flex justify-end">
              <ChartActionBar
                onDownloadPng={() => downloadElementAsPng(`${data.zipCode}-drilldown.png`, ref.current)}
                onDownloadCsv={() => downloadCsv(`${data.zipCode}-drilldown.csv`, csvRows)}
              />
            </div>
          </div>

          <div ref={ref} className="rounded-[24px] border border-slate-100 bg-white p-4">
            <p className="mb-4 text-lg font-semibold text-slate-900">Family Income Distribution</p>
            <div className="h-[260px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={data.distribution} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
                  <CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" />
                  <XAxis dataKey="bucket" tick={{ fill: "#64748b", fontSize: 11 }} />
                  <YAxis tick={{ fill: "#64748b", fontSize: 12 }} />
                  <Tooltip formatter={(value: number) => formatDashboardValue(value, "number")} />
                  <Bar dataKey="current" fill="#38bdf8" radius={[8, 8, 0, 0]} />
                  <Bar dataKey="projected" fill="#4f46e5" radius={[8, 8, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

export default memo(ZipDrilldownCard);
