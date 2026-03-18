"use client";

import { memo, useMemo, useRef } from "react";
import { Fragment } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import ChartActionBar from "./ChartActionBar";
import { DashboardSeries, DashboardTimeSeriesPoint, downloadCsv, downloadElementAsPng, formatDashboardValue } from "@/lib/dashboard";

interface Props {
  title: string;
  subtitle?: string;
  data: DashboardTimeSeriesPoint[];
  series: DashboardSeries[];
  fileBaseName?: string;
}

function TrendChart({ title, subtitle, data, series, fileBaseName = "trend-chart" }: Props) {
  const ref = useRef<HTMLDivElement>(null);

  const chartData = useMemo(() => {
    return data.map((point, index) => {
      const previous = data[index - 1];
      const row: Record<string, string | number | boolean | null | undefined> = {
        year: point.year,
      };

      for (const item of series) {
        const value = point[item.key] as number | null | undefined;
        row[`${item.key}_historical`] = point.projected ? null : value;
        row[`${item.key}_projected`] = point.projected ? value : previous?.projected ? null : value;
      }

      return row;
    });
  }, [data, series]);

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
          <LineChart data={chartData} margin={{ top: 16, right: 16, left: 0, bottom: 0 }}>
            <CartesianGrid stroke="#e2e8f0" strokeDasharray="3 3" />
            <XAxis dataKey="year" tick={{ fill: "#64748b", fontSize: 12 }} stroke="#cbd5e1" />
            <YAxis tick={{ fill: "#64748b", fontSize: 12 }} stroke="#cbd5e1" />
            <Tooltip
              formatter={(value: number, key: string) => {
                const match = series.find((item) => key.startsWith(item.key));
                return [formatDashboardValue(value, match?.format), match?.label ?? key];
              }}
              labelFormatter={(label) => `Year ${label}`}
            />
            <Legend />
            {series.map((item) => (
              <Fragment key={item.key}>
                <Line
                  type="monotone"
                  dataKey={`${item.key}_historical`}
                  name={item.label}
                  stroke={item.color}
                  strokeWidth={3}
                  dot={{ r: 4 }}
                  activeDot={{ r: 6 }}
                  connectNulls
                />
                <Line
                  type="monotone"
                  dataKey={`${item.key}_projected`}
                  name={`${item.label} (Projected)`}
                  stroke={item.color}
                  strokeWidth={3}
                  strokeDasharray="6 6"
                  dot={{ r: 4 }}
                  activeDot={{ r: 6 }}
                  connectNulls
                />
              </Fragment>
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

export default memo(TrendChart);
