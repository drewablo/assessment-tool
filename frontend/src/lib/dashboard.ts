export type DashboardMetricValue = number | null | undefined;

export interface DashboardMetricOption {
  key: string;
  label: string;
  format?: "number" | "currency" | "percent";
}

export interface DashboardSeries {
  key: string;
  label: string;
  color: string;
  format?: "number" | "currency" | "percent";
}

export interface DashboardTimeSeriesPoint {
  year: number;
  projected?: boolean;
  [key: string]: string | number | boolean | null | undefined;
}

export interface DashboardDistributionBucket {
  bucket: string;
  primary: number;
  comparison?: number | null;
}

export interface DashboardReferenceLine {
  value: number;
  label: string;
  color?: string;
}

export interface ZipMetricSnapshot {
  label: string;
  current: number;
  projected: number;
  format?: "number" | "currency" | "percent";
  invertChange?: boolean;
}

export interface ZipDistributionBucket {
  bucket: string;
  current: number;
  projected: number;
}

export interface ZipDrilldownData {
  zipCode: string;
  placeLabel?: string;
  summary: string;
  currentYear: number;
  projectedYear: number;
  metrics: ZipMetricSnapshot[];
  distribution: ZipDistributionBucket[];
}

export interface DashboardSidebarItem {
  key: string;
  title: string;
  description: string;
  badge?: string;
}

export interface DashboardTabItem {
  key: string;
  label: string;
}

export interface ParameterBarField {
  label: string;
  value: string | number;
}

export function formatDashboardValue(
  value: DashboardMetricValue,
  format: DashboardMetricOption["format"] = "number",
): string {
  if (value == null || Number.isNaN(value)) return "N/A";

  if (format === "currency") {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: 0,
    }).format(Number(value));
  }

  if (format === "percent") {
    return `${Number(value).toFixed(1)}%`;
  }

  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: Math.abs(Number(value)) >= 100 ? 0 : 1,
  }).format(Number(value));
}

export function downloadCsv<T extends object>(filename: string, rows: T[]) {
  if (typeof window === "undefined") return;
  const headers = Array.from(
    rows.reduce((set, row) => {
      Object.keys(row as Record<string, unknown>).forEach((key) => set.add(key));
      return set;
    }, new Set<string>()),
  );

  const escape = (value: string | number | boolean | null | undefined) => {
    const text = value == null ? "" : String(value);
    return /[",\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
  };

  const lines = [headers.join(",")];
  for (const row of rows) {
    const record = row as Record<string, string | number | boolean | null | undefined>;
    lines.push(headers.map((header) => escape(record[header])).join(","));
  }

  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}

export async function downloadElementAsPng(filename: string, node: HTMLElement | null) {
  if (typeof window === "undefined" || !node) return;
  const { toPng } = await import("html-to-image");
  const dataUrl = await toPng(node, {
    cacheBust: true,
    pixelRatio: 2,
    backgroundColor: "#ffffff",
  });
  const link = document.createElement("a");
  link.href = dataUrl;
  link.download = filename;
  link.click();
}
