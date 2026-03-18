import type { FeatureCollection } from "geojson";
import type { DashboardPreviewModule } from "@/lib/dashboard-preview-data";
import type { DashboardTimeSeriesPoint } from "@/lib/dashboard";
import type { DashboardResponse } from "@/lib/types";

export function toDashboardModuleConfig(payload: DashboardResponse): DashboardPreviewModule {
  const trendYears = Array.from(
    new Set(
      Object.values(payload.data.time_series)
        .flat()
        .map((point) => point.year),
    ),
  ).sort((a, b) => a - b);

  const trendData: DashboardTimeSeriesPoint[] = trendYears.map((year) => {
    const row: DashboardTimeSeriesPoint = { year };
    let projected = false;
    for (const [seriesKey, points] of Object.entries(payload.data.time_series)) {
      const point = points.find((candidate) => candidate.year === year);
      if (!point) continue;
      row[seriesKey] = point.value;
      projected = projected || Boolean(point.projected);
    }
    if (projected) row.projected = true;
    return row;
  });

  const featureCollection = (payload.catchment.geojson ?? {
    type: "FeatureCollection",
    features: [],
  }) as FeatureCollection;

  return {
    slug: payload.data.slug as DashboardPreviewModule["slug"],
    label: payload.data.label,
    eyebrow: payload.data.eyebrow,
    title: payload.data.title,
    description: `${payload.data.description} ${payload.metadata.projection_label ?? ""}`.trim(),
    primaryLabel: payload.data.primary_label,
    primaryValue: payload.data.primary_value,
    secondaryLabel: payload.data.secondary_label,
    secondaryValue: payload.data.secondary_value,
    driveTimeMinutes: payload.catchment.drive_time_minutes,
    zipCount: payload.catchment.zip_codes.length,
    sidebarItems: payload.data.sidebar_items.map((item) => ({
      key: item.key,
      title: item.title,
      description: item.description,
      badge: item.badge ?? undefined,
    })),
    tabs: payload.data.tabs,
    metricOptions: payload.data.metric_options,
    featureCollection,
    trendTitle: payload.data.trend_title,
    trendSubtitle: payload.data.trend_subtitle,
    trendSeries: payload.data.trend_series,
    trendData,
    distributionTitle: payload.data.distribution_title,
    distributionSubtitle: payload.data.distribution_subtitle,
    distributionData: payload.data.distribution.map((row) => ({
      bucket: row.bucket,
      primary: row.primary,
      comparison: row.comparison ?? undefined,
    })),
    zipDrilldowns: Object.fromEntries(
      Object.entries(payload.data.drilldowns).map(([zipCode, drilldown]) => [
        zipCode,
        {
          zipCode: drilldown.zip_code,
          placeLabel: drilldown.place_label ?? undefined,
          summary: drilldown.summary,
          currentYear: drilldown.current_year,
          projectedYear: drilldown.projected_year,
          metrics: drilldown.metrics.map((metric) => ({
            label: metric.label,
            current: metric.current,
            projected: metric.projected,
            format: metric.format,
            invertChange: metric.invert_change,
          })),
          distribution: drilldown.distribution.map((row) => ({
            bucket: row.bucket,
            current: row.primary,
            projected: row.comparison ?? 0,
          })),
        },
      ]),
    ),
    highlightCards: payload.data.highlight_cards,
  };
}
