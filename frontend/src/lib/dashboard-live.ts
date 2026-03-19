import type { FeatureCollection } from "geojson";
import type { DashboardPreviewModule, DashboardPreviewView } from "@/lib/dashboard-preview-data";
import type { ParameterBarField } from "@/lib/dashboard";
import type { DashboardTimeSeriesPoint } from "@/lib/dashboard";
import type { AnalysisRequest, AnalysisResponse, DashboardResponse } from "@/lib/types";

function requestFields(request: AnalysisRequest): ParameterBarField[] {
  const shared: ParameterBarField[] = [{ label: "Market", value: request.market_context }];

  if (request.ministry_type === "schools") {
    return [
      { label: "Grades", value: request.grade_level.replace("_", " ").toUpperCase() },
      { label: "Gender", value: request.gender },
      { label: "Weighting", value: request.weighting_profile.replaceAll("_", " ") },
      ...shared,
    ];
  }

  if (request.ministry_type === "elder_care") {
    return [
      { label: "Care level", value: request.care_level.replaceAll("_", " ") },
      ...shared,
    ];
  }

  return [
    { label: "Target", value: (request.housing_target_population ?? "all_ages").replaceAll("_", " ") },
    ...shared,
  ];
}

function sidebarViewsForModule(slug: DashboardPreviewModule["slug"]): Record<string, DashboardPreviewView> | undefined {
  if (slug === "schools") {
    return {
      affordability: {
        title: "Affordability",
        description: "Wave 1 schools work starts with tuition fit, aid pressure, and the depth of high-income families across the catchment.",
        tabs: [
          { key: "summary", label: "Summary" },
          { key: "median_income", label: "Median Income" },
          { key: "high_income", label: "High Income ($200K+)" },
          { key: "distribution", label: "Distribution" },
          { key: "change_average", label: "Change in Average" },
        ],
        distributionReferenceLine: {
          value: 1000,
          label: "Tuition-qualified family target",
          color: "#f59e0b",
        },
      },
    };
  }

  if (slug === "elder-care") {
    return {
      partnership_viability: {
        title: "Partnership Viability",
        description: "Wave 1 elder-care work centers on nearby operators, their ownership profile, and visible quality signals.",
        tabs: [
          { key: "service_map", label: "Service Map" },
          { key: "potential_partners", label: "Potential Partners" },
        ],
        tableVariant: "partner",
      },
    };
  }

  if (slug === "housing") {
    return {
      community_profile: {
        title: "Community Profile",
        description: "Wave 1 folds demographic trends into Community Profile so population, tenure, and burden context stay together.",
        tabs: [
          { key: "population_trend", label: "Population Trend" },
          { key: "renter_owner", label: "Renter vs. Owner" },
          { key: "age_distribution", label: "Age Distribution" },
          { key: "poverty_rate", label: "Poverty Rate" },
        ],
      },
    };
  }

  return undefined;
}

export function toDashboardModuleConfig(
  payload: DashboardResponse,
  request: AnalysisRequest,
  analysisResult?: AnalysisResponse | null,
): DashboardPreviewModule {
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
    address: payload.catchment.center.address,
    primaryLabel: payload.data.primary_label,
    primaryValue: payload.data.primary_value,
    secondaryLabel: payload.data.secondary_label,
    secondaryValue: payload.data.secondary_value,
    driveTimeMinutes: payload.catchment.drive_time_minutes,
    zipCount: payload.catchment.zip_codes.length,
    parameterFields: requestFields(request),
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
    distributionReferenceLine:
      payload.data.slug === "schools"
        ? {
            value: 1000,
            label: "Tuition-qualified family target",
            color: "#f59e0b",
          }
        : undefined,
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
    sidebarViews: sidebarViewsForModule(payload.data.slug as DashboardPreviewModule["slug"]),
    competitors: analysisResult?.competitor_schools ?? [],
    competitorCounts: {
      catholicCount: analysisResult?.catholic_school_count ?? 0,
      totalPrivateCount:
        analysisResult?.total_private_school_count ??
        analysisResult?.competitor_schools.length ??
        0,
      radiusMiles: analysisResult?.radius_miles ?? 0,
    },
  };
}
