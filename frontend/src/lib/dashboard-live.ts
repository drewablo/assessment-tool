import type { FeatureCollection } from "geojson";
import type { DashboardPreviewModule, DashboardPreviewView } from "@/lib/dashboard-preview-data";
import type { ParameterBarField } from "@/lib/dashboard";
import type { DashboardTimeSeriesPoint } from "@/lib/dashboard";
import type { AnalysisRequest, AnalysisResponse, DashboardResponse } from "@/lib/types";

export function dashboardCompetitors(analysisResult?: AnalysisResponse | null) {
  return analysisResult?.competitor_schools ?? analysisResult?.housing_projects ?? [];
}

export function shouldShowCompetitorTable(activeSidebar: string, tableVariant: string | undefined, competitorCount: number) {
  return tableVariant !== "partner" && ["competitors", "market_landscape", "existing_resources", "enrollment"].includes(activeSidebar) && competitorCount > 0;
}

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

function sidebarViewsForModule(
  slug: DashboardPreviewModule["slug"],
  analysisResult?: AnalysisResponse | null,
): Record<string, DashboardPreviewView> | undefined {
  const demographics = analysisResult?.demographics;
  const competitors = dashboardCompetitors(analysisResult);

  if (slug === "schools") {
    return {
      affordability: {
        title: "Affordability",
        description: "Tuition fit, aid pressure, and the depth of high-income families across the catchment.",
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
      student_body: {
        title: "Student Body",
        description: "School-age population trend and Catholic-affiliation context alongside catchment enrollment comparison.",
        tabs: [
          { key: "age_cohorts", label: "Age Cohorts" },
          { key: "population_trend", label: "Population Trend" },
          { key: "catchment_enrollment", label: "Catchment vs. Enrollment" },
          { key: "catholic_affiliation", label: "Catholic Affiliation" },
        ],
        callout: {
          tone: "info",
          title: "Catchment comparison requires your enrollment data",
          body: "Select the \"Catchment vs. Enrollment\" tab and enter your current enrollment by grade to compare against the catchment's school-age population.",
        },
        metricOptions: [
          { key: "schoolAgePopulation", label: "School-Age Population" },
          { key: "familiesWithChildren", label: "Families with Children" },
          { key: "medianFamilyIncome", label: "Median Family Income", format: "currency" as const },
        ],
        trendTitle: "School-Age Population Trend",
        trendSubtitle: "School-age and family trend series provide the baseline for student body context.",
        trendSeries: [
          { key: "schoolAgePopulation", label: "School-Age Population", color: "#7c3aed" },
          { key: "familiesWithChildren", label: "Families with Children", color: "#2563eb" },
        ],
        distributionTitle: "Student Body Context",
        distributionSubtitle: "Until grade-band cohort payloads are added, this view summarizes the available school-age and Catholic-affiliation counts instead.",
        distributionPrimaryLabel: "Current",
        distributionComparisonLabel: "Projected",
        distributionData: [
          {
            bucket: "School-age pop.",
            primary: demographics?.school_age_population ?? 0,
            comparison: Math.round((demographics?.school_age_population ?? 0) * 1.05),
          },
          {
            bucket: "Est. Catholic",
            primary: demographics?.estimated_catholic_school_age ?? 0,
            comparison: Math.round((demographics?.estimated_catholic_school_age ?? 0) * 1.05),
          },
        ],
        highlightCards: [
          {
            label: "School-age pop.",
            value: `${Math.round(demographics?.school_age_population ?? 0).toLocaleString()}`,
            detail: "Current school-age population across the catchment.",
          },
          {
            label: "Est. Catholic school-age",
            value: `${Math.round(demographics?.estimated_catholic_school_age ?? 0).toLocaleString()}`,
            detail: "Directional estimate using the current Catholic-affiliation methodology.",
          },
          {
            label: "Catholic share",
            value: demographics?.estimated_catholic_pct != null ? `${demographics.estimated_catholic_pct.toFixed(1)}%` : "N/A",
            detail: "State-level contextual share applied to school-age population.",
          },
        ],
      },
      enrollment: {
        title: "Enrollment",
        description: "Market-size and competitor-overlap framing with an interactive enrollment scenario modeler.",
        tabs: [
          { key: "market_size", label: "Market Size" },
          { key: "competitor_overlap", label: "Competitor Overlap" },
          { key: "enrollment_scenarios", label: "Enrollment Scenarios" },
        ],
        callout: {
          tone: "info",
          title: "Financial scenario modeler available",
          body: "Select the \"Enrollment Scenarios\" tab to access the interactive What-If financial model with enrollment, tuition, and aid assumptions.",
        },
        metricOptions: [
          { key: "familiesWithChildren", label: "Families with Children" },
          { key: "schoolAgePopulation", label: "School-Age Population" },
          { key: "competitorCount", label: "Nearby Competitors" },
        ],
        trendTitle: "Addressable Market vs. Reference Enrollment",
        trendSubtitle: "Use current addressable-market depth and nearby competitor counts to judge how aggressive the initial enrollment target should be.",
        distributionTitle: "Enrollment Planning Context",
        distributionSubtitle: "Addressable market and benchmark enrollment size provide context for scenario planning.",
        distributionPrimaryLabel: "Current",
        distributionComparisonLabel: "Directional target",
        distributionData: [
          {
            bucket: "Addressable market",
            primary: demographics?.total_addressable_market ?? 0,
            comparison: Math.round((demographics?.total_addressable_market ?? 0) * 1.1),
          },
          {
            bucket: "Reference enrollment",
            primary: demographics?.reference_enrollment ?? 0,
            comparison: demographics?.reference_enrollment ?? 0,
          },
          {
            bucket: "Nearby competitors",
            primary: competitors.length,
            comparison: competitors.length,
          },
        ],
        highlightCards: [
          {
            label: "Market depth",
            value: demographics?.market_depth_ratio != null ? `${demographics.market_depth_ratio.toFixed(1)}×` : "N/A",
            detail: "Addressable families relative to the current reference enrollment benchmark.",
          },
          {
            label: "Addressable market",
            value: `${Math.round(demographics?.total_addressable_market ?? 0).toLocaleString()}`,
            detail: "Propensity-weighted families currently estimated to be reachable.",
          },
          {
            label: "Reference size",
            value: `${Math.round(demographics?.reference_enrollment ?? 0).toLocaleString()}`,
            detail: "Benchmark enrollment size used in the market-depth calculation.",
          },
        ],
      },
    };
  }

  if (slug === "elder-care") {
    return {
      partnership_viability: {
        title: "Partnership Viability",
        description: "Nearby operators, their ownership profile, and visible quality signals.",
        tabs: [
          { key: "service_map", label: "Service Map" },
          { key: "potential_partners", label: "Potential Partners" },
        ],
        tableVariant: "partner",
      },
      projections: {
        title: "Projections",
        description: "Elder-care projections with a cohort-breakdown lens and care-planning takeaways.",
        tabs: [
          { key: "cohort_breakdown", label: "Cohort Breakdown" },
          { key: "care_implications", label: "Care Implications" },
        ],
        callout: {
          tone: "info",
          title: "Cohort detail is partially available",
          body: "Current live data supports 65+ and 75+ trend views today; a fuller 75–84 / 85+ breakout remains tied to richer age-cohort payload work.",
        },
        trendTitle: "Aging Pipeline Outlook",
        trendSubtitle: "Current live projections show the total senior base and the older 75+ cohort so care planning can start before deeper cohort enrichment lands.",
        trendSeries: [
          { key: "seniors65Plus", label: "Seniors 65+", color: "#2563eb" },
          { key: "seniors75Plus", label: "Seniors 75+", color: "#16a34a" },
        ],
        distributionTitle: "Current vs. 10-Year Senior Cohorts",
        distributionSubtitle: "The available cohort split shows how much of the senior base is already concentrated in higher-acuity age bands.",
        distributionPrimaryLabel: "Current",
        distributionComparisonLabel: "10-Year",
        distributionData: [
          {
            bucket: "65-74",
            primary: Math.max((demographics?.seniors_65_plus ?? 0) - (demographics?.seniors_75_plus ?? 0), 0),
            comparison: Math.max((demographics?.seniors_projected_10yr ?? demographics?.seniors_65_plus ?? 0) - (demographics?.seniors_projected_5yr ?? demographics?.seniors_75_plus ?? 0), 0),
          },
          {
            bucket: "75+",
            primary: demographics?.seniors_75_plus ?? 0,
            comparison: demographics?.seniors_projected_5yr ?? demographics?.seniors_75_plus ?? 0,
          },
        ],
        highlightCards: [
          {
            label: "5-year seniors",
            value: `${Math.round(demographics?.seniors_projected_5yr ?? 0).toLocaleString()}`,
            detail: "Projected 5-year catchment total in the current elder-care model.",
          },
          {
            label: "10-year seniors",
            value: `${Math.round(demographics?.seniors_projected_10yr ?? 0).toLocaleString()}`,
            detail: "Projected 10-year catchment total for long-range care planning.",
          },
          {
            label: "Living alone",
            value: `${Math.round(demographics?.seniors_living_alone ?? 0).toLocaleString()}`,
            detail: "Directional proxy for care-navigation and support-intensity demand.",
          },
        ],
      },
    };
  }

  if (slug === "housing") {
    const totalUnits = competitors.reduce((sum, project) => sum + (project.total_units ?? project.enrollment ?? 0), 0);
    const supplyGap = Math.max((demographics?.hud_eligible_households ?? 0) - totalUnits, 0);

    return {
      community_profile: {
        title: "Community Profile",
        description: "Population, tenure, poverty, and housing-need trend context.",
        tabs: [
          { key: "population_trend", label: "Population Trend" },
          { key: "renter_owner", label: "Renter vs. Owner" },
          { key: "age_distribution", label: "Age Distribution" },
          { key: "poverty_rate", label: "Poverty Rate" },
        ],
        metricOptions: [
          { key: "totalPopulation", label: "Total Population" },
          { key: "renterHouseholds", label: "Renter Households" },
          { key: "costBurdenRate", label: "Cost-Burden Rate", format: "percent" as const },
          { key: "medianHouseholdIncome", label: "Median Household Income", format: "currency" as const },
        ],
        trendTitle: "Population & Household Trends",
        trendSubtitle: "Total population and renter household counts provide the demographic baseline for housing demand assessment.",
        trendSeries: [
          { key: "totalPopulation", label: "Total Population", color: "#6366f1" },
          { key: "renterHouseholds", label: "Renter Households", color: "#16a34a" },
        ],
        highlightCards: [
          {
            label: "Total population",
            value: `${Math.round(demographics?.total_population ?? 0).toLocaleString()}`,
            detail: "Current total population within the analysis catchment.",
          },
          {
            label: "Total households",
            value: `${Math.round(demographics?.total_households ?? 0).toLocaleString()}`,
            detail: "Total households in the catchment.",
          },
          {
            label: "HUD-eligible households",
            value: `${Math.round(demographics?.hud_eligible_households ?? 0).toLocaleString()}`,
            detail: "Estimated households below 60% of area median income.",
          },
        ],
        tabViews: {
          population_trend: {
            trendTitle: "Population & Household Trends",
            trendSubtitle: "Total population and renter household counts provide the demographic baseline for housing demand assessment.",
            trendSeries: [
              { key: "totalPopulation", label: "Total Population", color: "#6366f1" },
              { key: "renterHouseholds", label: "Renter Households", color: "#16a34a" },
            ],
          },
          renter_owner: {
            trendTitle: "Renter vs. Owner Tenure",
            trendSubtitle: "Renter household share relative to total households shows where housing pressure is concentrated.",
            trendSeries: [
              { key: "renterHouseholds", label: "Renter Households", color: "#16a34a" },
            ],
            highlightCards: [
              {
                label: "Renter households",
                value: `${Math.round(demographics?.renter_households ?? 0).toLocaleString()}`,
                detail: "Current renter households in the catchment.",
              },
              {
                label: "Owner-occupied",
                value: demographics?.owner_occupied_pct != null ? `${(demographics.owner_occupied_pct * 100).toFixed(1)}%` : "N/A",
                detail: "Share of households that are owner-occupied.",
              },
              {
                label: "Total households",
                value: `${Math.round(demographics?.total_households ?? 0).toLocaleString()}`,
                detail: "Total households in the catchment.",
              },
            ],
          },
          age_distribution: {
            trendTitle: "Age Cohort Trends",
            trendSubtitle: "Population age structure helps identify current and future housing demand across cohorts.",
            trendSeries: [
              { key: "totalPopulation", label: "Total Population", color: "#6366f1" },
            ],
            highlightCards: [
              {
                label: "Total population",
                value: `${Math.round(demographics?.total_population ?? 0).toLocaleString()}`,
                detail: "Current total population within the analysis catchment.",
              },
              {
                label: "Seniors 65+",
                value: `${Math.round(demographics?.seniors_65_plus ?? 0).toLocaleString()}`,
                detail: "Current senior population in the catchment.",
              },
              {
                label: "Under 18",
                value: `${Math.round(demographics?.population_under_18 ?? 0).toLocaleString()}`,
                detail: "Current population under 18 in the catchment.",
              },
            ],
          },
          poverty_rate: {
            trendTitle: "Cost Burden & Affordability",
            trendSubtitle: "Cost-burdened renter households and HUD-eligible counts frame the depth of housing affordability need.",
            trendSeries: [
              { key: "costBurdenedHouseholds", label: "Cost-Burdened Households", color: "#dc2626" },
              { key: "hudEligibleHouseholds", label: "HUD-Eligible Households", color: "#2563eb" },
            ],
            highlightCards: [
              {
                label: "Cost-burdened renters",
                value: `${Math.round(demographics?.cost_burdened_renter_households ?? 0).toLocaleString()}`,
                detail: "Renter households spending 30%+ of income on housing.",
              },
              {
                label: "HUD-eligible",
                value: `${Math.round(demographics?.hud_eligible_households ?? 0).toLocaleString()}`,
                detail: "Estimated households below 60% of area median income.",
              },
              {
                label: "Median income",
                value: `$${Math.round(demographics?.median_household_income ?? 0).toLocaleString()}`,
                detail: "Median household income across the catchment.",
              },
            ],
          },
        },
      },
      existing_resources: {
        title: "Existing Resources",
        description: "Nearby subsidized inventory, approximate supply gap, and QCT/DDA context.",
        tabs: [
          { key: "subsidized_map", label: "Subsidized Housing Map" },
          { key: "project_table", label: "Project Table" },
          { key: "supply_gap", label: "Supply Gap" },
          { key: "pipeline", label: "Pipeline" },
        ],
        callout: {
          tone: "info",
          title: "QCT/DDA boundary overlays available",
          body: "When boundary data is available for this catchment, QCT and DDA designation polygons are rendered on the map with dashed outlines.",
        },
        metricOptions: [
          { key: "hudEligibleHouseholds", label: "HUD-Eligible Households" },
          { key: "costBurdenedHouseholds", label: "Cost-Burdened Households" },
          { key: "renterHouseholds", label: "Renter Households" },
        ],
        trendTitle: "Existing Subsidized Supply vs. Need",
        trendSubtitle: "Visible affordability pressure contrasted with eligible-household depth across the catchment.",
        trendSeries: [
          { key: "costBurdenedHouseholds", label: "Cost-Burdened Households", color: "#dc2626" },
          { key: "hudEligibleHouseholds", label: "HUD-Eligible Households", color: "#2563eb" },
        ],
        highlightCards: [
          {
            label: "Approx. supply gap",
            value: `${Math.round(supplyGap).toLocaleString()} HH`,
            detail: "HUD-eligible households minus currently visible subsidized units in the competitive set.",
          },
          {
            label: "QCT projects",
            value: `${Math.round(demographics?.qct_designated_projects ?? 0)}`,
            detail: "Projects currently flagged as QCT-designated in the available enrichment.",
          },
          {
            label: "DDA projects",
            value: `${Math.round(demographics?.dda_designated_projects ?? 0)}`,
            detail: "Projects currently flagged as DDA-designated in the available enrichment.",
          },
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
    centerLat: payload.catchment.center.lat,
    centerLon: payload.catchment.center.lng,
    radiusMiles: analysisResult?.catchment_mode === "radius" ? analysisResult.radius_miles : undefined,
    boundaryOverlays: (payload.catchment.boundary_overlays ?? undefined) as FeatureCollection | undefined,
    analysisResult: analysisResult ?? undefined,
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
    sidebarViews: sidebarViewsForModule(payload.data.slug as DashboardPreviewModule["slug"], analysisResult),
    competitors: dashboardCompetitors(analysisResult),
    competitorCounts: {
      catholicCount: analysisResult?.catholic_school_count ?? 0,
      totalPrivateCount:
        analysisResult?.total_private_school_count ??
        dashboardCompetitors(analysisResult).length ??
        0,
      radiusMiles: analysisResult?.radius_miles ?? 0,
    },
  };
}
