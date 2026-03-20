import test from "node:test";
import assert from "node:assert/strict";

import { dashboardCompetitors, shouldShowCompetitorTable, toDashboardModuleConfig } from "./dashboard-live";

const payload = {
  catchment: {
    center: { address: "123 Main St", lat: 26.64, lng: -81.87 },
    drive_time_minutes: 20,
    zip_codes: ["33901"],
    geojson: { type: "FeatureCollection", features: [] },
  },
  data: {
    slug: "housing",
    label: "Housing",
    eyebrow: "Housing dashboard",
    title: "Housing Market View",
    description: "Housing description",
    primary_label: "Focus",
    primary_value: "Community profile",
    secondary_label: "ZIPs",
    secondary_value: "1",
    sidebar_items: [
      { key: "community_profile", title: "Community Profile", description: "desc" },
      { key: "existing_resources", title: "Existing Resources", description: "desc" },
    ],
    tabs: [{ key: "summary", label: "Summary" }],
    metric_options: [{ key: "totalPopulation", label: "Total Population", format: "number" }],
    metric_maps: {},
    trend_title: "Trend",
    trend_subtitle: "Subtitle",
    trend_series: [],
    time_series: {
      totalPopulation: [
        { year: 2022, value: 1000, projected: false, label: "Historical" },
        { year: 2029, value: 1100, projected: true, label: "Projected" },
      ],
      renterHouseholds: [
        { year: 2022, value: 300, projected: false, label: "Historical" },
        { year: 2029, value: 330, projected: true, label: "Projected" },
      ],
    },
    distribution_title: "Distribution",
    distribution_subtitle: "Subtitle",
    distribution: [],
    drilldowns: {},
    highlight_cards: [],
  },
  metadata: {
    data_year: 2022,
    projection_years: [2025, 2026, 2027, 2028, 2029],
    last_updated: "2026-03-19T00:00:00Z",
    confidence_band: "medium",
    projection_label: "Directional only",
    geometry_source: "test",
    freshness: {},
  },
} as const;

const housingCompetitor = {
  name: "Harbor Apartments",
  lat: 26.65,
  lon: -81.86,
  distance_miles: 2.1,
  affiliation: "HUD LIHTC",
  is_catholic: false,
  city: "Fort Myers",
  state: "FL",
  zip_code: "33901",
  enrollment: 42,
  gender: "N/A",
  grade_level: "Housing",
  total_units: 80,
};

test("toDashboardModuleConfig keeps housing competitors on existing resources view", () => {
  const analysisResult = {
    ministry_type: "housing",
    competitor_schools: [housingCompetitor],
    catholic_school_count: 0,
    total_private_school_count: 1,
    radius_miles: 5,
    demographics: {
      total_population: 1000,
      total_households: 400,
      hud_eligible_households: 120,
    },
  } as any;

  const config = toDashboardModuleConfig(payload as any, { ministry_type: "housing", market_context: "urban", housing_target_population: "all_ages" } as any, analysisResult);

  assert.equal(config.competitors?.length, 1);
  assert.equal(config.sidebarViews?.existing_resources?.tableVariant, undefined);
  assert.equal(shouldShowCompetitorTable("existing_resources", config.sidebarViews?.existing_resources?.tableVariant, config.competitors?.length ?? 0), true);
});

test("dashboardCompetitors falls back to housing_projects when needed", () => {
  const competitors = dashboardCompetitors({ competitor_schools: undefined, housing_projects: [housingCompetitor] } as any);
  assert.equal(competitors.length, 1);
});

test("toDashboardModuleConfig builds schools config with trend data", () => {
  const schoolsPayload = {
    ...payload,
    data: {
      ...payload.data,
      slug: "schools",
      label: "Schools",
      title: "School Market View",
      time_series: {
        familiesWithChildren: [
          { year: 2020, value: 3000, projected: false, label: "Historical" },
          { year: 2022, value: 3400, projected: false, label: "Historical" },
          { year: 2029, value: 3800, projected: true, label: "Projected" },
        ],
        schoolAgePopulation: [
          { year: 2020, value: 4000, projected: false, label: "Historical" },
          { year: 2022, value: 4100, projected: false, label: "Historical" },
          { year: 2029, value: 4400, projected: true, label: "Projected" },
        ],
      },
    },
  };

  const config = toDashboardModuleConfig(
    schoolsPayload as any,
    { ministry_type: "schools", market_context: "suburban", gender: "coed", grade_level: "k12", weighting_profile: "standard_baseline" } as any,
    null,
  );

  assert.equal(config.slug, "schools");
  assert.equal(config.trendData.length, 3);
  assert.equal(config.trendData[0].year, 2020);
  assert.equal(config.trendData[2].projected, true);
  assert.notEqual(config.distributionReferenceLine, undefined);
});

test("toDashboardModuleConfig builds elder-care config with sidebar views", () => {
  const elderPayload = {
    ...payload,
    data: {
      ...payload.data,
      slug: "elder-care",
      label: "Elder Care",
      title: "Elder Care Market View",
      sidebar_items: [
        { key: "community_profile", title: "Community Profile", description: "desc", badge: "Core" },
        { key: "partnership_viability", title: "Partnership Viability", description: "desc" },
      ],
    },
  };

  const analysisResult = {
    ministry_type: "elder_care",
    competitor_schools: [housingCompetitor],
    catholic_school_count: 0,
    total_private_school_count: 1,
    radius_miles: 5,
    demographics: {
      seniors_65_plus: 5200,
      seniors_75_plus: 2100,
      seniors_living_alone: 1200,
      seniors_projected_5yr: 5600,
      seniors_projected_10yr: 6100,
      total_population: 25000,
      total_households: 9800,
    },
  } as any;

  const config = toDashboardModuleConfig(elderPayload as any, { ministry_type: "elder_care", market_context: "suburban", care_level: "all" } as any, analysisResult);

  assert.equal(config.slug, "elder-care");
  assert.notEqual(config.sidebarViews, undefined);
  assert.notEqual(config.sidebarViews?.partnership_viability, undefined);
  assert.equal(config.sidebarViews?.partnership_viability?.tableVariant, "partner");
});

test("shouldShowCompetitorTable returns false for partner table variant", () => {
  assert.equal(shouldShowCompetitorTable("partnership_viability", "partner", 5), false);
  assert.equal(shouldShowCompetitorTable("competitors", undefined, 5), true);
  assert.equal(shouldShowCompetitorTable("competitors", undefined, 0), false);
});
