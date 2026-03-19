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

  assert.equal(config.competitors.length, 1);
  assert.equal(config.sidebarViews?.existing_resources?.tableVariant, undefined);
  assert.equal(shouldShowCompetitorTable("existing_resources", config.sidebarViews?.existing_resources?.tableVariant, config.competitors.length), true);
});

test("dashboardCompetitors falls back to housing_projects when needed", () => {
  const competitors = dashboardCompetitors({ competitor_schools: undefined, housing_projects: [housingCompetitor] } as any);
  assert.equal(competitors.length, 1);
});
