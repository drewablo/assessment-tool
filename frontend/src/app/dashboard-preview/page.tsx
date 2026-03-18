"use client";

import { useMemo, useState } from "react";
import type { FeatureCollection } from "geojson";
import {
  ChoroplethMap,
  DashboardSidebar,
  DistributionChart,
  ParameterBar,
  TabbedSubview,
  TrendChart,
  ZipDrilldownCard,
} from "@/components/dashboard";
import {
  DashboardMetricOption,
  DashboardSidebarItem,
  DashboardTabItem,
  ZipDrilldownData,
} from "@/lib/dashboard";

const sidebarItems: DashboardSidebarItem[] = [
  { key: "market_overview", title: "Market Overview", description: "Population, income, and diversity context for the catchment.", badge: "New" },
  { key: "affordability", title: "Affordability", description: "High-income families, distribution shifts, and gap analysis." },
  { key: "enrollment", title: "Enrollment", description: "Market depth, competitor density, and school opportunity." },
  { key: "student_body", title: "Student Body", description: "School-age cohorts and forward-looking demand signals." },
  { key: "competitors", title: "Competitors", description: "Nearby schools and sortable landscape review." },
];

const tabs: DashboardTabItem[] = [
  { key: "summary", label: "Summary" },
  { key: "median", label: "Median" },
  { key: "high_income", label: "High Income" },
  { key: "distribution", label: "Distribution" },
  { key: "change_average", label: "Change in Average" },
];

const metricOptions: DashboardMetricOption[] = [
  { key: "schoolAgePopulation", label: "School-Age Population" },
  { key: "familiesWithChildren", label: "Families with Children" },
  { key: "medianFamilyIncome", label: "Median Family Income", format: "currency" },
];

const timeSeries = [
  { year: 2019, families: 9180, highIncome: 4630 },
  { year: 2020, families: 9410, highIncome: 4875 },
  { year: 2021, families: 9720, highIncome: 5250 },
  { year: 2022, families: 10030, highIncome: 5690 },
  { year: 2023, families: 10210, highIncome: 6030 },
  { year: 2024, families: 10580, highIncome: 6450 },
  { year: 2025, families: 10860, highIncome: 6845, projected: true },
  { year: 2026, families: 11140, highIncome: 7240, projected: true },
  { year: 2027, families: 11470, highIncome: 7710, projected: true },
  { year: 2028, families: 11850, highIncome: 8235, projected: true },
  { year: 2029, families: 12210, highIncome: 8790, projected: true },
];

const incomeDistribution = [
  { bucket: "<$50K", primary: 1342, comparison: 1210 },
  { bucket: "$50K-$75K", primary: 2140, comparison: 2265 },
  { bucket: "$75K-$100K", primary: 1982, comparison: 2148 },
  { bucket: "$100K-$125K", primary: 1724, comparison: 1836 },
  { bucket: "$125K-$150K", primary: 1360, comparison: 1475 },
  { bucket: "$150K-$200K", primary: 1088, comparison: 1248 },
  { bucket: "$200K-$350K", primary: 904, comparison: 1125 },
  { bucket: ">$350K", primary: 290, comparison: 406 },
];

const zipDrilldowns: Record<string, ZipDrilldownData> = {
  "33971": {
    zipCode: "33971",
    placeLabel: "Lehigh Acres",
    summary: "Strong family growth with improving income capacity and a narrowing tuition gap.",
    currentYear: 2024,
    projectedYear: 2029,
    metrics: [
      { label: "Families with Children", current: 4867, projected: 5326 },
      { label: "Median Family Income", current: 66098, projected: 72651, format: "currency" },
      { label: "Financial Gap", current: 27000, projected: 26000, format: "currency", invertChange: true },
    ],
    distribution: [
      { bucket: "<$50K", current: 580, projected: 512 },
      { bucket: "$25K-$50K", current: 1041, projected: 1022 },
      { bucket: "$50K-$75K", current: 1262, projected: 1298 },
      { bucket: "$75K-$100K", current: 938, projected: 1006 },
      { bucket: "$100K-$125K", current: 543, projected: 602 },
      { bucket: "$125K-$150K", current: 342, projected: 388 },
      { bucket: "$150K-$200K", current: 423, projected: 486 },
      { bucket: ">$200K", current: 175, projected: 236 },
    ],
  },
  "33913": {
    zipCode: "33913",
    placeLabel: "Fort Myers",
    summary: "Highest-income ZIP in the catchment with a large projected gain in affluent households.",
    currentYear: 2024,
    projectedYear: 2029,
    metrics: [
      { label: "Families with Children", current: 3924, projected: 4295 },
      { label: "Median Family Income", current: 91144, projected: 100882, format: "currency" },
      { label: "Financial Gap", current: 12000, projected: 8800, format: "currency", invertChange: true },
    ],
    distribution: [
      { bucket: "<$50K", current: 211, projected: 168 },
      { bucket: "$25K-$50K", current: 348, projected: 310 },
      { bucket: "$50K-$75K", current: 621, projected: 588 },
      { bucket: "$75K-$100K", current: 774, projected: 812 },
      { bucket: "$100K-$125K", current: 642, projected: 711 },
      { bucket: "$125K-$150K", current: 518, projected: 596 },
      { bucket: "$150K-$200K", current: 497, projected: 618 },
      { bucket: ">$200K", current: 313, projected: 492 },
    ],
  },
};

const featureCollection: FeatureCollection = {
  type: "FeatureCollection",
  features: [
    {
      type: "Feature",
      properties: {
        zipCode: "33971",
        name: "33971 Lehigh Acres",
        schoolAgePopulation: 9200,
        familiesWithChildren: 4867,
        medianFamilyIncome: 66098,
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [-81.93, 26.59],
          [-81.86, 26.59],
          [-81.86, 26.66],
          [-81.93, 26.66],
          [-81.93, 26.59],
        ]],
      },
    },
    {
      type: "Feature",
      properties: {
        zipCode: "33913",
        name: "33913 Fort Myers",
        schoolAgePopulation: 7400,
        familiesWithChildren: 3924,
        medianFamilyIncome: 91144,
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [-81.86, 26.57],
          [-81.77, 26.57],
          [-81.77, 26.66],
          [-81.86, 26.66],
          [-81.86, 26.57],
        ]],
      },
    },
    {
      type: "Feature",
      properties: {
        zipCode: "33967",
        name: "33967 Fort Myers",
        schoolAgePopulation: 5600,
        familiesWithChildren: 2718,
        medianFamilyIncome: 80774,
      },
      geometry: {
        type: "Polygon",
        coordinates: [[
          [-81.88, 26.50],
          [-81.79, 26.50],
          [-81.79, 26.57],
          [-81.88, 26.57],
          [-81.88, 26.50],
        ]],
      },
    },
  ],
};

export default function DashboardPreviewPage() {
  const [activeSidebar, setActiveSidebar] = useState(sidebarItems[0].key);
  const [activeTab, setActiveTab] = useState(tabs[0].key);
  const [metricKey, setMetricKey] = useState(metricOptions[0].key);
  const [selectedZip, setSelectedZip] = useState<keyof typeof zipDrilldowns>("33971");

  const selectedMetric = useMemo(
    () => metricOptions.find((item) => item.key === metricKey) ?? metricOptions[0],
    [metricKey],
  );

  return (
    <main className="min-h-screen bg-[#f7f7fc] px-6 py-10 text-slate-900">
      <div className="mx-auto max-w-[1440px] space-y-8">
        <div className="space-y-2">
          <p className="text-sm font-semibold uppercase tracking-[0.25em] text-indigo-600">Dashboard shared components</p>
          <h1 className="text-4xl font-semibold tracking-tight">NAIS-style dashboard preview</h1>
          <p className="max-w-3xl text-base leading-7 text-slate-500">
            Mock-data preview of the shared Phase 1 dashboard components. This page is additive and does not alter the existing assessment workflow.
          </p>
        </div>

        <ParameterBar
          driveTimeMinutes={30}
          address="15680 Pine Ridge Road, Fort Myers, FL"
          primaryLabel="tuition"
          primaryValue="$27,950"
          secondaryLabel="View"
          secondaryValue="All ZIPs"
          zipCount={18}
        />

        <div className="grid gap-8 xl:grid-cols-[280px_minmax(0,1fr)]">
          <DashboardSidebar items={sidebarItems} activeKey={activeSidebar} onSelect={setActiveSidebar} />

          <section className="space-y-6">
            <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
              <div className="flex flex-col gap-4 border-b border-slate-100 pb-5 lg:flex-row lg:items-center lg:justify-between">
                <div>
                  <h2 className="text-4xl font-semibold tracking-tight text-slate-950">Household Income</h2>
                  <p className="mt-1 text-sm text-slate-500">Shared tabbed subview pattern for module-specific analytical slices.</p>
                </div>
                <TabbedSubview tabs={tabs} activeKey={activeTab} onChange={setActiveTab} />
              </div>
            </div>

            <TrendChart
              title="High-Income Families ($200K+)"
              subtitle="Within a 30-minute drive of 15680 Pine Ridge Road, Fort Myers between 2019–2029. Projected values are visually distinct."
              data={timeSeries.map((point) => ({ year: point.year, projected: point.projected, highIncome: point.highIncome, families: point.families }))}
              series={[
                { key: "highIncome", label: "High-Income Families", color: "#16a34a" },
                { key: "families", label: "Families with Children", color: "#2563eb" },
              ]}
              fileBaseName="high-income-families-preview"
            />

            <div className="grid gap-6 2xl:grid-cols-[minmax(0,1.2fr)_minmax(420px,0.8fr)]">
              <ChoroplethMap
                title="Total School-Age Population by ZIP Code"
                subtitle="ZIP-level shaded map with metric switching and click-to-select drilldown behavior."
                featureCollection={featureCollection}
                metric={selectedMetric}
                availableMetrics={metricOptions}
                selectedZip={selectedZip}
                onMetricChange={setMetricKey}
                onZipSelect={(zipCode) => {
                  if (zipCode in zipDrilldowns) {
                    setSelectedZip(zipCode as keyof typeof zipDrilldowns);
                  }
                }}
                fileBaseName="school-age-population-choropleth"
              />

              <ZipDrilldownCard data={zipDrilldowns[selectedZip]} defaultOpen />
            </div>

            <DistributionChart
              title="2024 vs 2029 Distribution of Household Income"
              subtitle="Shared horizontal-bar comparison view for income and demographic bucket analysis."
              data={incomeDistribution}
              primaryLabel="2024"
              comparisonLabel="2029"
              primaryColor="#6366f1"
              comparisonColor="#16a34a"
              fileBaseName="income-distribution-preview"
            />
          </section>
        </div>
      </div>
    </main>
  );
}
