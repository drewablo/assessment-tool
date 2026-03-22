"use client";

import dynamic from "next/dynamic";
import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  ChoroplethMap,
  DashboardSidebar,
  DistributionChart,
  ParameterBar,
  TabbedSubview,
  TrendChart,
  ZipDrilldownCard,
} from "@/components/dashboard";
import CompetitorTable from "@/components/CompetitorTable";
import CatchmentComparisonView from "@/components/dashboard/CatchmentComparisonView";
import PartnerFacilityTable from "@/components/PartnerFacilityTable";
import { DashboardPreviewModule } from "@/lib/dashboard-preview-data";

const WhatIfSimulator = dynamic(() => import("@/components/WhatIfSimulator"), { ssr: false });

interface Props {
  config: DashboardPreviewModule;
  embedded?: boolean;
  backHref?: string;
  backLabel?: string;
}

const TREND_TABS = new Set([
  "summary",
  "projections",
  "population_trend",
  "market_size",
  "median_income",
  "change_average",
  "cohort_breakdown",
  "age_cohorts",
  "renter_owner",
  "age_distribution",
  "poverty_rate",
  "care_implications",
]);

const MAP_TABS = new Set([
  "summary",
  "map_view",
  "median_income",
  "high_income",
  "market_size",
  "competitor_overlap",
  "service_map",
  "subsidized_map",
  "competitor_map",
]);

const DRILLDOWN_TABS = new Set([
  "summary",
  "map_view",
  "drilldown",
  "median_income",
  "high_income",
  "market_size",
  "competitor_overlap",
  "service_map",
  "subsidized_map",
  "competitor_map",
]);

const DISTRIBUTION_TABS = new Set([
  "summary",
  "distribution",
  "catholic_affiliation",
  "supply_gap",
  "poverty_rate",
  "care_implications",
]);

const COMPETITOR_TABLE_TABS = new Set(["competitor_overlap", "competitor_table", "project_table"]);
const PARTNER_TABLE_TABS = new Set(["potential_partners"]);

export default function ModuleDashboardView({ config, embedded = false, backHref, backLabel }: Props) {
  const [activeSidebar, setActiveSidebar] = useState(config.sidebarItems[0]?.key ?? "");
  const [activeTab, setActiveTab] = useState(config.tabs[0]?.key ?? "");
  const [metricKey, setMetricKey] = useState(config.metricOptions[0]?.key ?? "");
  const [selectedZip, setSelectedZip] = useState("");
  const drilldownRef = useRef<HTMLDivElement>(null);

  const activeView = config.sidebarViews?.[activeSidebar];
  const activeTabView = activeView?.tabViews?.[activeTab];
  const currentTabs = activeView?.tabs ?? config.tabs;
  const currentMetricOptions = activeView?.metricOptions ?? config.metricOptions;
  const currentZipDrilldowns = activeView?.zipDrilldowns ?? config.zipDrilldowns;
  const currentHighlightCards = activeTabView?.highlightCards ?? activeView?.highlightCards ?? config.highlightCards;
  const currentTrendTitle = activeTabView?.trendTitle ?? activeView?.trendTitle ?? config.trendTitle;
  const currentTrendSubtitle = activeTabView?.trendSubtitle ?? activeView?.trendSubtitle ?? config.trendSubtitle;
  const currentTrendSeries = activeTabView?.trendSeries ?? activeView?.trendSeries ?? config.trendSeries;
  const currentTrendData = activeTabView?.trendData ?? activeView?.trendData ?? config.trendData;
  const currentDistributionTitle = activeTabView?.distributionTitle ?? activeView?.distributionTitle ?? config.distributionTitle;
  const currentDistributionSubtitle = activeTabView?.distributionSubtitle ?? activeView?.distributionSubtitle ?? config.distributionSubtitle;
  const currentDistributionData = activeTabView?.distributionData ?? activeView?.distributionData ?? config.distributionData;
  const currentDistributionPrimaryLabel =
    activeTabView?.distributionPrimaryLabel ?? activeView?.distributionPrimaryLabel ?? "2024";
  const currentDistributionComparisonLabel =
    activeTabView?.distributionComparisonLabel ?? activeView?.distributionComparisonLabel ?? "2029";
  const currentDistributionReferenceLine =
    activeView?.distributionReferenceLine ?? config.distributionReferenceLine;
  const currentCallout = activeView?.callout;

  useEffect(() => {
    const nextTab = currentTabs[0]?.key ?? "";
    if (!currentTabs.some((tab) => tab.key === activeTab)) {
      setActiveTab(nextTab);
    }
  }, [activeTab, currentTabs]);

  useEffect(() => {
    const nextMetric = currentMetricOptions[0]?.key ?? "";
    if (!currentMetricOptions.some((item) => item.key === metricKey)) {
      setMetricKey(nextMetric);
    }
  }, [currentMetricOptions, metricKey]);

  useEffect(() => {
    const tabMetricMap: Record<string, string> = {
      competitor_map: "competitorCount",
      competitor_overlap: "competitorCount",
      high_income: "medianFamilyIncome",
      market_size: "schoolAgePopulation",
      median_income: "medianFamilyIncome",
      service_map: "facilityCount",
      subsidized_map: "hudEligibleHouseholds",
    };

    const targetMetric = tabMetricMap[activeTab];
    if (targetMetric && currentMetricOptions.some((item) => item.key === targetMetric) && targetMetric !== metricKey) {
      setMetricKey(targetMetric);
    }
  }, [activeTab, currentMetricOptions, metricKey]);

  const effectiveSelectedZip = useMemo(() => {
    const zipKeys = Object.keys(currentZipDrilldowns);
    if (zipKeys.length === 0) return "";
    return selectedZip && selectedZip in currentZipDrilldowns ? selectedZip : zipKeys[0];
  }, [currentZipDrilldowns, selectedZip]);

  useEffect(() => {
    if (effectiveSelectedZip !== selectedZip) {
      setSelectedZip(effectiveSelectedZip);
    }
  }, [effectiveSelectedZip, selectedZip]);

  const selectedMetric = useMemo(
    () => currentMetricOptions.find((item) => item.key === metricKey) ?? currentMetricOptions[0],
    [currentMetricOptions, metricKey],
  );

  const zipData = effectiveSelectedZip ? currentZipDrilldowns[effectiveSelectedZip] : undefined;
  const showScenarioModeler =
    activeSidebar === "enrollment" && activeTab === "enrollment_scenarios" && config.slug === "schools" && config.analysisResult;
  const showCatchmentComparison =
    activeSidebar === "student_body" && activeTab === "catchment_enrollment" && config.slug === "schools";
  const showPartnerTable =
    activeView?.tableVariant === "partner" &&
    (PARTNER_TABLE_TABS.has(activeTab) || (currentTabs.length === 1 && activeTab === currentTabs[0]?.key)) &&
    (config.competitors?.length ?? 0) > 0;
  const showCompetitorTable =
    activeView?.tableVariant !== "partner" &&
    (config.competitors?.length ?? 0) > 0 &&
    (activeSidebar === "competitors" || COMPETITOR_TABLE_TABS.has(activeTab));

  const showTrendChart = TREND_TABS.has(activeTab);
  const showMapPanel = MAP_TABS.has(activeTab);
  const showDrilldownPanel = DRILLDOWN_TABS.has(activeTab) && Object.keys(currentZipDrilldowns).length > 0;
  const showDistributionChart = DISTRIBUTION_TABS.has(activeTab);
  const hasFocusedContent =
    showTrendChart ||
    showMapPanel ||
    showDrilldownPanel ||
    showDistributionChart ||
    showCompetitorTable ||
    showPartnerTable ||
    Boolean(showScenarioModeler) ||
    showCatchmentComparison;
  const showFallbackSummary = !hasFocusedContent && currentTabs[0]?.key === activeTab;

  const wrapperClassName = `${embedded ? "rounded-2xl bg-[#f7f7fc] p-3 sm:p-4" : "min-h-screen bg-[#f7f7fc] px-4 py-6"} text-slate-900`;

  const content = (
    <div className={`mx-auto ${embedded ? "max-w-full" : "max-w-[1600px]"} space-y-5`}>
      <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-semibold tracking-tight text-slate-900">{config.title}</h1>
          {!embedded && (
            <Link href={backHref ?? "/"} className="text-xs font-medium text-indigo-600 hover:text-indigo-800">
              ← {backLabel ?? "Back to analysis"}
            </Link>
          )}
        </div>
        <ParameterBar
          driveTimeMinutes={config.driveTimeMinutes}
          address={config.address ?? ""}
          primaryLabel={config.primaryLabel}
          primaryValue={config.primaryValue}
          secondaryLabel={config.secondaryLabel}
          secondaryValue={config.secondaryValue}
          zipCount={config.zipCount}
          parameterFields={config.parameterFields}
        />
      </div>

      <div className="flex flex-wrap gap-3">
        {currentHighlightCards.map((card) => (
          <div key={card.label} className="flex items-baseline gap-2 rounded-xl border border-slate-200 bg-white px-3 py-2 shadow-sm">
            <span className="text-lg font-bold tracking-tight text-slate-900">{card.value}</span>
            <span className="text-xs text-slate-500">{card.label}</span>
          </div>
        ))}
      </div>

      <div className="grid gap-5 xl:grid-cols-[240px_minmax(0,1fr)]">
        <DashboardSidebar items={config.sidebarItems} activeKey={activeSidebar} onSelect={setActiveSidebar} />

        <section className="space-y-4">
          <div className="flex flex-col gap-3 rounded-2xl border border-slate-200 bg-white px-5 py-3 shadow-sm lg:flex-row lg:items-center lg:justify-between">
            <div>
              <h2 className="text-lg font-semibold text-slate-900">
                {activeView?.title ?? config.sidebarItems.find((item) => item.key === activeSidebar)?.title ?? config.title}
              </h2>
              <p className="text-xs text-slate-500">
                {activeView?.description ?? config.sidebarItems.find((item) => item.key === activeSidebar)?.description}
              </p>
            </div>
            <TabbedSubview tabs={currentTabs} activeKey={activeTab} onChange={setActiveTab} />
          </div>

          {currentCallout ? (
            <div
              className={`rounded-2xl border px-4 py-3 text-sm shadow-sm ${
                currentCallout.tone === "warning"
                  ? "border-amber-200 bg-amber-50 text-amber-900"
                  : "border-sky-200 bg-sky-50 text-sky-900"
              }`}
            >
              <p className="font-semibold">{currentCallout.title}</p>
              <p className="mt-1">{currentCallout.body}</p>
            </div>
          ) : null}

          {(showTrendChart || showFallbackSummary) ? (
            <TrendChart
              title={currentTrendTitle}
              subtitle={currentTrendSubtitle}
              data={currentTrendData}
              series={currentTrendSeries}
              fileBaseName={`${config.slug}-trend`}
            />
          ) : null}

          {(showMapPanel || showFallbackSummary) ? (
            <div className="grid gap-6">
              <ChoroplethMap
                title={`${selectedMetric?.label ?? "Market"} by ZIP Code`}
                subtitle="Click a ZIP to open the drilldown card."
                featureCollection={config.featureCollection}
                metric={selectedMetric}
                availableMetrics={currentMetricOptions}
                selectedZip={effectiveSelectedZip}
                onMetricChange={setMetricKey}
                onZipSelect={(zipCode) => {
                  setSelectedZip(zipCode);
                  requestAnimationFrame(() => {
                    drilldownRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
                  });
                }}
                fileBaseName={`${config.slug}-choropleth`}
                competitors={config.competitors}
                ministryType={config.slug === "elder-care" ? "elder_care" : config.slug}
                centerLabel={config.address}
                centerLat={config.centerLat}
                centerLon={config.centerLon}
                radiusMiles={config.radiusMiles}
                boundaryOverlays={config.boundaryOverlays}
              />

              {(showDrilldownPanel || showFallbackSummary) ? (
                zipData ? (
                  <div ref={drilldownRef}>
                    <ZipDrilldownCard data={zipData} defaultOpen />
                  </div>
                ) : effectiveSelectedZip ? (
                  <div
                    ref={drilldownRef}
                    className="rounded-2xl border border-dashed border-slate-300 bg-white p-6 text-sm text-slate-500 shadow-sm"
                  >
                    No drilldown data available for ZIP {effectiveSelectedZip}. This ZIP may have insufficient census tract coverage.
                  </div>
                ) : (
                  <div
                    ref={drilldownRef}
                    className="rounded-2xl border border-dashed border-slate-300 bg-white p-6 text-sm text-slate-500 shadow-sm"
                  >
                    Select a ZIP on the map to open the drilldown card.
                  </div>
                )
              ) : null}
            </div>
          ) : null}

          {(showDistributionChart || showFallbackSummary) ? (
            <DistributionChart
              title={currentDistributionTitle}
              subtitle={currentDistributionSubtitle}
              data={currentDistributionData}
              primaryLabel={currentDistributionPrimaryLabel}
              comparisonLabel={currentDistributionComparisonLabel}
              primaryColor="#6366f1"
              comparisonColor="#16a34a"
              referenceLine={currentDistributionReferenceLine}
              fileBaseName={`${config.slug}-distribution`}
            />
          ) : null}

          {showCompetitorTable && config.competitorCounts ? (
            <CompetitorTable
              schools={config.competitors ?? []}
              catholicCount={config.competitorCounts.catholicCount}
              totalPrivateCount={config.competitorCounts.totalPrivateCount}
              radiusMiles={config.competitorCounts.radiusMiles}
              ministryType={config.slug === "elder-care" ? "elder_care" : config.slug}
            />
          ) : null}

          {showPartnerTable ? <PartnerFacilityTable facilities={config.competitors ?? []} /> : null}

          {showScenarioModeler && config.analysisResult ? <WhatIfSimulator result={config.analysisResult} /> : null}

          {showCatchmentComparison ? (
            <CatchmentComparisonView
              schoolAgePopulation={config.analysisResult?.demographics.school_age_population}
              estimatedCatholicSchoolAge={config.analysisResult?.demographics.estimated_catholic_school_age}
              estimatedCatholicPct={config.analysisResult?.demographics.estimated_catholic_pct}
            />
          ) : null}
        </section>
      </div>
    </div>
  );

  return embedded ? <div className={wrapperClassName}>{content}</div> : <main className={wrapperClassName}>{content}</main>;
}
