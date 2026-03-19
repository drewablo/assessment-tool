"use client";

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
import PartnerFacilityTable from "@/components/PartnerFacilityTable";
import { DashboardPreviewModule } from "@/lib/dashboard-preview-data";

interface Props {
  config: DashboardPreviewModule;
  embedded?: boolean;
  backHref?: string;
  backLabel?: string;
}

export default function ModuleDashboardView({ config, embedded = false, backHref, backLabel }: Props) {
  const [activeSidebar, setActiveSidebar] = useState(config.sidebarItems[0]?.key ?? "");
  const [activeTab, setActiveTab] = useState(config.tabs[0]?.key ?? "");
  const [metricKey, setMetricKey] = useState(config.metricOptions[0]?.key ?? "");
  const [selectedZip, setSelectedZip] = useState(Object.keys(config.zipDrilldowns)[0] ?? "");
  const drilldownRef = useRef<HTMLDivElement>(null);

  const activeView = config.sidebarViews?.[activeSidebar];
  const currentTabs = activeView?.tabs ?? config.tabs;
  const currentMetricOptions = activeView?.metricOptions ?? config.metricOptions;
  const currentZipDrilldowns = activeView?.zipDrilldowns ?? config.zipDrilldowns;
  const currentHighlightCards = activeView?.highlightCards ?? config.highlightCards;
  const currentTrendTitle = activeView?.trendTitle ?? config.trendTitle;
  const currentTrendSubtitle = activeView?.trendSubtitle ?? config.trendSubtitle;
  const currentTrendSeries = activeView?.trendSeries ?? config.trendSeries;
  const currentTrendData = activeView?.trendData ?? config.trendData;
  const currentDistributionTitle = activeView?.distributionTitle ?? config.distributionTitle;
  const currentDistributionSubtitle = activeView?.distributionSubtitle ?? config.distributionSubtitle;
  const currentDistributionData = activeView?.distributionData ?? config.distributionData;
  const currentDistributionPrimaryLabel = activeView?.distributionPrimaryLabel ?? "2024";
  const currentDistributionComparisonLabel = activeView?.distributionComparisonLabel ?? "2029";
  const currentDistributionReferenceLine = activeView?.distributionReferenceLine ?? config.distributionReferenceLine;
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
    const zipKeys = Object.keys(currentZipDrilldowns);
    if (zipKeys.length === 0) return;
    if (!(selectedZip in currentZipDrilldowns)) {
      setSelectedZip(zipKeys[0]);
    }
  }, [currentZipDrilldowns, selectedZip]);

  const selectedMetric = useMemo(
    () => currentMetricOptions.find((item) => item.key === metricKey) ?? currentMetricOptions[0],
    [currentMetricOptions, metricKey],
  );

  const zipData = selectedZip ? currentZipDrilldowns[selectedZip] : undefined;
  const showCompetitorTable = activeView?.tableVariant !== "partner" && ["competitors", "market_landscape", "existing_resources", "enrollment"].includes(activeSidebar) && (config.competitors?.length ?? 0) > 0;
  const showPartnerTable = activeView?.tableVariant === "partner" && (config.competitors?.length ?? 0) > 0;

  const wrapperClassName = `${embedded ? "rounded-2xl bg-[#f7f7fc] p-3 sm:p-4" : "min-h-screen bg-[#f7f7fc] px-4 py-6"} text-slate-900`;

  const content = (
    <div className={`mx-auto ${embedded ? "max-w-full" : "max-w-[1600px]"} space-y-5`}>
      {/* --- Compact header: title + parameter chips on one line --- */}
      <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-semibold tracking-tight text-slate-900">{config.title}</h1>
          {!embedded && (
            <Link href={backHref ?? "/dashboard-preview"} className="text-xs font-medium text-indigo-600 hover:text-indigo-800">
              ← {backLabel ?? "Gallery"}
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

      {/* --- Highlight cards: compact inline strip --- */}
      <div className="flex flex-wrap gap-3">
        {currentHighlightCards.map((card) => (
          <div key={card.label} className="flex items-baseline gap-2 rounded-xl border border-slate-200 bg-white px-3 py-2 shadow-sm">
            <span className="text-lg font-bold tracking-tight text-slate-900">{card.value}</span>
            <span className="text-xs text-slate-500">{card.label}</span>
          </div>
        ))}
      </div>

      {/* --- Main content: sidebar + data panels --- */}
      <div className="grid gap-5 xl:grid-cols-[240px_minmax(0,1fr)]">
        <DashboardSidebar items={config.sidebarItems} activeKey={activeSidebar} onSelect={setActiveSidebar} />

        <section className="space-y-4">
          {/* Section header with tabs — compact */}
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

          {/* Trend chart */}
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

          <TrendChart
            title={currentTrendTitle}
            subtitle={currentTrendSubtitle}
            data={currentTrendData}
            series={currentTrendSeries}
            fileBaseName={`${config.slug}-trend`}
          />

          {/* Map + drilldown stacked */}
          <div className="grid gap-6">
            <ChoroplethMap
              title={`${selectedMetric?.label ?? "Market"} by ZIP Code`}
              subtitle="Click a ZIP to open the drilldown card."
              featureCollection={config.featureCollection}
              metric={selectedMetric}
              availableMetrics={currentMetricOptions}
              selectedZip={selectedZip}
              onMetricChange={setMetricKey}
              onZipSelect={(zipCode) => {
                if (zipCode in currentZipDrilldowns) {
                  setSelectedZip(zipCode);
                  requestAnimationFrame(() => {
                    drilldownRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
                  });
                }
              }}
              fileBaseName={`${config.slug}-choropleth`}
              competitors={config.competitors}
              ministryType={config.slug === "elder-care" ? "elder_care" : config.slug}
              centerLabel={config.address}
              centerLat={config.centerLat}
              centerLon={config.centerLon}
            />

            {zipData ? (
              <div ref={drilldownRef}>
                <ZipDrilldownCard data={zipData} defaultOpen />
              </div>
            ) : (
              <div className="rounded-2xl border border-dashed border-slate-300 bg-white p-6 text-sm text-slate-500 shadow-sm">
                Select a ZIP on the map to open the drilldown card.
              </div>
            )}
          </div>

          {/* Distribution chart */}
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

          {/* Competitor table */}
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
        </section>
      </div>
    </div>
  );

  return embedded ? (
    <div className={wrapperClassName}>
      {content}
    </div>
  ) : (
    <main className={wrapperClassName}>
      {content}
    </main>
  );
}
