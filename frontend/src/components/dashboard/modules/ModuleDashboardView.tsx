"use client";

import Link from "next/link";
import { useMemo, useRef, useState } from "react";
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
import { DashboardPreviewModule } from "@/lib/dashboard-preview-data";

interface Props {
  config: DashboardPreviewModule;
  embedded?: boolean;
}

export default function ModuleDashboardView({ config, embedded = false }: Props) {
  const [activeSidebar, setActiveSidebar] = useState(config.sidebarItems[0]?.key ?? "");
  const [activeTab, setActiveTab] = useState(config.tabs[0]?.key ?? "");
  const [metricKey, setMetricKey] = useState(config.metricOptions[0]?.key ?? "");
  const [selectedZip, setSelectedZip] = useState(Object.keys(config.zipDrilldowns)[0] ?? "");
  const drilldownRef = useRef<HTMLDivElement>(null);

  const selectedMetric = useMemo(
    () => config.metricOptions.find((item) => item.key === metricKey) ?? config.metricOptions[0],
    [config.metricOptions, metricKey],
  );

  const zipData = selectedZip ? config.zipDrilldowns[selectedZip] : undefined;
  const showCompetitorTable = ["competitors", "market_landscape"].includes(activeSidebar) && (config.competitors?.length ?? 0) > 0;

  const wrapperClassName = `${embedded ? "rounded-2xl bg-[#f7f7fc] p-3 sm:p-4" : "min-h-screen bg-[#f7f7fc] px-4 py-6"} text-slate-900`;

  const content = (
    <div className={`mx-auto ${embedded ? "max-w-full" : "max-w-[1600px]"} space-y-5`}>
      {/* --- Compact header: title + parameter chips on one line --- */}
      <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-semibold tracking-tight text-slate-900">{config.title}</h1>
          {!embedded && (
            <Link href="/dashboard-preview" className="text-xs font-medium text-indigo-600 hover:text-indigo-800">
              ← Gallery
            </Link>
          )}
        </div>
        <ParameterBar
          driveTimeMinutes={config.driveTimeMinutes}
          address={config.address ?? "15680 Pine Ridge Road, Fort Myers, FL"}
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
        {config.highlightCards.map((card) => (
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
                {config.sidebarItems.find((item) => item.key === activeSidebar)?.title ?? config.title}
              </h2>
              <p className="text-xs text-slate-500">
                {config.sidebarItems.find((item) => item.key === activeSidebar)?.description}
              </p>
            </div>
            <TabbedSubview tabs={config.tabs} activeKey={activeTab} onChange={setActiveTab} />
          </div>

          {/* Trend chart */}
          <TrendChart
            title={config.trendTitle}
            subtitle={config.trendSubtitle}
            data={config.trendData}
            series={config.trendSeries}
            fileBaseName={`${config.slug}-trend`}
          />

          {/* Map + drilldown side by side */}
          <div className="grid gap-6 xl:grid-cols-[minmax(0,1.4fr)_minmax(340px,0.6fr)]">
            <ChoroplethMap
              title={`${selectedMetric?.label ?? "Market"} by ZIP Code`}
              subtitle="Click a ZIP to open the drilldown card."
              featureCollection={config.featureCollection}
              metric={selectedMetric}
              availableMetrics={config.metricOptions}
              selectedZip={selectedZip}
              onMetricChange={setMetricKey}
              onZipSelect={(zipCode) => {
                if (zipCode in config.zipDrilldowns) {
                  setSelectedZip(zipCode);
                  requestAnimationFrame(() => {
                    drilldownRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
                  });
                }
              }}
              fileBaseName={`${config.slug}-choropleth`}
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
            title={config.distributionTitle}
            subtitle={config.distributionSubtitle}
            data={config.distributionData}
            primaryLabel="2024"
            comparisonLabel="2029"
            primaryColor="#6366f1"
            comparisonColor="#16a34a"
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
