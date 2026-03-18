"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import {
  ChoroplethMap,
  DashboardSidebar,
  DistributionChart,
  ParameterBar,
  TabbedSubview,
  TrendChart,
  ZipDrilldownCard,
} from "@/components/dashboard";
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

  const selectedMetric = useMemo(
    () => config.metricOptions.find((item) => item.key === metricKey) ?? config.metricOptions[0],
    [config.metricOptions, metricKey],
  );

  const zipData = selectedZip ? config.zipDrilldowns[selectedZip] : undefined;

  const Wrapper = embedded ? "div" : "main";

  return (
    <Wrapper className={`${embedded ? "rounded-[32px] bg-[#f7f7fc] p-4 sm:p-6" : "min-h-screen bg-[#f7f7fc] px-6 py-10"} text-slate-900`}>
      <div className={`mx-auto ${embedded ? "max-w-full" : "max-w-[1440px]"} space-y-8`}>
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div className="space-y-2">
            <p className="text-sm font-semibold uppercase tracking-[0.25em] text-indigo-600">{config.eyebrow}</p>
            <h1 className="text-4xl font-semibold tracking-tight">{config.title}</h1>
            <p className="max-w-3xl text-base leading-7 text-slate-500">{config.description}</p>
          </div>
          {!embedded && (
            <Link href="/dashboard-preview" className="text-sm font-medium text-indigo-700 hover:text-indigo-900">
              Back to dashboard module gallery
            </Link>
          )}
        </div>

        <ParameterBar
          driveTimeMinutes={config.driveTimeMinutes}
          address="15680 Pine Ridge Road, Fort Myers, FL"
          primaryLabel={config.primaryLabel}
          primaryValue={config.primaryValue}
          secondaryLabel={config.secondaryLabel}
          secondaryValue={config.secondaryValue}
          zipCount={config.zipCount}
        />

        <div className="grid gap-4 md:grid-cols-3">
          {config.highlightCards.map((card) => (
            <div key={card.label} className="rounded-[24px] border border-slate-200 bg-white p-5 shadow-sm">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">{card.label}</p>
              <p className="mt-3 text-3xl font-semibold tracking-tight text-slate-950">{card.value}</p>
              <p className="mt-2 text-sm text-slate-500">{card.detail}</p>
            </div>
          ))}
        </div>

        <div className="grid gap-8 xl:grid-cols-[280px_minmax(0,1fr)]">
          <DashboardSidebar items={config.sidebarItems} activeKey={activeSidebar} onSelect={setActiveSidebar} />

          <section className="space-y-6">
            <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
              <div className="flex flex-col gap-4 border-b border-slate-100 pb-5 lg:flex-row lg:items-center lg:justify-between">
                <div>
                  <h2 className="text-4xl font-semibold tracking-tight text-slate-950">{config.sidebarItems.find((item) => item.key === activeSidebar)?.title ?? config.title}</h2>
                  <p className="mt-1 text-sm text-slate-500">{config.sidebarItems.find((item) => item.key === activeSidebar)?.description}</p>
                </div>
                <TabbedSubview tabs={config.tabs} activeKey={activeTab} onChange={setActiveTab} />
              </div>
            </div>

            <TrendChart
              title={config.trendTitle}
              subtitle={config.trendSubtitle}
              data={config.trendData}
              series={config.trendSeries}
              fileBaseName={`${config.slug}-trend`}
            />

            <div className="grid gap-6 2xl:grid-cols-[minmax(0,1.2fr)_minmax(420px,0.8fr)]">
              <ChoroplethMap
                title={`${selectedMetric?.label ?? "Market"} by ZIP Code`}
                subtitle="Shared ZIP-level choropleth component reused across all Phase 2 module dashboards."
                featureCollection={config.featureCollection}
                metric={selectedMetric}
                availableMetrics={config.metricOptions}
                selectedZip={selectedZip}
                onMetricChange={setMetricKey}
                onZipSelect={(zipCode) => {
                  if (zipCode in config.zipDrilldowns) {
                    setSelectedZip(zipCode);
                  }
                }}
                fileBaseName={`${config.slug}-choropleth`}
              />

              {zipData ? (
                <ZipDrilldownCard data={zipData} defaultOpen />
              ) : (
                <div className="rounded-[28px] border border-dashed border-slate-300 bg-white p-8 text-sm text-slate-500 shadow-sm">
                  Select a ZIP on the map to open the drilldown card.
                </div>
              )}
            </div>

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
          </section>
        </div>
      </div>
    </Wrapper>
  );
}
