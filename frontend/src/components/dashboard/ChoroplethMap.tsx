"use client";

import "leaflet/dist/leaflet.css";
import { memo, useEffect, useMemo, useRef } from "react";
import type { FeatureCollection, GeoJsonObject } from "geojson";
import ChartActionBar from "./ChartActionBar";
import {
  DashboardMetricOption,
  downloadCsv,
  downloadElementAsPng,
  formatDashboardValue,
} from "@/lib/dashboard";

interface Props {
  title: string;
  subtitle?: string;
  featureCollection: FeatureCollection;
  metric: DashboardMetricOption;
  availableMetrics: DashboardMetricOption[];
  selectedZip?: string | null;
  onMetricChange: (key: string) => void;
  onZipSelect?: (zipCode: string) => void;
  fileBaseName?: string;
}

function getColor(value: number, min: number, max: number) {
  if (!Number.isFinite(value)) return "#e2e8f0";
  const range = max - min || 1;
  const ratio = Math.max(0, Math.min(1, (value - min) / range));
  const lightness = 92 - ratio * 45;
  return `hsl(238 76% ${lightness}%)`;
}

function ChoroplethMap({
  title,
  subtitle,
  featureCollection,
  metric,
  availableMetrics,
  selectedZip,
  onMetricChange,
  onZipSelect,
  fileBaseName = "choropleth-map",
}: Props) {
  const shellRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<HTMLDivElement>(null);
  const mapInstanceRef = useRef<import("leaflet").Map | null>(null);
  const layerRef = useRef<import("leaflet").GeoJSON | null>(null);
  const metricRows = useMemo(
    () =>
      featureCollection.features.map((feature) => ({
        zip_code: String(feature.properties?.zipCode ?? feature.properties?.zip ?? ""),
        metric: metric.label,
        value: Number(feature.properties?.[metric.key]),
      })),
    [featureCollection.features, metric.key, metric.label],
  );

  useEffect(() => {
    let mounted = true;

    async function init() {
      if (!mapRef.current || featureCollection.features.length === 0) return;
      const L = (await import("leaflet")).default;
      if (!mounted) return;

      if (!mapInstanceRef.current) {
        mapInstanceRef.current = L.map(mapRef.current, { scrollWheelZoom: false }).setView([26.58, -81.86], 10);
        L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
          attribution: '&copy; <a href="https://openstreetmap.org/copyright">OpenStreetMap</a>',
        }).addTo(mapInstanceRef.current);
      }

      const values = featureCollection.features
        .map((feature) => Number(feature.properties?.[metric.key]))
        .filter((value) => Number.isFinite(value));
      const min = values.length ? Math.min(...values) : 0;
      const max = values.length ? Math.max(...values) : 1;

      layerRef.current?.remove();
      layerRef.current = L.geoJSON(featureCollection as GeoJsonObject, {
        style: (feature) => {
          const raw = Number(feature?.properties?.[metric.key]);
          const zip = String(feature?.properties?.zipCode ?? feature?.properties?.zip ?? "");
          return {
            color: selectedZip === zip ? "#0f172a" : "#ffffff",
            weight: selectedZip === zip ? 2.5 : 1.2,
            fillColor: getColor(raw, min, max),
            fillOpacity: 0.72,
          };
        },
        onEachFeature: (feature, layer) => {
          const zip = String(feature.properties?.zipCode ?? feature.properties?.zip ?? "");
          const label = feature.properties?.name ?? zip;
          const value = Number(feature.properties?.[metric.key]);
          layer.bindTooltip(`${label}: ${formatDashboardValue(value, metric.format)}`);
          layer.on("click", () => onZipSelect?.(zip));
        },
      }).addTo(mapInstanceRef.current);

      const bounds = layerRef.current.getBounds();
      if (bounds.isValid()) {
        mapInstanceRef.current.fitBounds(bounds.pad(0.08));
      }
    }

    void init();

    return () => {
      mounted = false;
      if (mapInstanceRef.current) {
        mapInstanceRef.current.remove();
        mapInstanceRef.current = null;
      }
      layerRef.current = null;
    };
  }, [featureCollection, metric, onZipSelect, selectedZip]);

  const isEmpty = featureCollection.features.length === 0;

  return (
    <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
      <div className="mb-6 flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
        <div>
          <h3 className="text-2xl font-semibold tracking-tight text-slate-950">{title}</h3>
          {subtitle ? <p className="mt-1 text-sm text-slate-500">{subtitle}</p> : null}
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <select
            value={metric.key}
            onChange={(event) => onMetricChange(event.target.value)}
            className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm"
          >
            {availableMetrics.map((item) => (
              <option key={item.key} value={item.key}>{item.label}</option>
            ))}
          </select>
          <ChartActionBar
            onDownloadPng={() => downloadElementAsPng(`${fileBaseName}.png`, shellRef.current)}
            onDownloadCsv={() => downloadCsv(`${fileBaseName}.csv`, metricRows)}
          />
        </div>
      </div>

      <div ref={shellRef} className="space-y-4">
        {isEmpty ? (
          <div className="flex h-[420px] w-full items-center justify-center rounded-[24px] border border-dashed border-slate-200 bg-slate-50 px-6 text-center text-sm text-slate-500">
            ZIP geometry is not available for this catchment yet. Populate the ZCTA cache and rerun the dashboard to render the choropleth.
          </div>
        ) : (
          <div ref={mapRef} className="h-[420px] w-full overflow-hidden rounded-[24px] border border-slate-100" />
        )}
        <div className="flex flex-wrap items-center justify-between gap-4 text-sm text-slate-500">
          <div className="flex items-center gap-3">
            <span className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-400">Low</span>
            <div className="h-3 w-32 rounded-full bg-gradient-to-r from-indigo-100 via-indigo-300 to-indigo-800" />
            <span className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-400">High</span>
          </div>
          <p>Click a ZIP to drive the drilldown card.</p>
        </div>
      </div>
    </div>
  );
}

export default memo(ChoroplethMap);
