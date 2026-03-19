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
import type { CompetitorSchool } from "@/lib/types";

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
  competitors?: CompetitorSchool[];
  ministryType?: "schools" | "housing" | "elder_care";
  centerLabel?: string;
  centerLat?: number;
  centerLon?: number;
  boundaryOverlays?: FeatureCollection;
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
  competitors,
  ministryType = "schools",
  centerLabel,
  centerLat,
  centerLon,
  boundaryOverlays,
}: Props) {
  const shellRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<HTMLDivElement>(null);
  const mapInstanceRef = useRef<import("leaflet").Map | null>(null);
  const layerRef = useRef<import("leaflet").GeoJSON | null>(null);
  const markerLayerRef = useRef<import("leaflet").LayerGroup | null>(null);
  const overlayRef = useRef<import("leaflet").GeoJSON | null>(null);
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

      // --- QCT/DDA boundary overlays ---
      overlayRef.current?.remove();
      if (boundaryOverlays && boundaryOverlays.features.length > 0) {
        overlayRef.current = L.geoJSON(boundaryOverlays as GeoJsonObject, {
          style: (feature) => {
            const isQCT = feature?.properties?.designation_type === "QCT";
            return {
              color: isQCT ? "#dc2626" : "#2563eb",
              weight: 2,
              dashArray: "6 4",
              fillColor: isQCT ? "#dc2626" : "#2563eb",
              fillOpacity: 0.08,
            };
          },
          onEachFeature: (feature, layer) => {
            const dtype = feature.properties?.designation_type ?? "Unknown";
            const name = feature.properties?.area_name ?? feature.properties?.geoid11 ?? "";
            layer.bindTooltip(`${dtype}: ${name}`);
          },
        }).addTo(mapInstanceRef.current!);
      }

      // --- Competitor / facility markers ---
      markerLayerRef.current?.clearLayers();
      markerLayerRef.current = L.layerGroup().addTo(mapInstanceRef.current);

      // Center pin (analysis subject)
      if (centerLat != null && centerLon != null) {
        const centerIcon = L.divIcon({
          className: "",
          html: `<div style="width:18px;height:18px;background:#172d57;border:3px solid white;border-radius:50%;box-shadow:0 2px 6px rgba(0,0,0,0.4);"></div>`,
          iconSize: [18, 18],
          iconAnchor: [9, 9],
        });
        L.marker([centerLat, centerLon], { icon: centerIcon })
          .addTo(markerLayerRef.current)
          .bindPopup(`<strong>${centerLabel ?? "Analysis center"}</strong>`);
      }

      if (competitors && competitors.length > 0) {
        for (const comp of competitors) {
          if (!Number.isFinite(comp.lat) || !Number.isFinite(comp.lon)) continue;
          const isSection202 = comp.affiliation === "HUD Section 202";
          const color =
            ministryType === "schools" && comp.is_catholic
              ? "#3b82f6"
              : isSection202
              ? "#f59e0b"
              : "#9ca3af";
          const icon = L.divIcon({
            className: "",
            html: `<div style="width:10px;height:10px;background:${color};border:2px solid white;border-radius:50%;box-shadow:0 1px 3px rgba(0,0,0,0.3);"></div>`,
            iconSize: [10, 10],
            iconAnchor: [5, 5],
          });

          let popup = `<strong>${comp.name}</strong><br>${comp.affiliation}<br>${comp.distance_miles} mi`;
          if (isSection202) {
            const addr = [comp.street_address, comp.city, comp.state, comp.zip_code].filter(Boolean);
            if (addr.length) popup += `<br><span style="color:#666">${addr.join(", ")}</span>`;
            if (comp.enrollment) popup += `<br>${comp.enrollment.toLocaleString()} assisted units`;
            if (comp.total_units) popup += ` / ${comp.total_units.toLocaleString()} total`;
          } else if (comp.enrollment) {
            const unit = ministryType === "schools" ? "students" : ministryType === "housing" ? "units" : "beds";
            popup += `<br>${comp.enrollment.toLocaleString()} ${unit}`;
          }

          L.marker([comp.lat, comp.lon], { icon })
            .addTo(markerLayerRef.current!)
            .bindPopup(popup, { maxWidth: 260 });
        }
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
      markerLayerRef.current = null;
      overlayRef.current = null;
    };
  }, [featureCollection, metric, onZipSelect, selectedZip, competitors, ministryType, centerLabel, centerLat, centerLon, boundaryOverlays]);

  const isEmpty = featureCollection.features.length === 0;

  return (
    <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
      <div className="mb-6 flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div className="min-w-0">
          <h3 className="text-xl font-semibold tracking-tight text-slate-950">{title}</h3>
          {subtitle ? <p className="mt-1 text-sm text-slate-500">{subtitle}</p> : null}
        </div>
        <div className="flex shrink-0 flex-wrap items-center gap-3">
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
          <div className="flex h-[520px] w-full items-center justify-center rounded-[24px] border border-dashed border-slate-200 bg-slate-50 px-6 text-center text-sm text-slate-500">
            ZIP geometry is not available for this catchment yet. Populate the ZCTA cache and rerun the dashboard to render the choropleth.
          </div>
        ) : (
          <div ref={mapRef} className="h-[520px] w-full overflow-hidden rounded-[24px] border border-slate-100" />
        )}
        <div className="flex flex-wrap items-center justify-between gap-4 text-sm text-slate-500">
          <div className="flex items-center gap-3">
            <span className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-400">Low</span>
            <div className="h-3 w-32 rounded-full bg-gradient-to-r from-indigo-100 via-indigo-300 to-indigo-800" />
            <span className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-400">High</span>
          </div>
          <p>Click a ZIP to drive the drilldown card.</p>
        </div>
        {(competitors?.length ?? 0) > 0 && (
          <div className="flex flex-wrap items-center gap-4 text-xs text-slate-500">
            {centerLat != null && (
              <span className="flex items-center gap-1.5">
                <span className="inline-block h-3 w-3 rounded-full border-2 border-white bg-[#172d57]" />
                Analysis center
              </span>
            )}
            {ministryType === "schools" && (
              <span className="flex items-center gap-1.5">
                <span className="inline-block h-3 w-3 rounded-full border-2 border-white bg-blue-500" />
                Catholic school
              </span>
            )}
            <span className="flex items-center gap-1.5">
              <span className="inline-block h-3 w-3 rounded-full border-2 border-white bg-gray-400" />
              {ministryType === "schools" ? "Other private" : ministryType === "housing" ? "Housing project" : "Elder care facility"}
            </span>
            {ministryType === "housing" && competitors?.some((c) => c.affiliation === "HUD Section 202") && (
              <span className="flex items-center gap-1.5">
                <span className="inline-block h-3 w-3 rounded-full border-2 border-white bg-amber-400" />
                HUD Section 202
              </span>
            )}
          </div>
        )}
        {(boundaryOverlays?.features?.length ?? 0) > 0 && (
          <div className="flex flex-wrap items-center gap-4 text-xs text-slate-500">
            <span className="flex items-center gap-1.5">
              <span className="inline-block h-3 w-5 rounded border-2 border-dashed border-red-500 bg-red-500/10" />
              QCT boundary
            </span>
            <span className="flex items-center gap-1.5">
              <span className="inline-block h-3 w-5 rounded border-2 border-dashed border-blue-500 bg-blue-500/10" />
              DDA boundary
            </span>
          </div>
        )}
      </div>
    </div>
  );
}

export default memo(ChoroplethMap);
