"use client";

/**
 * Dynamic Leaflet map — must be client-only (no SSR) because Leaflet
 * requires the browser's window object.
 */

import "leaflet/dist/leaflet.css";
import { useEffect, useRef } from "react";
import { CompetitorSchool } from "@/lib/types";

interface Props {
  lat: number;
  lon: number;
  radiusMiles: number;
  schools: CompetitorSchool[];
  schoolName: string;
  isochronePolygon?: object | null;
  catchmentType?: string;
  catchmentMinutes?: number | null;
  ministryType?: "schools" | "housing" | "elder_care";
}

export default function SchoolMap({
  lat,
  lon,
  radiusMiles,
  schools,
  schoolName,
  isochronePolygon,
  catchmentType = "radius",
  catchmentMinutes,
  ministryType = "schools",
}: Props) {
  const mapRef = useRef<HTMLDivElement>(null);
  const mapInstanceRef = useRef<unknown>(null);

  useEffect(() => {
    // Dynamically import Leaflet to avoid SSR issues
    let L: typeof import("leaflet");
    let map: import("leaflet").Map;

    async function initMap() {
      L = (await import("leaflet")).default;

      // Fix default marker icon paths broken by webpack
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      delete (L.Icon.Default.prototype as any)._getIconUrl;
      L.Icon.Default.mergeOptions({
        iconRetinaUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png",
        iconUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png",
        shadowUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png",
      });

      if (!mapRef.current || mapInstanceRef.current) return;

      map = L.map(mapRef.current).setView([lat, lon], 11);
      mapInstanceRef.current = map;

      L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: '&copy; <a href="https://openstreetmap.org/copyright">OpenStreetMap</a>',
      }).addTo(map);

      // Draw catchment boundary — isochrone polygon preferred, circle fallback
      if (isochronePolygon && catchmentType === "isochrone") {
        L.geoJSON(
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          { type: "Feature", geometry: isochronePolygon, properties: {} } as any,
          {
            style: {
              color: "#172d57",
              fillColor: "#172d57",
              fillOpacity: 0.06,
              weight: 2,
              dashArray: "6 4",
            },
          }
        ).addTo(map);
      } else {
        // Fallback: simple radius circle
        L.circle([lat, lon], {
          radius: radiusMiles * 1609.34,
          color: "#172d57",
          fillColor: "#172d57",
          fillOpacity: 0.05,
          weight: 2,
          dashArray: "6 4",
        }).addTo(map);
      }

      // Subject school pin
      const subjectIcon = L.divIcon({
        className: "",
        html: `<div style="
          width:20px;height:20px;
          background:#172d57;
          border:3px solid white;
          border-radius:50%;
          box-shadow:0 2px 6px rgba(0,0,0,0.4);
        "></div>`,
        iconSize: [20, 20],
        iconAnchor: [10, 10],
      });

      L.marker([lat, lon], { icon: subjectIcon })
        .addTo(map)
        .bindPopup(`<strong>${schoolName}</strong><br><em>Analysis center</em>`, { maxWidth: 200 });

      // Competitor pins
      for (const school of schools) {
        const color = ministryType === "schools" && school.is_catholic ? "#3b82f6" : "#9ca3af";
        const icon = L.divIcon({
          className: "",
          html: `<div style="
            width:12px;height:12px;
            background:${color};
            border:2px solid white;
            border-radius:50%;
            box-shadow:0 1px 4px rgba(0,0,0,0.3);
          "></div>`,
          iconSize: [12, 12],
          iconAnchor: [6, 6],
        });

        L.marker([school.lat, school.lon], { icon })
          .addTo(map)
          .bindPopup(
            `<strong>${school.name}</strong><br>
            ${school.affiliation}<br>
            ${school.distance_miles} mi away
            ${school.enrollment ? `<br>${school.enrollment.toLocaleString()} ${ministryType === "schools" ? "students" : ministryType === "housing" ? "units" : "beds"}` : ""}`,
            { maxWidth: 220 }
          );
      }
    }

    initMap();

    return () => {
      if (mapInstanceRef.current) {
        (mapInstanceRef.current as import("leaflet").Map).remove();
        mapInstanceRef.current = null;
      }
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const catchmentLabel =
    catchmentType === "isochrone" && catchmentMinutes
      ? `${catchmentMinutes}-min drive catchment`
      : `${radiusMiles}-mile radius`;

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
      <div className="px-6 pt-5 pb-3">
        <h2 className="text-lg font-bold text-gray-900">Market Map</h2>
        <p className="text-xs text-gray-400">
          {catchmentLabel} · {ministryType === "schools"
            ? "private schools from NCES PSS 2021–22"
            : ministryType === "housing"
            ? "affordable housing projects in analysis catchment"
            : "elder care facilities in analysis catchment"}
          {catchmentType === "isochrone" && (
            <span className="ml-1 text-blue-500 font-medium">· road-network isochrone</span>
          )}
        </p>
      </div>
      <div ref={mapRef} className="h-80 w-full" />
      <div className="px-6 py-3 border-t border-gray-100 flex gap-4 text-xs text-gray-500">
        <span className="flex items-center gap-1.5">
          <span className="w-3 h-3 rounded-full bg-[#172d57] border-2 border-white inline-block" />
          Analysis center
        </span>
        {ministryType === "schools" && (
          <span className="flex items-center gap-1.5">
            <span className="w-3 h-3 rounded-full bg-blue-500 border-2 border-white inline-block" />
            Catholic school
          </span>
        )}
        <span className="flex items-center gap-1.5">
          <span className="w-3 h-3 rounded-full bg-gray-400 border-2 border-white inline-block" />
          {ministryType === "schools" ? "Other private" : ministryType === "housing" ? "Housing project" : "Elder care facility"}
        </span>
      </div>
    </div>
  );
}
