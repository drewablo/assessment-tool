"use client";

import { memo, useMemo, useState } from "react";
import { ChevronDown, ChevronUp, ChevronsUpDown } from "lucide-react";
import type { CompetitorSchool } from "@/lib/types";

type SortCol = "name" | "ownership" | "careType" | "distance_miles" | "enrollment" | "mds_overall_rating";
type SortDir = "asc" | "desc";
type Preset = "all" | "mission_aligned" | "rated" | "memory_care";

interface Props {
  facilities: CompetitorSchool[];
}

function ownershipBucket(value: string | null | undefined) {
  const text = (value ?? "").toLowerCase();
  if (text.includes("non-profit") || text.includes("nonprofit") || text.includes("faith") || text.includes("church") || text.includes("religious")) return "Non-profit";
  if (text.includes("government") || text.includes("county") || text.includes("state licensed") || text.includes("state ") || text.includes("municipal")) return "Public";
  if (text.includes("for profit") || text.includes("for-profit") || text.includes("corporation") || text.includes("llc") || text.includes("inc.") || text.includes("partnership")) return "For-profit";
  return "Unknown";
}

function careTypeLabel(value: string | null | undefined) {
  const text = (value ?? "").replaceAll("_", " ").trim();
  if (!text) return "Unspecified";
  return text
    .split(" ")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function SortHeader({
  label,
  col,
  sortCol,
  sortDir,
  onSort,
}: {
  label: string;
  col: SortCol;
  sortCol: SortCol;
  sortDir: SortDir;
  onSort: (col: SortCol) => void;
}) {
  const active = sortCol === col;
  const Icon = active ? (sortDir === "asc" ? ChevronUp : ChevronDown) : ChevronsUpDown;

  return (
    <th className="cursor-pointer pb-2 text-left text-xs font-medium text-slate-500 transition-colors hover:text-slate-700" onClick={() => onSort(col)}>
      <span className="inline-flex items-center gap-1">
        {label}
        <Icon className={`h-3 w-3 ${active ? "text-slate-700" : "text-slate-300"}`} />
      </span>
    </th>
  );
}

function PartnerFacilityTableInner({ facilities }: Props) {
  const [preset, setPreset] = useState<Preset>("all");
  const [sortCol, setSortCol] = useState<SortCol>("mds_overall_rating");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const operatorCounts = useMemo(() => {
    return facilities.reduce<Record<string, number>>((counts, facility) => {
      const key = facility.affiliation || facility.name;
      counts[key] = (counts[key] ?? 0) + 1;
      return counts;
    }, {});
  }, [facilities]);

  const rows = useMemo(() => {
    const filtered = facilities.filter((facility) => {
      if (preset === "mission_aligned") return ownershipBucket(facility.affiliation) === "Non-profit";
      if (preset === "rated") return (facility.mds_overall_rating ?? 0) >= 4;
      if (preset === "memory_care") return careTypeLabel(facility.grade_level).toLowerCase().includes("memory");
      return true;
    });

    return [...filtered].sort((left, right) => {
      const values: Record<SortCol, [string | number, string | number]> = {
        name: [left.name.toLowerCase(), right.name.toLowerCase()],
        ownership: [ownershipBucket(left.affiliation), ownershipBucket(right.affiliation)],
        careType: [careTypeLabel(left.grade_level), careTypeLabel(right.grade_level)],
        distance_miles: [left.distance_miles, right.distance_miles],
        enrollment: [left.enrollment ?? -1, right.enrollment ?? -1],
        mds_overall_rating: [left.mds_overall_rating ?? -1, right.mds_overall_rating ?? -1],
      };
      const [a, b] = values[sortCol];
      if (a === b) return 0;
      const comparison = a < b ? -1 : 1;
      return sortDir === "asc" ? comparison : -comparison;
    });
  }, [facilities, preset, sortCol, sortDir]);

  function handleSort(col: SortCol) {
    if (sortCol === col) {
      setSortDir((current) => (current === "asc" ? "desc" : "asc"));
      return;
    }
    setSortCol(col);
    setSortDir(col === "distance_miles" ? "asc" : "desc");
  }

  const highlightCount = rows.filter((facility) => ownershipBucket(facility.affiliation) === "Non-profit").length;

  return (
    <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
      <div className="mb-4 flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <h3 className="text-2xl font-semibold tracking-tight text-slate-950">Potential Partners</h3>
          <p className="mt-1 text-sm text-slate-500">
            Review facilities by ownership profile, care type, bed count, rating, and whether the operator appears to run multiple nearby sites.
          </p>
        </div>
        <div className="flex flex-wrap gap-2 text-xs">
          <span className="rounded-full bg-emerald-50 px-3 py-1 font-medium text-emerald-700">{highlightCount} mission-aligned</span>
          <span className="rounded-full bg-slate-100 px-3 py-1 font-medium text-slate-700">{rows.length} candidate facilities</span>
        </div>
      </div>

      <div className="mb-4 flex flex-wrap gap-2">
        {[
          { key: "all", label: "All facilities" },
          { key: "mission_aligned", label: "Mission aligned" },
          { key: "rated", label: "Rated 4+" },
          { key: "memory_care", label: "Memory care" },
        ].map((option) => (
          <button
            key={option.key}
            type="button"
            onClick={() => setPreset(option.key as Preset)}
            className={`rounded-full px-3 py-1 text-xs font-medium transition-colors ${
              preset === option.key ? "bg-slate-900 text-white" : "bg-slate-100 text-slate-600 hover:bg-slate-200"
            }`}
          >
            {option.label}
          </button>
        ))}
      </div>

      <div className="overflow-x-auto">
        <table className="w-full min-w-[760px] text-sm">
          <thead>
            <tr className="border-b border-slate-100">
              <SortHeader label="Facility" col="name" sortCol={sortCol} sortDir={sortDir} onSort={handleSort} />
              <SortHeader label="Ownership" col="ownership" sortCol={sortCol} sortDir={sortDir} onSort={handleSort} />
              <SortHeader label="Care type" col="careType" sortCol={sortCol} sortDir={sortDir} onSort={handleSort} />
              <th className="pb-2 text-left text-xs font-medium text-slate-500">Operator footprint</th>
              <SortHeader label="Distance" col="distance_miles" sortCol={sortCol} sortDir={sortDir} onSort={handleSort} />
              <SortHeader label="Beds" col="enrollment" sortCol={sortCol} sortDir={sortDir} onSort={handleSort} />
              <SortHeader label="CMS rating" col="mds_overall_rating" sortCol={sortCol} sortDir={sortDir} onSort={handleSort} />
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {rows.map((facility) => {
              const owner = facility.affiliation || "Unknown";
              const ownership = ownershipBucket(owner);
              const operatorCount = operatorCounts[owner] ?? 1;
              return (
                <tr key={`${facility.name}-${facility.distance_miles}`} className="align-top">
                  <td className="py-3 pr-4">
                    <div className="font-medium text-slate-900">{facility.name}</div>
                    <div className="text-xs text-slate-500">{facility.city ?? "Unknown city"}</div>
                  </td>
                  <td className="py-3 pr-4 text-slate-600">
                    <div>{ownership}</div>
                    <div className="text-xs text-slate-500">{owner}</div>
                  </td>
                  <td className="py-3 pr-4 text-slate-600">{careTypeLabel(facility.grade_level)}</td>
                  <td className="py-3 pr-4 text-slate-600">
                    {operatorCount > 1 ? `Multi-site (${operatorCount})` : "Single-site"}
                  </td>
                  <td className="py-3 pr-4 text-slate-600">{facility.distance_miles.toFixed(1)} mi</td>
                  <td className="py-3 pr-4 text-slate-600">{facility.enrollment?.toLocaleString() ?? "—"}</td>
                  <td className="py-3 text-slate-600">
                    {facility.mds_overall_rating != null ? `${facility.mds_overall_rating} / 5` : "Not rated"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {rows.length === 0 ? (
        <p className="mt-4 text-sm text-slate-500">No facilities match the current partnership preset.</p>
      ) : null}
    </div>
  );
}

export default memo(PartnerFacilityTableInner);
