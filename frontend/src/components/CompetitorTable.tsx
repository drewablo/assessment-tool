"use client";

import { CompetitorSchool } from "@/lib/types";
import { memo, useMemo, useState } from "react";
import { ChevronUp, ChevronDown, ChevronsUpDown, ChevronLeft, ChevronRight } from "lucide-react";

const PAGE_SIZE = 15;

interface Props {
  schools: CompetitorSchool[];
  catholicCount: number;
  totalPrivateCount: number;
  radiusMiles: number;
  catchmentLabel?: string;
  ministryType?: "schools" | "housing" | "elder_care";
}

type SortCol = "name" | "affiliation" | "grade_level" | "distance_miles" | "enrollment" | "mds_overall_rating";
type SortDir = "asc" | "desc";

function SortHeader({
  label,
  col,
  sortCol,
  sortDir,
  onSort,
  right = false,
}: {
  label: string;
  col: SortCol;
  sortCol: SortCol | null;
  sortDir: SortDir;
  onSort: (col: SortCol) => void;
  right?: boolean;
}) {
  const active = sortCol === col;
  const Icon = active ? (sortDir === "asc" ? ChevronUp : ChevronDown) : ChevronsUpDown;
  return (
    <th
      className={`pb-2 font-medium cursor-pointer select-none hover:text-gray-700 transition-colors ${right ? "text-right" : ""}`}
      onClick={() => onSort(col)}
    >
      <span className={`inline-flex items-center gap-0.5 ${right ? "flex-row-reverse" : ""}`}>
        {label}
        <Icon className={`w-3 h-3 flex-shrink-0 ${active ? "text-gray-700" : "text-gray-300"}`} />
      </span>
    </th>
  );
}

function CompetitorTableInner({ schools, catholicCount, totalPrivateCount, radiusMiles, catchmentLabel, ministryType = "schools" }: Props) {
  const [filter, setFilter] = useState<"all" | "catholic" | "other">("all");
  const [sortCol, setSortCol] = useState<SortCol | null>("distance_miles");
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [page, setPage] = useState(0);

  function handleSort(col: SortCol) {
    setPage(0);
    if (sortCol === col) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortCol(col);
      setSortDir("asc");
    }
  }

  function handleFilter(f: "all" | "catholic" | "other") {
    setFilter(f);
    setPage(0);
  }

  const sorted = useMemo(() => {
    const filtered = schools.filter((s) => {
      if (filter === "catholic") return s.is_catholic;
      if (filter === "other") return !s.is_catholic;
      return true;
    });
    return [...filtered].sort((a, b) => {
      if (!sortCol) return 0;
      let va: string | number | null;
      let vb: string | number | null;
      if (sortCol === "distance_miles") {
        va = a.distance_miles;
        vb = b.distance_miles;
      } else if (sortCol === "enrollment") {
        va = a.enrollment ?? -1;
        vb = b.enrollment ?? -1;
      } else if (sortCol === "mds_overall_rating") {
        va = a.mds_overall_rating ?? -1;
        vb = b.mds_overall_rating ?? -1;
      } else {
        va = (a[sortCol] as string).toLowerCase();
        vb = (b[sortCol] as string).toLowerCase();
      }
      if (va === vb) return 0;
      const cmp = va < vb ? -1 : 1;
      return sortDir === "asc" ? cmp : -cmp;
    });
  }, [schools, filter, sortCol, sortDir]);

  const totalPages = Math.ceil(sorted.length / PAGE_SIZE);
  const paginated = sorted.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-4">
        <div>
          <h2 className="text-lg font-bold text-gray-900">{ministryType === "schools" ? "Competitor Schools" : ministryType === "housing" ? "Competing Housing Projects" : "Competing Elder Care Facilities"}</h2>
          <p className="text-xs text-gray-400">
            {ministryType === "schools" ? "NCES Private School Survey 2021–22" : ministryType === "housing" ? "HUD LIHTC inventory" : "CMS Care Compare inventory"} · within {catchmentLabel ?? `${radiusMiles} miles`}
          </p>
        </div>
        <div className="flex gap-2 text-sm">
          <span className="bg-blue-100 text-blue-800 font-semibold px-2 py-1 rounded">
            {ministryType === "schools" ? `${catholicCount} Catholic` : `${totalPrivateCount} Total`}
          </span>
          {ministryType === "schools" && (
            <span className="bg-gray-100 text-gray-700 font-semibold px-2 py-1 rounded">
              {totalPrivateCount - catholicCount} Other private
            </span>
          )}
        </div>
      </div>

      {schools.length === 0 ? (
        <p className="text-sm text-gray-500 text-center py-8">
          {ministryType === "schools"
            ? "No private school data available for this area."
            : ministryType === "housing"
            ? "No affordable housing projects found in this catchment."
            : "No elder care facilities found in this catchment."}
        </p>
      ) : (
        <>
          <div className="flex gap-2 mb-3">
            {(ministryType === "schools" ? (["all", "catholic", "other"] as const) : (["all"] as const)).map((f) => (
              <button
                key={f}
                onClick={() => handleFilter(f)}
                className={`text-xs px-3 py-1 rounded-full font-medium transition-colors ${
                  filter === f
                    ? "bg-gray-900 text-white"
                    : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                }`}
              >
                {f === "all" ? "All" : f === "catholic" ? "Catholic" : "Other Private"}
              </button>
            ))}
          </div>

          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs text-gray-400 border-b border-gray-100">
                  <SortHeader label={ministryType === "schools" ? "School" : "Facility"} col="name" sortCol={sortCol} sortDir={sortDir} onSort={handleSort} />
                  {(ministryType === "elder_care" || ministryType === "housing") && (
                    <SortHeader label={ministryType === "housing" ? "Source" : "Owner"} col="affiliation" sortCol={sortCol} sortDir={sortDir} onSort={handleSort} />
                  )}
                  <SortHeader label={ministryType === "schools" ? "Grade Level" : "Type"}  col="grade_level"   sortCol={sortCol} sortDir={sortDir} onSort={handleSort} />
                  <th className="pb-2 font-medium">{ministryType === "schools" ? "Gender" : "Category"}</th>
                  <SortHeader label="Distance"     col="distance_miles" sortCol={sortCol} sortDir={sortDir} onSort={handleSort} right />
                  <SortHeader label={ministryType === "schools" ? "Enrollment" : ministryType === "housing" ? "LI Units" : "Licensed Beds"}   col="enrollment"    sortCol={sortCol} sortDir={sortDir} onSort={handleSort} right />
                  {ministryType === "elder_care" && (
                    <SortHeader label="Rating" col="mds_overall_rating" sortCol={sortCol} sortDir={sortDir} onSort={handleSort} right />
                  )}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {paginated.map((school, i) => (
                  <tr key={i} className="hover:bg-gray-50 transition-colors">
                    <td className="py-2.5 pr-4">
                      <div className="flex items-center gap-1.5">
                        <span
                          className={`inline-block w-2 h-2 rounded-full flex-shrink-0 ${
                            school.is_catholic ? "bg-blue-500" : "bg-gray-300"
                          }`}
                        />
                        <span className="font-medium text-gray-800 text-xs">{school.name}</span>
                      </div>
                      {school.city && <div className="text-xs text-gray-400 ml-3.5">{school.city}</div>}
                    </td>
                    {(ministryType === "elder_care" || ministryType === "housing") && (
                      <td className="py-2.5 pr-4 text-xs text-gray-500">{school.affiliation}</td>
                    )}
                    <td className="py-2.5 pr-4 text-xs text-gray-500">{school.grade_level}</td>
                    <td className="py-2.5 pr-4 text-xs text-gray-500">
                      {ministryType === "housing" ? (
                        <span className="inline-flex items-center gap-1 flex-wrap">
                          {school.affiliation?.includes("QCT") ? (
                            <span className="px-1.5 py-0.5 rounded bg-purple-100 text-purple-800 text-[10px] font-semibold">QCT</span>
                          ) : null}
                          {school.affiliation?.includes("DDA") ? (
                            <span className="px-1.5 py-0.5 rounded bg-teal-100 text-teal-800 text-[10px] font-semibold">DDA</span>
                          ) : null}
                          {!school.affiliation?.includes("QCT") && !school.affiliation?.includes("DDA") ? "Standard" : null}
                        </span>
                      ) : school.gender}
                    </td>
                    <td className="py-2.5 text-right text-xs text-gray-600 whitespace-nowrap">
                      {school.distance_miles} mi
                    </td>
                    <td className="py-2.5 text-right text-xs text-gray-600">
                      {school.enrollment?.toLocaleString() ?? "—"}
                    </td>
                    {ministryType === "elder_care" && (
                      <td className="py-2.5 text-right text-xs text-gray-600">
                        {school.mds_overall_rating != null
                          ? "★".repeat(school.mds_overall_rating) + "☆".repeat(5 - school.mds_overall_rating)
                          : "—"}
                      </td>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>

            {sorted.length === 0 && (
              <p className="text-sm text-gray-400 text-center py-6">No schools match this filter.</p>
            )}
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between mt-3 pt-3 border-t border-gray-100">
              <span className="text-xs text-gray-400">
                {page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, sorted.length)} of {sorted.length}
              </span>
              <div className="flex items-center gap-1">
                <button
                  onClick={() => setPage((p) => Math.max(0, p - 1))}
                  disabled={page === 0}
                  className="p-1 rounded hover:bg-gray-100 disabled:opacity-30 transition-colors"
                  aria-label="Previous page"
                >
                  <ChevronLeft className="w-4 h-4" />
                </button>
                <span className="text-xs text-gray-600 px-2">
                  {page + 1} / {totalPages}
                </span>
                <button
                  onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
                  disabled={page === totalPages - 1}
                  className="p-1 rounded hover:bg-gray-100 disabled:opacity-30 transition-colors"
                  aria-label="Next page"
                >
                  <ChevronRight className="w-4 h-4" />
                </button>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}

export default memo(CompetitorTableInner);
