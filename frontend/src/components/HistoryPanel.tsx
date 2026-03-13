"use client";

import { useEffect, useState } from "react";
import { AnalysisHistoryRecord, AnalysisRequest, MinistryType } from "@/lib/types";
import { fetchHistory, deleteHistoryRecord } from "@/lib/api";
import { Clock, Trash2, RefreshCw, ChevronDown, ChevronUp } from "lucide-react";

interface Props {
  onRestore?: (req: Partial<AnalysisRequest>) => void;
}

function scoreColor(score: number | null): string {
  if (score === null) return "bg-gray-100 text-gray-500";
  if (score >= 75) return "bg-green-100 text-green-800";
  if (score >= 55) return "bg-yellow-100 text-yellow-800";
  if (score >= 35) return "bg-orange-100 text-orange-800";
  return "bg-red-100 text-red-800";
}

const MINISTRY_LABELS: Record<MinistryType, string> = {
  schools: "School",
  housing: "Housing",
  elder_care: "Elder Care",
};

export default function HistoryPanel({ onRestore }: Props) {
  const [records, setRecords] = useState<AnalysisHistoryRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState(false);
  const [filter, setFilter] = useState<MinistryType | "all">("all");
  const [deletingId, setDeletingId] = useState<number | null>(null);

  async function load() {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchHistory(filter === "all" ? undefined : filter, 0, 50);
      setRecords(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not load history. Ensure the backend has USE_DB=true.");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (expanded) {
      load();
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [expanded, filter]);

  async function handleDelete(id: number) {
    setDeletingId(id);
    try {
      await deleteHistoryRecord(id);
      setRecords((prev) => prev.filter((r) => r.id !== id));
    } catch {
      // silently ignore
    } finally {
      setDeletingId(null);
    }
  }

  function handleRestore(r: AnalysisHistoryRecord) {
    if (!onRestore) return;

    const fallback: Partial<AnalysisRequest> = {
      school_name: r.school_name,
      address: r.address,
      ministry_type: r.ministry_type,
    };

    onRestore(r.request_params ?? fallback);
  }

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
      {/* Toggle header */}
      <button
        className="w-full flex items-center justify-between px-5 py-4 text-left hover:bg-gray-50 transition-colors"
        onClick={() => setExpanded((v) => !v)}
        aria-expanded={expanded}
      >
        <div className="flex items-center gap-2">
          <Clock className="w-4 h-4 text-gray-400" />
          <span className="font-semibold text-sm text-gray-800">Analysis History</span>
          {records.length > 0 && (
            <span className="text-xs bg-gray-100 text-gray-600 rounded-full px-2 py-0.5 font-medium">
              {records.length}
            </span>
          )}
        </div>
        {expanded ? (
          <ChevronUp className="w-4 h-4 text-gray-400" />
        ) : (
          <ChevronDown className="w-4 h-4 text-gray-400" />
        )}
      </button>

      {expanded && (
        <div className="border-t border-gray-100 p-5 space-y-4">
          {/* Filter + refresh */}
          <div className="flex items-center justify-between gap-2 flex-wrap">
            <div className="flex gap-1.5">
              {(["all", "schools", "housing", "elder_care"] as const).map((f) => (
                <button
                  key={f}
                  onClick={() => setFilter(f)}
                  className={`text-xs px-3 py-1 rounded-full font-medium transition-colors ${
                    filter === f ? "bg-gray-900 text-white" : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                  }`}
                >
                  {f === "all" ? "All" : MINISTRY_LABELS[f as MinistryType]}
                </button>
              ))}
            </div>
            <button
              onClick={load}
              disabled={loading}
              className="flex items-center gap-1 text-xs text-gray-500 hover:text-gray-700 transition-colors disabled:opacity-50"
              aria-label="Refresh history"
            >
              <RefreshCw className={`w-3 h-3 ${loading ? "animate-spin" : ""}`} />
              Refresh
            </button>
          </div>

          {error && (
            <p className="text-xs text-amber-700 bg-amber-50 border border-amber-200 rounded-lg px-3 py-2">
              {error}
            </p>
          )}

          {!error && !loading && records.length === 0 && (
            <p className="text-sm text-gray-400 text-center py-6">No past analyses found.</p>
          )}

          {loading && records.length === 0 && (
            <div className="space-y-2 animate-pulse">
              {[1, 2, 3].map((i) => (
                <div key={i} className="h-12 bg-gray-100 rounded-lg" />
              ))}
            </div>
          )}

          {records.length > 0 && (
            <ul className="divide-y divide-gray-50 -mx-1">
              {records.map((r) => (
                <li key={r.id} className="flex items-center gap-3 px-1 py-2.5 hover:bg-gray-50 rounded-lg transition-colors group">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="text-sm font-medium text-gray-800 truncate">{r.school_name}</span>
                      <span className="text-xs text-gray-400 bg-gray-100 rounded px-1.5 py-0.5">
                        {MINISTRY_LABELS[r.ministry_type] ?? r.ministry_type}
                      </span>
                      {r.overall_score !== null && (
                        <span className={`text-xs font-semibold rounded px-1.5 py-0.5 ${scoreColor(r.overall_score)}`}>
                          {r.overall_score}
                        </span>
                      )}
                    </div>
                    <p className="text-xs text-gray-400 mt-0.5 truncate">{r.address}</p>
                    <p className="text-xs text-gray-300">
                      {new Date(r.created_at).toLocaleDateString(undefined, {
                        month: "short",
                        day: "numeric",
                        year: "numeric",
                      })}
                    </p>
                  </div>
                  <div className="flex items-center gap-1 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-opacity">
                    {onRestore && (
                      <button
                        onClick={() => handleRestore(r)}
                        className="text-xs px-2.5 py-1 rounded bg-gray-100 text-gray-700 hover:bg-gray-200 transition-colors font-medium"
                        title="Re-run this analysis"
                      >
                        Load
                      </button>
                    )}
                    <button
                      onClick={() => handleDelete(r.id)}
                      disabled={deletingId === r.id}
                      className="p-1.5 rounded text-gray-300 hover:text-red-500 hover:bg-red-50 transition-colors disabled:opacity-40"
                      aria-label="Delete record"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}
