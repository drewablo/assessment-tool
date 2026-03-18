"use client";

import { useEffect, useMemo, useState } from "react";
import ModuleDashboardView from "@/components/dashboard/modules/ModuleDashboardView";
import { fetchDashboard } from "@/lib/api";
import { toDashboardModuleConfig } from "@/lib/dashboard-live";
import type { AnalysisRequest, AnalysisResponse, DashboardResponse } from "@/lib/types";

interface Props {
  request: AnalysisRequest;
  result?: AnalysisResponse | null;
}

export default function LiveModuleDashboard({ request, result }: Props) {
  const [payload, setPayload] = useState<DashboardResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      setError(null);
      try {
        const next = await fetchDashboard(request);
        if (!cancelled) {
          setPayload(next);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Unable to load live dashboard data.");
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, [request]);

  const config = useMemo(() => (payload ? toDashboardModuleConfig(payload, request, result) : null), [payload, request, result]);

  if (loading) {
    return (
      <div className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <p className="text-sm font-semibold text-gray-900">Loading market dashboard…</p>
        <p className="mt-2 text-sm text-gray-500">
          Fetching ZIP boundaries, drilldowns, and projected time series for this analysis.
        </p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="rounded-2xl border border-amber-200 bg-amber-50 p-6 shadow-sm">
        <p className="text-sm font-semibold text-amber-900">Dashboard temporarily unavailable</p>
        <p className="mt-2 text-sm text-amber-800">{error}</p>
        {error.includes("ingest-zcta") ? (
          <p className="mt-3 text-xs text-amber-700">
            Ask your deployment owner to warm the ZIP boundary cache, then reload this analysis.
          </p>
        ) : null}
      </div>
    );
  }

  if (!config) {
    return (
      <div className="rounded-2xl border border-dashed border-gray-300 bg-white p-6 text-sm text-gray-500 shadow-sm">
        No dashboard payload was returned for this module.
      </div>
    );
  }

  if (payload && payload.catchment.zip_codes.length === 0) {
    return (
      <div className="rounded-2xl border border-dashed border-slate-300 bg-white p-6 text-sm text-slate-600 shadow-sm">
        <p className="font-semibold text-slate-900">No ZIP boundaries are available for this catchment yet</p>
        <p className="mt-2">
          The analysis completed, but ZIP boundary data is unavailable for this catchment in the current environment.
        </p>
        <p className="mt-2 text-xs text-slate-500">
          Geometry source: {payload.metadata.geometry_source ?? "unknown"}.
        </p>
      </div>
    );
  }

  return <ModuleDashboardView config={config} embedded />;
}
