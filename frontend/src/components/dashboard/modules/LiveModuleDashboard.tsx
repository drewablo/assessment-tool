"use client";

import { useEffect, useMemo, useState } from "react";
import ModuleDashboardView from "@/components/dashboard/modules/ModuleDashboardView";
import { fetchDashboard } from "@/lib/api";
import { toDashboardModuleConfig } from "@/lib/dashboard-live";
import type { AnalysisRequest, DashboardResponse } from "@/lib/types";

interface Props {
  request: AnalysisRequest;
}

export default function LiveModuleDashboard({ request }: Props) {
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

  const config = useMemo(() => (payload ? toDashboardModuleConfig(payload) : null), [payload]);

  if (loading) {
    return (
      <div className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
        <p className="text-sm font-semibold text-gray-900">Loading live ZIP dashboard…</p>
        <p className="mt-2 text-sm text-gray-500">
          Fetching live ZIP boundaries, drilldowns, and projected time series for this module.
        </p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="rounded-2xl border border-amber-200 bg-amber-50 p-6 shadow-sm">
        <p className="text-sm font-semibold text-amber-900">Live dashboard unavailable</p>
        <p className="mt-2 text-sm text-amber-800">{error}</p>
        <p className="mt-3 text-xs text-amber-700">
          Stage 1 and Stage 2 results remain available above while the dashboard layer degrades gracefully.
        </p>
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

  return <ModuleDashboardView config={config} embedded />;
}
