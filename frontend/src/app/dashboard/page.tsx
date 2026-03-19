"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import ModuleDashboardView from "@/components/dashboard/modules/ModuleDashboardView";
import { fetchDashboard } from "@/lib/api";
import { toDashboardModuleConfig } from "@/lib/dashboard-live";
import { loadDashboardContext } from "@/lib/dashboard-session";
import type { DashboardSessionContext } from "@/lib/dashboard-session";
import type { DashboardResponse } from "@/lib/types";

export default function DashboardPage() {
  const [context, setContext] = useState<DashboardSessionContext | null>(null);
  const [payload, setPayload] = useState<DashboardResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const restored = loadDashboardContext();
    if (restored) {
      setContext(restored);
    } else {
      setError("No analysis context found. Run an analysis first, then open the dashboard from the results page.");
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!context) return;
    let cancelled = false;

    async function load() {
      setLoading(true);
      setError(null);
      try {
        const next = await fetchDashboard(context!.request);
        if (!cancelled) setPayload(next);
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Unable to load dashboard data.");
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, [context]);

  const config = useMemo(
    () => (payload && context ? toDashboardModuleConfig(payload, context.request, context.result) : null),
    [payload, context],
  );

  if (error) {
    return (
      <main className="min-h-screen bg-[#f7f7fc] px-6 py-10 text-slate-900">
        <div className="mx-auto max-w-[800px] space-y-6">
          <div className="rounded-2xl border border-amber-200 bg-amber-50 p-6 shadow-sm">
            <p className="text-sm font-semibold text-amber-900">Dashboard unavailable</p>
            <p className="mt-2 text-sm text-amber-800">{error}</p>
          </div>
          <Link href="/" className="text-sm font-medium text-indigo-600 hover:text-indigo-800">
            ← Back to analysis
          </Link>
        </div>
      </main>
    );
  }

  if (loading) {
    return (
      <main className="min-h-screen bg-[#f7f7fc] px-6 py-10 text-slate-900">
        <div className="mx-auto max-w-[800px]">
          <div className="rounded-2xl border border-gray-200 bg-white p-6 shadow-sm">
            <p className="text-sm font-semibold text-gray-900">Loading market dashboard…</p>
            <p className="mt-2 text-sm text-gray-500">
              Fetching ZIP boundaries, drilldowns, and projected time series for this analysis.
            </p>
          </div>
        </div>
      </main>
    );
  }

  if (!config) {
    return (
      <main className="min-h-screen bg-[#f7f7fc] px-6 py-10 text-slate-900">
        <div className="mx-auto max-w-[800px]">
          <div className="rounded-2xl border border-dashed border-gray-300 bg-white p-6 text-sm text-gray-500 shadow-sm">
            No dashboard payload was returned for this module.
          </div>
          <Link href="/" className="mt-4 inline-block text-sm font-medium text-indigo-600 hover:text-indigo-800">
            ← Back to analysis
          </Link>
        </div>
      </main>
    );
  }

  return <ModuleDashboardView config={config} backHref="/" backLabel="Back to analysis" />;
}
