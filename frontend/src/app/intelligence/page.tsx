"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { fetchOpportunities, fetchPipelineStatus, fetchPortfolioWorkspace, fetchScoringWeights, updatePortfolioWorkspaceCandidates } from "@/lib/api";
import { MinistryType, OpportunityRecord, PipelineStatusResponse, ScoringWeightsResponse } from "@/lib/types";
import { formatPercentile, freshnessBadgeClass } from "@/lib/intelligence";

const PREFILL_ANALYSIS_KEY = "intelligence_prefill_analysis";
const PREFILL_COMPARE_KEY = "intelligence_prefill_compare";
const PORTFOLIO_WORKSPACE_KEY = "portfolio_workspace_id";

const ministryOptions: { value: MinistryType; label: string }[] = [
  { value: "schools", label: "Schools" },
  { value: "housing", label: "Housing" },
  { value: "elder_care", label: "Elder Care" },
];

export default function IntelligencePage() {
  const [pipelineStatus, setPipelineStatus] = useState<PipelineStatusResponse | null>(null);
  const [opportunities, setOpportunities] = useState<OpportunityRecord[]>([]);
  const [weights, setWeights] = useState<ScoringWeightsResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loadState, setLoadState] = useState<"idle" | "loading" | "db-disabled">("idle");
  const [methodologyOpen, setMethodologyOpen] = useState(false);
  const [filters, setFilters] = useState({ ministryType: "schools" as MinistryType, state: "", minScore: 65, limit: 25 });
  const [sortBy, setSortBy] = useState<"overall_score" | "percentile_state" | "percentile_national">("overall_score");
  const [actionMessage, setActionMessage] = useState<string | null>(null);

  async function loadConsole(currentFilters = filters) {
    setLoadState("loading");
    setError(null);

    try {
      const [statusRes, oppsRes, weightsRes] = await Promise.all([
        fetchPipelineStatus(),
        fetchOpportunities({ ministryType: currentFilters.ministryType, state: currentFilters.state || undefined, minScore: currentFilters.minScore, limit: currentFilters.limit }),
        fetchScoringWeights(),
      ]);
      setPipelineStatus(statusRes);
      setOpportunities(oppsRes);
      setWeights(weightsRes);
      setLoadState("idle");
    } catch (e) {
      const message = e instanceof Error ? e.message : "Unable to load intelligence data.";
      if (message.toLowerCase().includes("database not enabled") || message.includes("USE_DB=true")) {
        setLoadState("db-disabled");
      } else {
        setError(message);
        setLoadState("idle");
      }
    }
  }

  const sortedOpportunities = useMemo(() => [...opportunities].sort((a, b) => (b[sortBy] ?? -1) - (a[sortBy] ?? -1)), [opportunities, sortBy]);

  useEffect(() => {
    loadConsole();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function saveCandidate(opportunity: OpportunityRecord) {
    const workspaceId = window.localStorage.getItem(PORTFOLIO_WORKSPACE_KEY);
    if (!workspaceId) {
      setActionMessage("No portfolio workspace found in this browser. Create/select one before saving candidates.");
      return;
    }

    try {
      const current = await fetchPortfolioWorkspace(workspaceId);
      const response = await updatePortfolioWorkspaceCandidates(workspaceId, [
        ...current.candidate_locations,
        {
          name: `${opportunity.geoid} (${opportunity.ministry_type})`,
          address: `Census tract ${opportunity.geoid}`,
          notes: `Opportunity score ${opportunity.overall_score}`,
        },
      ]);
      setActionMessage(`Saved to workspace ${response.engagement_name}.`);
    } catch {
      setActionMessage("Unable to save candidate to workspace. Verify workspace id in local storage.");
    }
  }

  function prefillAnalysis(opportunity: OpportunityRecord) {
    window.localStorage.setItem(PREFILL_ANALYSIS_KEY, JSON.stringify({ school_name: `Opportunity ${opportunity.geoid}`, address: `Census tract ${opportunity.geoid}`, ministry_type: opportunity.ministry_type }));
    window.location.href = "/";
  }

  function prefillCompare(opportunity: OpportunityRecord) {
    window.localStorage.setItem(PREFILL_COMPARE_KEY, JSON.stringify({ school_name: `Opportunity ${opportunity.geoid}`, address: `Census tract ${opportunity.geoid}`, ministry_types: [opportunity.ministry_type] }));
    setActionMessage("Compare payload prepared. Use this in the compare workflow when available.");
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white border-b border-gray-200">
        <div className="max-w-6xl mx-auto px-4 sm:px-6 py-4 flex items-center justify-between">
          <div>
            <h1 className="text-lg font-bold text-gray-900">Intelligence Console</h1>
            <p className="text-sm text-gray-500">Operational health, discovery opportunities, and scoring methodology.</p>
          </div>
          <div className="flex gap-2">
            <Link href="/dashboard-preview" className="px-3 py-2 text-sm rounded-lg bg-indigo-50 border border-indigo-200 text-indigo-700">
              Dashboard Preview
            </Link>
            <button onClick={() => loadConsole()} className="px-3 py-2 text-sm rounded-lg bg-gray-900 text-white">Refresh</button>
            <Link href="/" className="px-3 py-2 text-sm rounded-lg bg-white border border-gray-200 text-gray-700">Back to Analysis</Link>
          </div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 sm:px-6 py-8 space-y-6">
        {loadState === "db-disabled" ? (
          <div className="rounded-xl border border-amber-300 bg-amber-50 p-6">
            <h2 className="text-lg font-semibold text-amber-900">Console unavailable in non-DB mode</h2>
            <p className="text-sm text-amber-800 mt-2">This environment is running with USE_DB=false, so pipeline and opportunities endpoints return 501 by design. Enable USE_DB=true to use operational health and discovery views.</p>
          </div>
        ) : (
          <>
            {error && <div className="rounded-lg border border-red-200 bg-red-50 text-red-700 px-4 py-3 text-sm">{error}</div>}

            {pipelineStatus && (
              <section className="bg-white border border-gray-200 rounded-2xl p-5">
                <div className="flex items-center justify-between mb-4">
                  <h2 className="text-base font-semibold text-gray-900">Pipeline Health</h2>
                  {pipelineStatus.retry_recommended && <span className="text-xs font-semibold px-2 py-1 rounded bg-red-100 text-red-700">Retry Recommended</span>}
                </div>
                <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-3 mb-4">
                  {Object.entries(pipelineStatus.record_counts).map(([key, value]) => (
                    <div key={key} className="rounded-lg border border-gray-100 bg-gray-50 p-3">
                      <p className="text-xs uppercase tracking-wide text-gray-500">{key.replaceAll("_", " ")}</p>
                      <p className="text-lg font-semibold text-gray-900">{value.toLocaleString()}</p>
                    </div>
                  ))}
                </div>
                <div className="space-y-3">
                  {Object.entries(pipelineStatus.pipelines).map(([name, detail]) => (
                    <div key={name} className={`rounded-xl border p-4 ${detail.freshness_status === "stale" ? "border-red-300 bg-red-50" : "border-gray-200 bg-white"}`}>
                      <div className="flex items-center justify-between">
                        <p className="font-medium text-gray-900">{name.replaceAll("_", " ")}</p>
                        <span className={`text-xs px-2 py-1 rounded-full font-semibold ${freshnessBadgeClass(detail.freshness_status)}`}>{detail.freshness_status}</span>
                      </div>
                      <div className="mt-2 grid sm:grid-cols-4 gap-2 text-sm text-gray-600">
                        <p>Freshness: <strong>{detail.freshness_hours ?? "N/A"}h</strong></p>
                        <p>Processed: <strong>{detail.records_processed ?? "—"}</strong></p>
                        <p>Inserted: <strong>{detail.records_inserted ?? "—"}</strong></p>
                        <p>Last success: <strong>{detail.last_success ? new Date(detail.last_success).toLocaleString() : "Never"}</strong></p>
                      </div>
                      {detail.last_failure.error_message && <p className="mt-2 text-xs text-red-700">Last failure: {detail.last_failure.error_message}</p>}
                    </div>
                  ))}
                </div>

                {pipelineStatus.hud_ingest && (
                  <div className="mt-5">
                    <h3 className="text-sm font-semibold text-gray-900 mb-2">HUD Normalized Ingest</h3>
                    <div className="grid md:grid-cols-3 gap-3">
                      {Object.entries(pipelineStatus.hud_ingest).map(([family, detail]) => (
                        <div key={family} className={`rounded-xl border p-4 ${detail.status === "failed" ? "border-red-300 bg-red-50" : "border-gray-200 bg-white"}`}>
                          <div className="flex items-center justify-between">
                            <p className="font-medium text-gray-900">{family.replaceAll("_", " ")}</p>
                            <span className={`text-xs px-2 py-1 rounded-full font-semibold ${detail.status === "success" ? "bg-emerald-100 text-emerald-700" : detail.status === "failed" ? "bg-red-100 text-red-700" : "bg-gray-100 text-gray-700"}`}>
                              {detail.status}
                            </span>
                          </div>
                          <div className="mt-2 space-y-1 text-xs text-gray-600">
                            <p>Year: <strong>{detail.dataset_year ?? "—"}</strong></p>
                            <p>Version: <strong>{detail.source_version ?? "—"}</strong></p>
                            <p>Finished: <strong>{detail.finished_at ? new Date(detail.finished_at).toLocaleString() : "Never"}</strong></p>
                          </div>
                          {detail.error_message && <p className="mt-2 text-xs text-red-700">{detail.error_message}</p>}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </section>
            )}

            <section className="bg-white border border-gray-200 rounded-2xl p-5">
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-base font-semibold text-gray-900">Opportunities Explorer</h2>
                <select value={sortBy} onChange={(e) => setSortBy(e.target.value as typeof sortBy)} className="text-sm border border-gray-200 rounded-lg px-2 py-1">
                  <option value="overall_score">Sort: Overall score</option>
                  <option value="percentile_state">Sort: State percentile</option>
                  <option value="percentile_national">Sort: National percentile</option>
                </select>
              </div>

              <div className="grid sm:grid-cols-4 gap-3 mb-4">
                <select value={filters.ministryType} onChange={(e) => setFilters((s) => ({ ...s, ministryType: e.target.value as MinistryType }))} className="border border-gray-200 rounded-lg px-3 py-2 text-sm">{ministryOptions.map((m) => <option key={m.value} value={m.value}>{m.label}</option>)}</select>
                <input value={filters.state} onChange={(e) => setFilters((s) => ({ ...s, state: e.target.value }))} placeholder="State FIPS (optional)" className="border border-gray-200 rounded-lg px-3 py-2 text-sm" />
                <input type="number" value={filters.minScore} onChange={(e) => setFilters((s) => ({ ...s, minScore: Number(e.target.value) }))} className="border border-gray-200 rounded-lg px-3 py-2 text-sm" />
                <input type="number" value={filters.limit} onChange={(e) => setFilters((s) => ({ ...s, limit: Number(e.target.value) }))} className="border border-gray-200 rounded-lg px-3 py-2 text-sm" />
              </div>
              <button onClick={() => loadConsole(filters)} className="mb-4 text-sm rounded-lg px-3 py-2 bg-gray-900 text-white">Apply Filters</button>

              <div className="overflow-x-auto">
                <table className="min-w-full text-sm">
                  <thead><tr className="text-left text-gray-500 border-b"><th className="py-2 pr-3">GeoID</th><th className="py-2 pr-3">Score</th><th className="py-2 pr-3">Percentiles</th><th className="py-2 pr-3">Context</th><th className="py-2 pr-3">Actions</th></tr></thead>
                  <tbody>
                    {sortedOpportunities.map((o) => (
                      <tr key={o.geoid} className="border-b border-gray-100 align-top">
                        <td className="py-3 pr-3 font-medium text-gray-900">{o.geoid}</td>
                        <td className="py-3 pr-3">{o.overall_score}</td>
                        <td className="py-3 pr-3">{formatPercentile(o.percentile_state)} state · {formatPercentile(o.percentile_national)} national</td>
                        <td className="py-3 pr-3 text-xs text-gray-500">Market {o.market_size_score} · Income {o.income_score} · Competition {o.competition_score} · Family density {o.family_density_score}</td>
                        <td className="py-3 pr-3"><div className="flex flex-col gap-1"><button onClick={() => prefillAnalysis(o)} className="text-xs text-left text-blue-700 hover:underline">Prefill new analysis</button><button onClick={() => prefillCompare(o)} className="text-xs text-left text-blue-700 hover:underline">Prefill compare flow</button><button onClick={() => saveCandidate(o)} className="text-xs text-left text-blue-700 hover:underline">Save as portfolio candidate</button></div></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {sortedOpportunities.length === 0 && <p className="text-sm text-gray-500 py-4">No opportunities found. Adjust filters and refresh.</p>}
              </div>
              {actionMessage && <p className="mt-3 text-xs text-gray-600">{actionMessage}</p>}
            </section>

            <section className="bg-white border border-gray-200 rounded-2xl p-5">
              <button onClick={() => setMethodologyOpen((v) => !v)} className="w-full flex items-center justify-between">
                <h2 className="text-base font-semibold text-gray-900">Methodology</h2>
                <span className="text-sm text-gray-500">{methodologyOpen ? "Hide" : "Show"} weighting logic</span>
              </button>
              {methodologyOpen && weights && (
                <div className="mt-4 grid md:grid-cols-2 gap-4">
                  {Object.entries(weights)
                    .filter(([key]) => key !== "hierarchical_by_ministry")
                    .map(([ministry, factors]) => {
                      if (!factors || typeof factors !== "object") return null;
                      return (
                        <div key={ministry} className="rounded-xl border border-gray-100 bg-gray-50 p-4">
                          <h3 className="font-semibold text-gray-900 capitalize mb-2">{ministry.replaceAll("_", " ")}</h3>
                          <ul className="space-y-2">{Object.entries(factors as Record<string, unknown>).map(([factor, config]) => {
                            if (config && typeof config === "object" && "weight" in config) {
                              const entry = config as { weight: number; description: string };
                              return <li key={factor} className="text-sm text-gray-700"><span className="font-medium capitalize">{factor.replaceAll("_", " ")}</span>: {(entry.weight * 100).toFixed(0)}% — {entry.description}</li>;
                            }
                            if (config && typeof config === "object") {
                              return (
                                <li key={factor} className="text-sm text-gray-700">
                                  <span className="font-medium capitalize">{factor.replaceAll("_", " ")}</span>
                                  <ul className="ml-4 mt-1 space-y-1">{Object.entries(config as Record<string, { weight: number; description: string }>).map(([subFactor, subConfig]) => <li key={subFactor}><span className="font-medium capitalize">{subFactor.replaceAll("_", " ")}</span>: {(subConfig.weight * 100).toFixed(0)}% — {subConfig.description}</li>)}</ul>
                                </li>
                              );
                            }
                            return null;
                          })}</ul>
                        </div>
                      );
                    })}

                  {weights.hierarchical_by_ministry && Object.entries(weights.hierarchical_by_ministry).map(([moduleName, moduleFactors]) => (
                    <div key={`hier-${moduleName}`} className="rounded-xl border border-purple-100 bg-purple-50 p-4">
                      <h3 className="font-semibold text-gray-900 mb-2">Hierarchical · {moduleName.replaceAll("_", " ")}</h3>
                      <ul className="space-y-2">{Object.entries(moduleFactors).map(([factor, config]) => <li key={factor} className="text-sm text-gray-700"><span className="font-medium capitalize">{factor.replaceAll("_", " ")}</span>: {(config.weight * 100).toFixed(0)}% — {config.description}</li>)}</ul>
                    </div>
                  ))}
                </div>
              )}
            </section>
          </>
        )}
      </main>
    </div>
  );
}
