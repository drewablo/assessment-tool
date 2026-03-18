"use client";

import dynamic from "next/dynamic";
import { AnalysisResponse, AnalysisRequest } from "@/lib/types";
import ScoreGauge from "./ScoreGauge";
import MetricCard from "./MetricCard";
import DemographicsPanel from "./DemographicsPanel";
import PopulationGravityPanel from "./PopulationGravityPanel";
import ElderCareGravityPanel from "./ElderCareGravityPanel";
import HousingGravityPanel from "./HousingGravityPanel";
import CompetitorTable from "./CompetitorTable";
import TrendPanel from "./TrendPanel";
import BenchmarkPanel from "./BenchmarkPanel";
import HierarchicalScorePanel from "./HierarchicalScorePanel";
import { Download, FileText, AlertCircle, RefreshCw, CheckCircle2, XCircle, Network } from "lucide-react";
import { exportBoardPack, exportCsv, exportPdf, BoardPackExportResponse } from "@/lib/api";
import { useState } from "react";

// Load Leaflet and heavy panels client-side only (browser APIs / large bundles)
const SchoolMap = dynamic(() => import("./SchoolMap"), { ssr: false });
const WhatIfSimulator = dynamic(() => import("./WhatIfSimulator"), { ssr: false });
const Stage2Dashboard = dynamic(() => import("./Stage2Dashboard"), { ssr: false });
const LiveModuleDashboard = dynamic(() => import("./dashboard/modules/LiveModuleDashboard"), { ssr: false });

const weightingProfileLabels: Record<string, string> = {
  standard_baseline: "Standard baseline",
  affordability_sensitive: "Affordability-sensitive",
  demand_primacy: "Demand-primacy",
};

interface Props {
  result: AnalysisResponse;
  request: AnalysisRequest;
  onReset: () => void;
  onRerun?: (updated: AnalysisRequest) => void;
}


const recommendationColors: Record<string, string> = {
  "Strong Sustainability Conditions": "bg-green-50 border-green-200 text-green-900",
  "Moderate Sustainability Conditions": "bg-yellow-50 border-yellow-200 text-yellow-900",
  "Challenging Market Conditions": "bg-orange-50 border-orange-200 text-orange-900",
  "Difficult Market Conditions": "bg-red-50 border-red-200 text-red-900",
  "Strong Affordable Housing Opportunity": "bg-green-50 border-green-200 text-green-900",
  "Moderate Affordable Housing Opportunity": "bg-yellow-50 border-yellow-200 text-yellow-900",
  "Challenging Affordable Housing Market": "bg-orange-50 border-orange-200 text-orange-900",
};

type Toast = { id: number; type: "success" | "error"; message: string };

export default function ResultsDashboard({ result, request, onReset, onRerun }: Props) {
  const [exportingCsv, setExportingCsv] = useState(false);
  const [exportingPdf, setExportingPdf] = useState(false);
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [exportingBoardPack, setExportingBoardPack] = useState(false);
  const [boardPackPayload, setBoardPackPayload] = useState<BoardPackExportResponse | null>(null);
  const [rerunOpen, setRerunOpen] = useState(false);
  const [rerunGender, setRerunGender] = useState(request.gender);
  const [rerunGrade, setRerunGrade] = useState(request.grade_level);
  const [rerunMinutes, setRerunMinutes] = useState(request.drive_minutes);
  const [rerunProfile, setRerunProfile] = useState(request.weighting_profile);
  const [rerunContext, setRerunContext] = useState(request.market_context ?? "suburban");
  const [primaryView, setPrimaryView] = useState<"market_dashboard" | "assessment_detail">("market_dashboard");

  function addToast(type: "success" | "error", message: string) {
    const id = Date.now();
    setToasts((prev) => [...prev, { id, type, message }]);
    setTimeout(() => setToasts((prev) => prev.filter((t) => t.id !== id)), 4000);
  }

  async function handleExportCsv() {
    setExportingCsv(true);
    try {
      await exportCsv(request);
      addToast("success", "CSV report downloaded.");
    } catch {
      addToast("error", "CSV export failed. Please try again.");
    } finally {
      setExportingCsv(false);
    }
  }



  async function handleExportBoardPack() {
    setExportingBoardPack(true);
    try {
      const payload = await exportBoardPack(request);
      setBoardPackPayload(payload);
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `board_pack_${request.school_name.replace(/\s+/g, "_")}.json`;
      a.click();
      URL.revokeObjectURL(url);
      addToast("success", "Board-pack JSON downloaded.");
    } catch {
      addToast("error", "Board-pack export failed. Please try again.");
    } finally {
      setExportingBoardPack(false);
    }
  }

  async function handleExportPdf() {
    setExportingPdf(true);
    try {
      await exportPdf(request);
      addToast("success", "PDF report downloaded.");
    } catch {
      addToast("error", "PDF export failed. Please try again.");
    } finally {
      setExportingPdf(false);
    }
  }

  const recommendationStyle =
    recommendationColors[result.recommendation] ?? "bg-gray-50 border-gray-200 text-gray-900";
  const profileLabel = weightingProfileLabels[result.feasibility_score.weighting_profile] ?? "Custom";
  const w = result.feasibility_score;

  const dataAgeWarning = result.data_freshness?.sources?.some(
    (s) => s.freshness_hours != null && s.freshness_hours > 8760 // > 1 year
  );

  const oldestSource = result.data_freshness?.sources?.reduce<
    { source_label: string; freshness_hours: number | null } | null
  >((oldest, s) => {
    if (s.freshness_hours == null) return oldest;
    if (oldest == null || (oldest.freshness_hours != null && s.freshness_hours > oldest.freshness_hours))
      return { source_label: s.source_label, freshness_hours: s.freshness_hours ?? null };
    return oldest;
  }, null);

  return (
    <div className="space-y-6">
      {/* Toast notifications */}
      {toasts.length > 0 && (
        <div className="fixed top-4 right-4 z-50 flex flex-col gap-2">
          {toasts.map((t) => (
            <div
              key={t.id}
              className={`flex items-center gap-2 px-4 py-3 rounded-lg shadow-lg text-sm font-medium transition-all ${
                t.type === "success"
                  ? "bg-green-50 border border-green-200 text-green-900"
                  : "bg-red-50 border border-red-200 text-red-900"
              }`}
            >
              {t.type === "success" ? (
                <CheckCircle2 className="w-4 h-4 text-green-600 flex-shrink-0" />
              ) : (
                <XCircle className="w-4 h-4 text-red-600 flex-shrink-0" />
              )}
              {t.message}
            </div>
          ))}
        </div>
      )}

      {/* Decision-support disclaimer — always visible, all ministry types */}
      <div className="bg-amber-50 border border-amber-200 rounded-xl px-5 py-4 flex gap-3">
        <AlertCircle className="w-4 h-4 text-amber-600 flex-shrink-0 mt-0.5" />
        <div className="space-y-1">
          <p className="text-sm font-semibold text-amber-900">
            Stage 1 Market Screen — Decision-Support Tool Only
          </p>
          <p className="text-xs text-amber-800 leading-relaxed">
            This analysis is a directional signal based on Census demographics and competitor data.
            It is <strong>not</strong> a directive to open, close, or transform a ministry.
            Strategic commitments require Stage 2 institutional economics review and Stage 3 local
            community validation. Use alongside pastoral discernment and professional judgment.
          </p>
        </div>
      </div>

      {/* Low data confidence warning */}
      {result.demographics?.data_confidence === "low" && (
        <div className="bg-orange-50 border border-orange-300 rounded-xl px-5 py-4 flex gap-3">
          <AlertCircle className="w-4 h-4 text-orange-600 flex-shrink-0 mt-0.5" />
          <div className="space-y-1">
            <p className="text-sm font-semibold text-orange-900">
              Low Data Confidence — Small or Sparse Catchment
            </p>
            <p className="text-xs text-orange-800 leading-relaxed">
              The catchment area has a small or thin population base, which reduces the reliability
              of Census demographic estimates. Confidence interval is wide (±18 points). This score
              should be treated as a rough directional signal only. <strong>Local knowledge and
              direct community engagement are especially important before drawing conclusions.</strong>
            </p>
          </div>
        </div>
      )}


      {result.export_readiness && !result.export_readiness.ready && (
        <div className="bg-amber-50 border border-amber-300 rounded-xl px-5 py-4">
          <p className="text-sm font-semibold text-amber-900">Board-ready export gating active</p>
          <p className="text-xs text-amber-800 mt-1">
            This run is directional and not currently export-ready for board materials.
            {result.export_readiness.reasons?.length ? ` Reasons: ${result.export_readiness.reasons.join("; ")}` : ""}
          </p>
        </div>
      )}

      {/* Header bar */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
        <div>
          <h2 className="text-xl font-bold text-gray-900">{result.school_name}</h2>
          <p className="text-sm text-gray-400">
            {result.analysis_address} · {result.ministry_type.replace("_", " ")} · {profileLabel} ·{" "}
            {result.catchment_type === "isochrone" && result.catchment_minutes
              ? `${result.catchment_minutes}-min drive catchment`
              : `${result.radius_miles}-mile radius`}
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <button
            onClick={handleExportCsv}
            disabled={exportingCsv}
            className="flex items-center gap-1.5 text-sm px-4 py-2 border border-gray-200 rounded-lg text-gray-600 hover:bg-gray-50 transition-colors disabled:opacity-50"
          >
            <Download className="w-4 h-4" />
            {exportingCsv ? "Exporting..." : "Export CSV"}
          </button>
          <button
            onClick={handleExportPdf}
            disabled={exportingPdf}
            className="flex items-center gap-1.5 text-sm px-4 py-2 border border-gray-200 rounded-lg text-gray-600 hover:bg-gray-50 transition-colors disabled:opacity-50"
          >
            <FileText className="w-4 h-4" />
            {exportingPdf ? "Generating..." : "Export PDF"}
          </button>
          <button
            onClick={handleExportBoardPack}
            disabled={exportingBoardPack || !result.export_readiness?.ready}
            className="flex items-center gap-1.5 text-sm px-4 py-2 border border-gray-200 rounded-lg text-gray-600 hover:bg-gray-50 transition-colors disabled:opacity-50"
          >
            <Network className="w-4 h-4" />
            {exportingBoardPack ? "Exporting..." : result.export_readiness?.ready ? "Board Pack" : "Board Pack (Blocked)"}
          </button>
          <button
            onClick={onReset}
            className="flex items-center gap-1.5 text-sm px-4 py-2 bg-gray-900 text-white rounded-lg hover:bg-gray-800 transition-colors"
          >
            <RefreshCw className="w-4 h-4" />
            New Analysis
          </button>
        </div>
      </div>

      {/* Re-run panel */}
      {onRerun && result.ministry_type === "schools" && (
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm">
          <button
            onClick={() => setRerunOpen((o) => !o)}
            className="w-full flex items-center justify-between px-5 py-3 text-sm font-medium text-gray-700 hover:bg-gray-50 transition-colors rounded-xl"
          >
            <span className="flex items-center gap-2">
              <RefreshCw className="w-4 h-4 text-gray-400" />
              Re-run with updated settings
            </span>
            <span className="text-xs text-gray-400">{rerunOpen ? "▲ collapse" : "▼ expand"}</span>
          </button>
          {rerunOpen && (
            <div className="px-5 pb-5 border-t border-gray-100 pt-4">
              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3 mb-4">
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Gender</label>
                  <select value={rerunGender} onChange={(e) => setRerunGender(e.target.value as typeof rerunGender)}
                    className="w-full border border-gray-300 rounded-lg px-2.5 py-1.5 text-xs bg-white focus:outline-none focus:ring-1 focus:ring-gray-400">
                    <option value="coed">Co-ed</option>
                    <option value="boys">Boys</option>
                    <option value="girls">Girls</option>
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Grade Span</label>
                  <select value={rerunGrade} onChange={(e) => setRerunGrade(e.target.value as typeof rerunGrade)}
                    className="w-full border border-gray-300 rounded-lg px-2.5 py-1.5 text-xs bg-white focus:outline-none focus:ring-1 focus:ring-gray-400">
                    <option value="k5">K–5</option>
                    <option value="k8">K–8</option>
                    <option value="high_school">High School</option>
                    <option value="k12">K–12</option>
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Drive time (min)</label>
                  <input type="number" min={5} max={60} step={5} value={rerunMinutes}
                    onChange={(e) => setRerunMinutes(Number(e.target.value))}
                    className="w-full border border-gray-300 rounded-lg px-2.5 py-1.5 text-xs bg-white focus:outline-none focus:ring-1 focus:ring-gray-400" />
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Market Context</label>
                  <select value={rerunContext} onChange={(e) => setRerunContext(e.target.value as typeof rerunContext)}
                    className="w-full border border-gray-300 rounded-lg px-2.5 py-1.5 text-xs bg-white focus:outline-none focus:ring-1 focus:ring-gray-400">
                    <option value="suburban">Suburban</option>
                    <option value="urban">Urban</option>
                    <option value="rural">Rural</option>
                  </select>
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-600 mb-1">Weighting</label>
                  <select value={rerunProfile} onChange={(e) => setRerunProfile(e.target.value as typeof rerunProfile)}
                    className="w-full border border-gray-300 rounded-lg px-2.5 py-1.5 text-xs bg-white focus:outline-none focus:ring-1 focus:ring-gray-400">
                    <option value="standard_baseline">Standard</option>
                    <option value="affordability_sensitive">Affordability</option>
                    <option value="demand_primacy">Demand</option>
                  </select>
                </div>
              </div>
              <button
                onClick={() => {
                  onRerun({ ...request, gender: rerunGender, grade_level: rerunGrade, drive_minutes: rerunMinutes, weighting_profile: rerunProfile, market_context: rerunContext });
                  setRerunOpen(false);
                }}
                className="flex items-center gap-2 px-4 py-2 bg-gray-900 text-white text-sm font-medium rounded-lg hover:bg-gray-700 transition-colors"
              >
                <RefreshCw className="w-3.5 h-3.5" />
                Re-run Analysis
              </button>
            </div>
          )}
        </div>
      )}

      <div className="rounded-2xl border border-slate-200 bg-white p-2 shadow-sm">
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
          <button
            type="button"
            onClick={() => setPrimaryView("market_dashboard")}
            className={`rounded-xl px-4 py-3 text-left transition ${
              primaryView === "market_dashboard" ? "bg-indigo-600 text-white shadow-sm" : "bg-white text-slate-700 hover:bg-slate-50"
            }`}
          >
            <p className="text-sm font-semibold">Market dashboard</p>
            <p className={`mt-1 text-xs ${primaryView === "market_dashboard" ? "text-indigo-100" : "text-slate-500"}`}>
              Maps, trends, and ZIP-level drilldowns for the strongest market questions.
            </p>
          </button>
          <button
            type="button"
            onClick={() => setPrimaryView("assessment_detail")}
            className={`rounded-xl px-4 py-3 text-left transition ${
              primaryView === "assessment_detail" ? "bg-slate-900 text-white shadow-sm" : "bg-white text-slate-700 hover:bg-slate-50"
            }`}
          >
            <p className="text-sm font-semibold">Assessment detail</p>
            <p className={`mt-1 text-xs ${primaryView === "assessment_detail" ? "text-slate-200" : "text-slate-500"}`}>
              Overall scores, factor cards, legacy panels, exports, and supporting diagnostics.
            </p>
          </button>
        </div>
      </div>

      {primaryView === "market_dashboard" ? (
        <LiveModuleDashboard request={request} result={result} />
      ) : (
        <>
          {/* Overall score + recommendation */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6 flex flex-col items-center justify-center gap-3">
              <ScoreGauge
                score={result.feasibility_score.overall}
                label="Overall Feasibility"
                conservative={result.feasibility_score.scenario_conservative}
                optimistic={result.feasibility_score.scenario_optimistic}
              />
              {result.data_freshness && (
                <div className={`flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-full border ${
                  dataAgeWarning
                    ? "bg-orange-50 border-orange-200 text-orange-700"
                    : "bg-gray-50 border-gray-200 text-gray-500"
                }`}>
                  <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${dataAgeWarning ? "bg-orange-400" : "bg-green-400"}`} />
                  <span>
                    Data: {oldestSource
                      ? `${(oldestSource.freshness_hours! / 8760).toFixed(1)}yr old`
                      : result.data_freshness.mode}
                  </span>
                </div>
              )}
            </div>
            <div className={`md:col-span-2 rounded-xl border p-6 ${recommendationStyle}`}>
              <h3 className="text-lg font-bold mb-2">{result.recommendation}</h3>
              <p className="text-sm leading-relaxed">{result.recommendation_detail}</p>
            </div>
          </div>

          {/* Metric cards */}
          <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-5 gap-4">
            <MetricCard metric={result.feasibility_score.market_size} />
            <MetricCard metric={result.feasibility_score.income} />
            <MetricCard metric={result.feasibility_score.competition} />
            <MetricCard metric={result.feasibility_score.family_density} />
            {result.feasibility_score.occupancy ? <MetricCard metric={result.feasibility_score.occupancy} /> : null}
            {result.feasibility_score.workforce ? <MetricCard metric={result.feasibility_score.workforce} /> : null}
          </div>

          {/* Benchmark percentile rankings */}
          {w.benchmarks && (
            <BenchmarkPanel benchmarks={w.benchmarks} overallScore={w.overall} />
          )}

          {/* Hierarchical score breakdown */}
          {w.hierarchical && (
            <HierarchicalScorePanel hierarchical={w.hierarchical} ministryType={result.ministry_type} />
          )}

          {/* Map */}
          <SchoolMap
            lat={result.lat}
            lon={result.lon}
            radiusMiles={result.radius_miles}
            schools={result.competitor_schools}
            schoolName={result.school_name}
            isochronePolygon={result.isochrone_polygon}
            catchmentType={result.catchment_type}
            catchmentMinutes={result.catchment_minutes}
            ministryType={result.ministry_type}
          />

          {/* Demographics */}
          <DemographicsPanel
            demographics={result.demographics}
            countyName={result.county_name}
            ministryType={result.ministry_type}
            gender={result.gender}
            gradeLevel={result.grade_level}
          />

          {result.ministry_type === "schools" && result.population_gravity && (
            <PopulationGravityPanel
              gravity={result.population_gravity}
              schoolName={result.school_name}
            />
          )}

          {result.ministry_type === "elder_care" && result.population_gravity && (
            <ElderCareGravityPanel gravity={result.population_gravity} />
          )}

          {result.ministry_type === "housing" && result.population_gravity && (
            <HousingGravityPanel gravity={result.population_gravity} />
          )}

          {/* Demographic trend */}
          {result.ministry_type === "schools" && result.trend && result.trend.trend_label !== "Unknown" && (
            <TrendPanel trend={result.trend} />
          )}

          {/* Competitor schools */}
          <CompetitorTable
            schools={result.competitor_schools}
            catholicCount={result.catholic_school_count}
            totalPrivateCount={result.total_private_school_count}
            radiusMiles={result.radius_miles}
            ministryType={result.ministry_type}
            catchmentLabel={
              result.catchment_type === "isochrone" && result.catchment_minutes
                ? `${result.catchment_minutes}-min drive catchment`
                : undefined
            }
          />

          {/* What-If financial simulator */}
          {result.ministry_type === "schools" && <WhatIfSimulator result={result} />}

          {w.stage2 && <Stage2Dashboard stage2={w.stage2} ministryType={result.ministry_type} />}

          {(result.decision_pathway || result.benchmark_narrative || result.board_report_pack || result.data_freshness || result.trace_id || boardPackPayload) && (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              {result.decision_pathway && (
                <div className="rounded-xl border border-gray-200 bg-white p-4 space-y-2">
                  <p className="text-xs font-semibold text-gray-500 uppercase">Decision Pathway</p>
                  <p className="text-sm font-semibold text-gray-900">Recommendation: {result.decision_pathway.recommended_pathway}</p>
                  <p className="text-xs text-gray-600">Pathway confidence: {result.decision_pathway.confidence}</p>
                  {result.decision_pathway.top_risks.length > 0 && <p className="text-xs text-gray-700">Top risks: {result.decision_pathway.top_risks.join(" • ")}</p>}
                  {result.decision_pathway.required_validations.length > 0 && <p className="text-xs text-gray-700">Required validations: {result.decision_pathway.required_validations.join(" • ")}</p>}
                  {result.decision_pathway.next_12_month_actions.length > 0 && <p className="text-xs text-gray-700">Next 12-month actions: {result.decision_pathway.next_12_month_actions.join(" • ")}</p>}
                  {result.decision_pathway.partner_assessment && (
                    <details className="text-xs text-gray-700">
                      <summary className="cursor-pointer font-semibold">Partner assessment</summary>
                      <p className="mt-1">Mission alignment score: {result.decision_pathway.partner_assessment.mission_alignment_score}/100</p>
                      <p>Risk transfer: {result.decision_pathway.partner_assessment.risk_transfer_profile}</p>
                    </details>
                  )}
                </div>
              )}

              {result.benchmark_narrative && (
                <div className="rounded-xl border border-gray-200 bg-white p-4 space-y-2">
                  <p className="text-xs font-semibold text-gray-500 uppercase">Benchmark Narrative</p>
                  <p className="text-sm text-gray-800">{result.benchmark_narrative.narrative_summary}</p>
                </div>
              )}

              {result.board_report_pack && (
                <details className="rounded-xl border border-gray-200 bg-white p-4 space-y-2 lg:col-span-2">
                  <summary className="cursor-pointer text-sm font-semibold text-gray-900">Board-ready report pack</summary>
                  <p className="text-xs text-gray-700 mt-2">{result.board_report_pack.executive_summary}</p>
                  <p className="text-xs text-gray-700">Immediate actions: {result.board_report_pack.immediate_next_actions.join(" • ")}</p>
                </details>
              )}

              {result.data_freshness && (
                <div className="rounded-xl border border-gray-200 bg-white p-4 space-y-2">
                  <p className="text-xs font-semibold text-gray-500 uppercase">Data freshness</p>
                  <p className="text-xs text-gray-700">Mode: {result.data_freshness.mode} · Generated: {result.data_freshness.generated_at_utc}</p>
                  <ul className="text-xs text-gray-700 space-y-1">
                    {result.data_freshness.sources.map((src) => (
                      <li key={src.source_key}>
                        {src.source_label}: {src.status}{src.freshness_hours != null ? ` (${src.freshness_hours}h)` : ""}
                        {src.notes ? ` — ${src.notes}` : ""}
                      </li>
                    ))}
                  </ul>
                </div>
              )}

              {(result.trace_id || boardPackPayload?.trace_id) && (
                <div className="rounded-xl border border-gray-200 bg-white p-4">
                  <p className="text-xs font-semibold text-gray-500 uppercase">Trace ID</p>
                  <p className="text-xs text-gray-700 break-all">{result.trace_id ?? boardPackPayload?.trace_id}</p>
                </div>
              )}

              {boardPackPayload && (
                <details className="rounded-xl border border-gray-200 bg-white p-4 lg:col-span-2">
                  <summary className="cursor-pointer text-sm font-semibold text-gray-900">Latest board-pack JSON preview</summary>
                  <pre className="mt-2 text-[11px] text-gray-700 overflow-auto max-h-64 bg-gray-50 border border-gray-200 rounded p-2">
                    {JSON.stringify(boardPackPayload, null, 2)}
                  </pre>
                </details>
              )}
            </div>
          )}
        </>
      )}

      {primaryView === "assessment_detail" && (
        <>
          {/* Data notes */}
          {result.data_notes.length > 0 && (
            <div className="bg-blue-50 border border-blue-200 rounded-xl p-4">
              <div className="flex gap-2 mb-2">
                <AlertCircle className="w-4 h-4 text-blue-500 flex-shrink-0 mt-0.5" />
                <p className="text-xs font-semibold text-blue-800">Data Sources & Methodology Notes</p>
              </div>
              <ul className="space-y-1">
                {result.data_notes.map((note, i) => (
                  <li key={i} className="text-xs text-blue-700 ml-6">
                    {note}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Stage 1/2/3 interpretation framework */}
          <div className="bg-gray-50 border border-gray-200 rounded-xl p-5">
            <h3 className="text-sm font-semibold text-gray-800 mb-3">
              How to Use This Score
            </h3>
            <p className="text-xs text-gray-600 mb-3 leading-relaxed">
              This tool produces a <strong>Stage 1 sustainability screen</strong> — a directional signal, not a
              decision-grade forecast. A high score warrants deeper investigation; a low score is a caution
              flag, not an automatic closure recommendation. Strategic commitments should only follow Stage 3 local validation.
            </p>
            <p className="text-xs text-gray-500 mb-4 leading-relaxed border-t border-gray-200 pt-3">
              <strong className="text-gray-600">Scoring methodology</strong> · Four market factors: Market Size ({w.market_size.weight}%), Income Level ({w.income.weight}%),
              Competition ({w.competition.weight}%), Family Density ({w.family_density.weight}%). Competition weights demand-validation at 60% and
              saturation at 40%, reflecting NCEA 2024-2025 data showing 39.3% of Catholic schools have
              waiting lists — existing school presence is more often a demand signal than market saturation.
              Income score includes a bonus for states with established parental choice programs (NCEA 2024-2025:
              18% of students nationally; 50%+ in FL, OH, IN, OK, IA, AZ).
              Enrollment benchmarks reference NCEA 2024-2025 Exhibit 6: modal Catholic school enrolls 150–299 students
              (38.6% of schools); microschool threshold is &lt;150 students.
            </p>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
              <div className="bg-white border border-green-200 rounded-lg p-3">
                <div className="flex items-center gap-2 mb-1.5">
                  <span className="w-5 h-5 rounded-full bg-green-600 text-white text-xs font-bold flex items-center justify-center flex-shrink-0">1</span>
                  <span className="text-xs font-semibold text-green-800">Market Feasibility</span>
                </div>
                <p className="text-xs text-gray-600 leading-relaxed">
                  <strong className="text-gray-700">This tool.</strong> Census demographics, competitor
                  presence, income levels, and family density. Indicates whether local conditions can support continued operation.
                </p>
              </div>
              <div className="bg-white border border-yellow-200 rounded-lg p-3">
                <div className="flex items-center gap-2 mb-1.5">
                  <span className="w-5 h-5 rounded-full bg-yellow-500 text-white text-xs font-bold flex items-center justify-center flex-shrink-0">2</span>
                  <span className="text-xs font-semibold text-yellow-800">Institutional Economics</span>
                </div>
                <p className="text-xs text-gray-600 leading-relaxed">
                  <strong className="text-gray-700">Next step.</strong> Enrollment sustainability, tuition
                  discount rate, operating margin, subsidy dependency, and mission fit for ongoing school operations.
                </p>
              </div>
              <div className="bg-white border border-blue-200 rounded-lg p-3">
                <div className="flex items-center gap-2 mb-1.5">
                  <span className="w-5 h-5 rounded-full bg-blue-600 text-white text-xs font-bold flex items-center justify-center flex-shrink-0">3</span>
                  <span className="text-xs font-semibold text-blue-800">Local Validation</span>
                </div>
                <p className="text-xs text-gray-600 leading-relaxed">
                  <strong className="text-gray-700">Before committing.</strong> Sponsor and community
                  engagement, feeder-school outreach, parent demand surveys, diocesan alignment, and
                  local listening sessions.
                </p>
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
