"use client";

import dynamic from "next/dynamic";
import { ReactNode, useState } from "react";
import { Download, FileText, AlertCircle, RefreshCw, CheckCircle2, XCircle, Network } from "lucide-react";
import { AnalysisResponse, AnalysisRequest } from "@/lib/types";
import ScoreGauge from "./ScoreGauge";
import MetricCard from "./MetricCard";
import BenchmarkPanel from "./BenchmarkPanel";
import HierarchicalScorePanel from "./HierarchicalScorePanel";
import { exportBoardPack, exportCsv, exportPdf } from "@/lib/api";

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

function InfoFooterCard({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="rounded-[28px] border border-slate-200 bg-white p-5 shadow-sm">
      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">{label}</p>
      <div className="mt-2 text-sm text-slate-800">{children}</div>
    </div>
  );
}

export default function ResultsDashboard({ result, request, onReset, onRerun }: Props) {
  const [exportingCsv, setExportingCsv] = useState(false);
  const [exportingPdf, setExportingPdf] = useState(false);
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [exportingBoardPack, setExportingBoardPack] = useState(false);
  const [rerunOpen, setRerunOpen] = useState(false);
  const [rerunGender, setRerunGender] = useState(request.gender);
  const [rerunGrade, setRerunGrade] = useState(request.grade_level);
  const [rerunMinutes, setRerunMinutes] = useState(request.drive_minutes);
  const [rerunProfile, setRerunProfile] = useState(request.weighting_profile);
  const [rerunContext, setRerunContext] = useState(request.market_context ?? "suburban");

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
      await exportBoardPack(request);
      addToast("success", "Board pack downloaded.");
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
    recommendationColors[result.recommendation] ?? "bg-slate-50 border-slate-200 text-slate-900";
  const profileLabel = weightingProfileLabels[result.feasibility_score.weighting_profile] ?? "Custom";
  const w = result.feasibility_score;

  const dataAgeWarning = result.data_freshness?.sources?.some(
    (s) => s.freshness_hours != null && s.freshness_hours > 2160,
  );

  const oldestSource = result.data_freshness?.sources?.reduce<
    { source_label: string; freshness_hours: number | null } | null
  >((oldest, s) => {
    if (s.freshness_hours == null) return oldest;
    if (oldest == null || (oldest.freshness_hours != null && s.freshness_hours > oldest.freshness_hours)) {
      return { source_label: s.source_label, freshness_hours: s.freshness_hours ?? null };
    }
    return oldest;
  }, null);

  return (
    <div className="space-y-6">
      {toasts.length > 0 && (
        <div className="fixed right-4 top-4 z-50 flex flex-col gap-2">
          {toasts.map((t) => (
            <div
              key={t.id}
              className={`flex items-center gap-2 rounded-2xl border px-4 py-3 text-sm font-medium shadow-lg transition-all ${
                t.type === "success"
                  ? "border-green-200 bg-green-50 text-green-900"
                  : "border-red-200 bg-red-50 text-red-900"
              }`}
            >
              {t.type === "success" ? (
                <CheckCircle2 className="h-4 w-4 flex-shrink-0 text-green-600" />
              ) : (
                <XCircle className="h-4 w-4 flex-shrink-0 text-red-600" />
              )}
              {t.message}
            </div>
          ))}
        </div>
      )}

      <div className="rounded-2xl border border-amber-200 bg-amber-50 px-5 py-4 flex gap-3">
        <AlertCircle className="mt-0.5 h-4 w-4 flex-shrink-0 text-amber-600" />
        <div className="space-y-1">
          <p className="text-sm font-semibold text-amber-900">
            Stage 1 Market Screen — Decision-Support Tool Only
          </p>
          <p className="text-xs leading-relaxed text-amber-800">
            This analysis is a directional signal based on Census demographics and competitor data.
            It is <strong>not</strong> a directive to open, close, or transform a ministry.
            Strategic commitments require Stage 2 institutional economics review and Stage 3 local
            community validation. Use alongside pastoral discernment and professional judgment.
          </p>
        </div>
      </div>

      {result.demographics?.data_confidence === "low" && (
        <div className="rounded-2xl border border-orange-300 bg-orange-50 px-5 py-4 flex gap-3">
          <AlertCircle className="mt-0.5 h-4 w-4 flex-shrink-0 text-orange-600" />
          <div className="space-y-1">
            <p className="text-sm font-semibold text-orange-900">
              Low Data Confidence — Small or Sparse Catchment
            </p>
            <p className="text-xs leading-relaxed text-orange-800">
              The catchment area has a small or thin population base, which reduces the reliability
              of Census demographic estimates. Confidence interval is wide (±18 points). This score
              should be treated as a rough directional signal only. <strong>Local knowledge and
              direct community engagement are especially important before drawing conclusions.</strong>
            </p>
          </div>
        </div>
      )}

      {result.export_readiness && !result.export_readiness.ready && (
        <div className="rounded-2xl border border-amber-300 bg-amber-50 px-5 py-4">
          <p className="text-sm font-semibold text-amber-900">Board-ready export gating active</p>
          <p className="mt-1 text-xs text-amber-800">
            This run is directional and not currently export-ready for board materials.
            {result.export_readiness.reasons?.length ? ` Reasons: ${result.export_readiness.reasons.join("; ")}` : ""}
          </p>
        </div>
      )}

      <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div className="space-y-2">
            <div>
              <h2 className="text-2xl font-semibold tracking-tight text-slate-950">{result.school_name}</h2>
              <p className="mt-1 text-sm text-slate-500">
                {result.analysis_address} · {result.ministry_type.replace("_", " ")} · {profileLabel} · {" "}
                {result.catchment_type === "isochrone" && result.catchment_minutes
                  ? `${result.catchment_minutes}-min drive catchment`
                  : `${result.radius_miles}-mile radius`}
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <span className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400 shadow-sm">
                {result.county_name}
              </span>
              {result.data_freshness && (
                <span
                  className={`rounded-xl border px-3 py-2 text-xs font-semibold shadow-sm ${
                    dataAgeWarning
                      ? "border-orange-200 bg-orange-50 text-orange-700"
                      : "border-slate-200 bg-white text-slate-500"
                  }`}
                >
                  Data age:{" "}
                  {oldestSource
                    ? `${(oldestSource.freshness_hours! / 8760).toFixed(1)} yr oldest source`
                    : result.data_freshness.mode}
                </span>
              )}
            </div>
          </div>

          <div className="flex flex-wrap gap-2 lg:justify-end">
            <button
              onClick={handleExportCsv}
              disabled={exportingCsv}
              className="flex items-center gap-1.5 rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm text-slate-600 transition-colors hover:bg-slate-50 disabled:opacity-50"
            >
              <Download className="h-4 w-4" />
              {exportingCsv ? "Exporting..." : "Export CSV"}
            </button>
            <button
              onClick={handleExportPdf}
              disabled={exportingPdf}
              className="flex items-center gap-1.5 rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm text-slate-600 transition-colors hover:bg-slate-50 disabled:opacity-50"
            >
              <FileText className="h-4 w-4" />
              {exportingPdf ? "Generating..." : "Export PDF"}
            </button>
            <button
              onClick={handleExportBoardPack}
              disabled={exportingBoardPack || !result.export_readiness?.ready}
              title={result.export_readiness?.ready ? "Export board pack" : "Board pack requires an export-ready run"}
              className="flex items-center gap-1.5 rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm text-slate-600 transition-colors hover:bg-slate-50 disabled:cursor-not-allowed disabled:text-slate-400 disabled:hover:bg-white"
            >
              <Network className="h-4 w-4" />
              {exportingBoardPack ? "Exporting..." : "Board Pack"}
            </button>
            <button
              onClick={onReset}
              className="flex items-center gap-1.5 rounded-xl bg-slate-950 px-4 py-2 text-sm text-white transition-colors hover:bg-slate-800"
            >
              <RefreshCw className="h-4 w-4" />
              New Analysis
            </button>
          </div>
        </div>
      </div>

      {onRerun && result.ministry_type === "schools" && (
        <div className="rounded-[28px] border border-slate-200 bg-white shadow-sm">
          <button
            onClick={() => setRerunOpen((o) => !o)}
            className="flex w-full items-center justify-between rounded-[28px] px-5 py-4 text-sm font-medium text-slate-800 transition-colors hover:bg-slate-50"
          >
            <span className="flex items-center gap-2">
              <RefreshCw className="h-4 w-4 text-slate-400" />
              Re-run with updated settings
            </span>
            <span className="text-xs text-slate-400">{rerunOpen ? "▲ collapse" : "▼ expand"}</span>
          </button>
          {rerunOpen && (
            <div className="border-t border-slate-100 px-5 pb-5 pt-4">
              <div className="mb-4 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
                <div>
                  <label className="mb-1 block text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Gender</label>
                  <select
                    value={rerunGender}
                    onChange={(e) => setRerunGender(e.target.value as typeof rerunGender)}
                    className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-indigo-200"
                  >
                    <option value="coed">Co-ed</option>
                    <option value="boys">Boys</option>
                    <option value="girls">Girls</option>
                  </select>
                </div>
                <div>
                  <label className="mb-1 block text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Grade Span</label>
                  <select
                    value={rerunGrade}
                    onChange={(e) => setRerunGrade(e.target.value as typeof rerunGrade)}
                    className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-indigo-200"
                  >
                    <option value="k5">K–5</option>
                    <option value="k8">K–8</option>
                    <option value="high_school">High School</option>
                    <option value="k12">K–12</option>
                  </select>
                </div>
                <div>
                  <label className="mb-1 block text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Drive time</label>
                  <input
                    type="number"
                    min={5}
                    max={60}
                    step={5}
                    value={rerunMinutes}
                    onChange={(e) => setRerunMinutes(Number(e.target.value))}
                    className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-indigo-200"
                  />
                </div>
                <div>
                  <label className="mb-1 block text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Market Context</label>
                  <select
                    value={rerunContext}
                    onChange={(e) => setRerunContext(e.target.value as typeof rerunContext)}
                    className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-indigo-200"
                  >
                    <option value="suburban">Suburban</option>
                    <option value="urban">Urban</option>
                    <option value="rural">Rural</option>
                  </select>
                </div>
                <div>
                  <label className="mb-1 block text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Weighting</label>
                  <select
                    value={rerunProfile}
                    onChange={(e) => setRerunProfile(e.target.value as typeof rerunProfile)}
                    className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs text-slate-800 focus:outline-none focus:ring-2 focus:ring-indigo-200"
                  >
                    <option value="standard_baseline">Standard</option>
                    <option value="affordability_sensitive">Affordability</option>
                    <option value="demand_primacy">Demand</option>
                  </select>
                </div>
              </div>
              <button
                onClick={() => {
                  onRerun({
                    ...request,
                    gender: rerunGender,
                    grade_level: rerunGrade,
                    drive_minutes: rerunMinutes,
                    weighting_profile: rerunProfile,
                    market_context: rerunContext,
                  });
                  setRerunOpen(false);
                }}
                className="flex items-center gap-2 rounded-xl bg-slate-950 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-slate-800"
              >
                <RefreshCw className="h-3.5 w-3.5" />
                Re-run Analysis
              </button>
            </div>
          )}
        </div>
      )}

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-[280px_minmax(0,1fr)]">
        <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
          <div className="flex flex-col items-center justify-center gap-4 text-center">
            <ScoreGauge
              score={result.feasibility_score.overall}
              label="Overall Feasibility"
              conservative={result.feasibility_score.scenario_conservative}
              optimistic={result.feasibility_score.scenario_optimistic}
            />
            {result.data_freshness && (
              <div
                className={`flex items-center gap-1.5 rounded-full border px-3 py-1.5 text-xs ${
                  dataAgeWarning
                    ? "border-orange-200 bg-orange-50 text-orange-700"
                    : "border-slate-200 bg-slate-50 text-slate-500"
                }`}
              >
                <span
                  className={`h-1.5 w-1.5 flex-shrink-0 rounded-full ${
                    dataAgeWarning ? "bg-orange-400" : "bg-green-400"
                  }`}
                />
                <span>
                  Data:{" "}
                  {oldestSource
                    ? `${(oldestSource.freshness_hours! / 8760).toFixed(1)}yr old`
                    : result.data_freshness.mode}
                </span>
              </div>
            )}
          </div>
        </div>

        <div className={`rounded-[28px] border p-6 shadow-sm ${recommendationStyle}`}>
          <p className="text-xs font-semibold uppercase tracking-[0.18em] opacity-70">Recommendation</p>
          <h3 className="mt-2 text-2xl font-semibold tracking-tight">{result.recommendation}</h3>
          <p className="mt-3 text-sm leading-relaxed">{result.recommendation_detail}</p>
        </div>
      </div>

      <LiveModuleDashboard request={request} result={result} />

      <section className="space-y-6">
        <div>
          <h3 className="text-xl font-semibold tracking-tight text-slate-950">Supporting Detail</h3>
          <p className="mt-1 text-sm text-slate-500">
            Supporting diagnostics and methodology behind the unified dashboard view.
          </p>
        </div>

        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-5">
          <MetricCard metric={result.feasibility_score.market_size} />
          <MetricCard metric={result.feasibility_score.income} />
          <MetricCard metric={result.feasibility_score.competition} />
          <MetricCard metric={result.feasibility_score.family_density} />
          {result.feasibility_score.occupancy ? <MetricCard metric={result.feasibility_score.occupancy} /> : null}
          {result.feasibility_score.workforce ? <MetricCard metric={result.feasibility_score.workforce} /> : null}
        </div>

        {w.benchmarks && <BenchmarkPanel benchmarks={w.benchmarks} overallScore={w.overall} />}

        {w.hierarchical && (
          <HierarchicalScorePanel hierarchical={w.hierarchical} ministryType={result.ministry_type} />
        )}

        {w.stage2 && <Stage2Dashboard stage2={w.stage2} ministryType={result.ministry_type} />}

        {result.decision_pathway && (
          <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Decision Pathway</p>
            <div className="mt-4 space-y-3">
              <div>
                <p className="text-sm text-slate-500">Recommended pathway</p>
                <p className="text-lg font-semibold tracking-tight text-slate-950">
                  {result.decision_pathway.recommended_pathway}
                </p>
              </div>
              <p className="text-sm text-slate-800">
                Pathway confidence: <span className="font-semibold">{result.decision_pathway.confidence}</span>
              </p>
              {result.decision_pathway.top_risks.length > 0 && (
                <div>
                  <p className="text-sm font-semibold text-slate-950">Top risks</p>
                  <p className="mt-1 text-sm text-slate-500">{result.decision_pathway.top_risks.join(" • ")}</p>
                </div>
              )}
              {result.decision_pathway.required_validations.length > 0 && (
                <div>
                  <p className="text-sm font-semibold text-slate-950">Required validations</p>
                  <p className="mt-1 text-sm text-slate-500">
                    {result.decision_pathway.required_validations.join(" • ")}
                  </p>
                </div>
              )}
              {result.decision_pathway.next_12_month_actions.length > 0 && (
                <div>
                  <p className="text-sm font-semibold text-slate-950">Next 12-month actions</p>
                  <p className="mt-1 text-sm text-slate-500">
                    {result.decision_pathway.next_12_month_actions.join(" • ")}
                  </p>
                </div>
              )}
              {result.decision_pathway.partner_assessment && (
                <details className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <summary className="cursor-pointer text-sm font-semibold text-slate-950">
                    Partner assessment
                  </summary>
                  <div className="mt-3 space-y-2 text-sm text-slate-500">
                    <p>
                      Mission alignment score:{" "}
                      <span className="font-semibold text-slate-800">
                        {result.decision_pathway.partner_assessment.mission_alignment_score}/100
                      </span>
                    </p>
                    <p>
                      Risk transfer:{" "}
                      <span className="font-semibold text-slate-800">
                        {result.decision_pathway.partner_assessment.risk_transfer_profile}
                      </span>
                    </p>
                  </div>
                </details>
              )}
            </div>
          </div>
        )}

        {(result.data_freshness || result.trace_id) && (
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            {result.data_freshness && (
              <InfoFooterCard label="Data Freshness">
                <p className="text-sm text-slate-500">
                  Mode: {result.data_freshness.mode} · Generated: {result.data_freshness.generated_at_utc}
                </p>
                <ul className="mt-3 space-y-1 text-sm text-slate-500">
                  {result.data_freshness.sources.map((src) => (
                    <li key={src.source_key}>
                      <span className="font-medium text-slate-800">{src.source_label}</span>: {src.status}
                      {src.freshness_hours != null ? ` (${src.freshness_hours}h)` : ""}
                      {src.notes ? ` — ${src.notes}` : ""}
                    </li>
                  ))}
                </ul>
              </InfoFooterCard>
            )}
            {result.trace_id && (
              <InfoFooterCard label="Trace ID">
                <p className="break-all text-sm text-slate-500">{result.trace_id}</p>
              </InfoFooterCard>
            )}
          </div>
        )}

        <details className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
          <summary className="cursor-pointer list-none text-lg font-semibold tracking-tight text-slate-950">
            Methodology and score usage
          </summary>
          <div className="mt-4 space-y-4 border-t border-slate-100 pt-4">
            {result.data_notes.length > 0 && (
              <div className="rounded-2xl border border-blue-200 bg-blue-50 p-4">
                <div className="mb-2 flex gap-2">
                  <AlertCircle className="mt-0.5 h-4 w-4 flex-shrink-0 text-blue-500" />
                  <p className="text-xs font-semibold uppercase tracking-[0.18em] text-blue-800">
                    Data Sources & Methodology Notes
                  </p>
                </div>
                <ul className="space-y-1">
                  {result.data_notes.map((note, i) => (
                    <li key={i} className="ml-6 text-xs text-blue-700">
                      {note}
                    </li>
                  ))}
                </ul>
              </div>
            )}

            <div>
              <h4 className="text-base font-semibold text-slate-950">How to Use This Score</h4>
              <p className="mt-2 text-sm leading-relaxed text-slate-500">
                This tool produces a <strong className="text-slate-800">Stage 1 sustainability screen</strong> — a
                directional signal, not a decision-grade forecast. A high score warrants deeper investigation;
                a low score is a caution flag, not an automatic closure recommendation. Strategic commitments
                should only follow Stage 3 local validation.
              </p>
              <p className="mt-3 border-t border-slate-100 pt-3 text-sm leading-relaxed text-slate-500">
                <strong className="text-slate-800">Scoring methodology</strong> · Four market factors: Market
                Size ({w.market_size.weight}%), Income Level ({w.income.weight}%), Competition ({w.competition.weight}%),
                Family Density ({w.family_density.weight}%). Competition weights demand-validation at 60% and
                saturation at 40%, reflecting NCEA 2024-2025 data showing 39.3% of Catholic schools have waiting
                lists — existing school presence is more often a demand signal than market saturation. Income
                score includes a bonus for states with established parental choice programs (NCEA 2024-2025:
                18% of students nationally; 50%+ in FL, OH, IN, OK, IA, AZ). Enrollment benchmarks reference
                NCEA 2024-2025 Exhibit 6: modal Catholic school enrolls 150–299 students (38.6% of schools);
                microschool threshold is &lt;150 students.
              </p>
            </div>

            <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
              <div className="rounded-[24px] border border-green-200 bg-white p-4">
                <div className="mb-2 flex items-center gap-2">
                  <span className="flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-green-600 text-xs font-bold text-white">1</span>
                  <span className="text-sm font-semibold text-green-800">Market Feasibility</span>
                </div>
                <p className="text-sm leading-relaxed text-slate-500">
                  <strong className="text-slate-800">This tool.</strong> Census demographics, competitor presence,
                  income levels, and family density. Indicates whether local conditions can support continued operation.
                </p>
              </div>
              <div className="rounded-[24px] border border-yellow-200 bg-white p-4">
                <div className="mb-2 flex items-center gap-2">
                  <span className="flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-yellow-500 text-xs font-bold text-white">2</span>
                  <span className="text-sm font-semibold text-yellow-800">Institutional Economics</span>
                </div>
                <p className="text-sm leading-relaxed text-slate-500">
                  <strong className="text-slate-800">Next step.</strong> Enrollment sustainability, tuition discount
                  rate, operating margin, subsidy dependency, and mission fit for ongoing operations.
                </p>
              </div>
              <div className="rounded-[24px] border border-blue-200 bg-white p-4">
                <div className="mb-2 flex items-center gap-2">
                  <span className="flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full bg-blue-600 text-xs font-bold text-white">3</span>
                  <span className="text-sm font-semibold text-blue-800">Local Validation</span>
                </div>
                <p className="text-sm leading-relaxed text-slate-500">
                  <strong className="text-slate-800">Before committing.</strong> Sponsor and community engagement,
                  feeder-school outreach, parent demand surveys, diocesan alignment, and local listening sessions.
                </p>
              </div>
            </div>
          </div>
        </details>
      </section>
    </div>
  );
}
