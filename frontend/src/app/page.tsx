"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import AnalysisForm from "@/components/AnalysisForm";
import ResultsDashboard from "@/components/ResultsDashboard";
import LoadingSkeleton from "@/components/LoadingSkeleton";
import HistoryPanel from "@/components/HistoryPanel";
import { runAnalysis } from "@/lib/api";
import { AnalysisRequest, AnalysisResponse } from "@/lib/types";

type SavedAnalysis = {
  id: string;
  label: string;
  request: AnalysisRequest;
  savedAt: string;
};

const STORAGE_KEY = "ministry_saved_analyses";
const DB_ENABLED = (process.env.NEXT_PUBLIC_USE_DB || "").toLowerCase() === "true";
const PREFILL_ANALYSIS_KEY = "intelligence_prefill_analysis";

export default function HomePage() {
  const [lane, setLane] = useState<"quick_screen" | "deep_review">("quick_screen");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<AnalysisResponse | null>(null);
  const [lastRequest, setLastRequest] = useState<AnalysisRequest | null>(null);
  const [savedAnalyses, setSavedAnalyses] = useState<SavedAnalysis[]>([]);
  const [draftRequest, setDraftRequest] = useState<AnalysisRequest | null>(null);
  const [saveStatus, setSaveStatus] = useState<string | null>(null);

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const parsed = JSON.parse(raw) as SavedAnalysis[];
        if (Array.isArray(parsed)) {
          setSavedAnalyses(parsed);
        }
      }

      const prefillRaw = window.localStorage.getItem(PREFILL_ANALYSIS_KEY);
      if (prefillRaw) {
        const prefill = JSON.parse(prefillRaw) as Partial<AnalysisRequest>;
        setDraftRequest((prev) => ({
          ...(prev ?? {
            school_name: "",
            address: "",
            ministry_type: "schools",
            mission_mode: false,
            drive_minutes: 20,
            gender: "coed",
            grade_level: "k12",
            weighting_profile: "standard_baseline",
            market_context: "suburban",
            care_level: "all",
          }),
          ...prefill,
        } as AnalysisRequest));
        window.localStorage.removeItem(PREFILL_ANALYSIS_KEY);
      }
    } catch {
      // Ignore invalid local cache
    }
  }, []);

  function persistSaved(next: SavedAnalysis[]) {
    setSavedAnalyses(next);
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
  }

  function handleSaveCurrent(req: AnalysisRequest): { ok: boolean; message: string } {
    const normalized = req.school_name.trim().toLowerCase();
    if (!normalized) {
      const message = "Project name is required before saving.";
      setSaveStatus(message);
      return { ok: false, message };
    }

    const duplicate = savedAnalyses.some(
      (entry) => entry.request.school_name.trim().toLowerCase() === normalized,
    );
    if (duplicate) {
      const message = `A saved project named \"${req.school_name.trim()}\" already exists.`;
      setSaveStatus(message);
      return { ok: false, message };
    }

    const timestamp = new Date().toISOString();
    const id = `${req.school_name}-${timestamp}`;
    const label = `${req.school_name} · ${req.ministry_type.replace("_", " ")} · ${req.address}`;
    const next = [{ id, label, request: req, savedAt: timestamp }, ...savedAnalyses].slice(0, 20);
    persistSaved(next);
    const message = `Saved ${req.school_name.trim()}.`;
    setSaveStatus(message);
    return { ok: true, message };
  }

  function handleDeleteSaved(id: string) {
    const next = savedAnalyses.filter((entry) => entry.id !== id);
    persistSaved(next);
    setSaveStatus("Saved project deleted.");
  }

  async function handleSubmit(req: AnalysisRequest) {
    setLoading(true);
    setError(null);
    setResult(null);
    setLastRequest(req);
    try {
      const data = await runAnalysis(req);
      setResult(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Analysis failed. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  function handleReset() {
    setResult(null);
    setError(null);
    setLastRequest(null);
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white border-b border-gray-200 sticky top-0 z-40">
        <div className="max-w-6xl mx-auto px-4 sm:px-6 py-3 flex items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-full flex items-center justify-center" style={{ background: "#172d57" }}>
              <span className="text-white text-xs font-bold">M</span>
            </div>
            <div>
              <h1 className="text-sm font-bold text-gray-900">Ministry Assessment Tool</h1>
              <p className="text-xs text-gray-400">Schools · Housing · Elder Care</p>
            </div>
          </div>
          <Link href="/intelligence" className="text-xs sm:text-sm px-3 py-2 rounded-lg border border-gray-200 text-gray-700 hover:bg-gray-50">
            Intelligence Console
          </Link>
        </div>
      </header>

      <main className="max-w-6xl mx-auto px-4 sm:px-6 py-8">
        {loading ? (
          <LoadingSkeleton />
        ) : result ? (
          <ResultsDashboard result={result} request={lastRequest!} onReset={handleReset} onRerun={handleSubmit} />
        ) : (
          <div className="max-w-2xl mx-auto">
            <div className="text-center mb-10">
              <h2 className="text-3xl font-bold text-gray-900 mb-3">Ministry Assessment Tool</h2>
              <p className="text-gray-500 text-base leading-relaxed">
                Select a ministry and analyze local demand, income fit, and competitive
                landscape using Census demographics plus ministry-specific competitor data.
              </p>
            </div>

            <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-8">
              <div className="mb-4 inline-flex rounded-lg border border-gray-200 overflow-hidden text-sm">
                <button
                  type="button"
                  onClick={() => setLane("quick_screen")}
                  className={`px-3 py-2 ${lane === "quick_screen" ? "bg-navy-700 text-white" : "bg-white text-gray-700"}`}
                >
                  Quick Screen
                </button>
                <button
                  type="button"
                  onClick={() => setLane("deep_review")}
                  className={`px-3 py-2 ${lane === "deep_review" ? "bg-navy-700 text-white" : "bg-white text-gray-700"}`}
                >
                  Deep Review
                </button>
              </div>
              <p className="text-xs text-gray-500 mb-4">
                {lane === "quick_screen"
                  ? "Quick Screen prioritizes speed and directional repeatability."
                  : "Deep Review supports expanded controls and board-ready export workflows."}
              </p>
              <AnalysisForm
                onSubmit={handleSubmit}
                loading={loading}
                initialRequest={draftRequest}
                savedOptions={savedAnalyses.map((s) => ({ id: s.id, label: s.label, request: s.request }))}
                saveStatus={saveStatus}
                onSaveCurrent={handleSaveCurrent}
                onDeleteSaved={handleDeleteSaved}
                onLoadSaved={(req) => setDraftRequest({ ...req })}
              />
            </div>



            <div className="mt-8">
              {!DB_ENABLED && (
                <p className="text-xs text-amber-700 bg-amber-50 border border-amber-200 rounded-lg px-3 py-2 mb-3">
                  Database-backed history is disabled in this environment (USE_DB=false). Local saved projects above remain available in this browser.
                </p>
              )}
              <HistoryPanel
                onRestore={(req) => {
                  setDraftRequest((prev) => ({
                    ...(prev ?? {
                      school_name: "",
                      address: "",
                      ministry_type: "schools",
                      mission_mode: false,
                      drive_minutes: 20,
                      gender: "coed",
                      grade_level: "k12",
                      weighting_profile: "standard_baseline",
                      market_context: "suburban",
                      care_level: "all",
                    }),
                    ...req,
                  } as AnalysisRequest));
                }}
              />
            </div>

            {error && (
              <div className="mt-4 bg-red-50 border border-red-200 rounded-xl px-5 py-4 text-sm text-red-700">
                <strong>Error:</strong> {error}
              </div>
            )}

            <div className="mt-10 grid grid-cols-3 gap-6 text-center text-sm text-gray-500">
              <div><div className="text-2xl mb-2">📍</div><p className="font-semibold text-gray-700 mb-1">Geocode</p><p>Address is geocoded to county-level with the Census Bureau API</p></div>
              <div><div className="text-2xl mb-2">📊</div><p className="font-semibold text-gray-700 mb-1">Assess</p><p>Population, income, and family data from ACS 5-year estimates</p></div>
              <div><div className="text-2xl mb-2">🏫</div><p className="font-semibold text-gray-700 mb-1">Score</p><p>Module-aware scoring calibrated for schools, housing, or elder care</p></div>
            </div>
          </div>
        )}
      </main>

      <footer className="border-t border-gray-100 mt-16 py-6 text-center text-xs text-gray-400">
        Data: US Census ACS 5-year estimates (2022) · NCES Private School Survey (2021–22) ·
        Catholic population: CARA state-level estimates
      </footer>
    </div>
  );
}
