"use client";

import { useEffect, useRef, useState } from "react";
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

function buildDefaultRequest(): AnalysisRequest {
  return {
    school_name: "",
    address: "",
    ministry_type: "schools",
    mission_mode: false,
    drive_minutes: 20,
    geography_mode: "catchment",
    gender: "coed",
    grade_level: "k12",
    weighting_profile: "standard_baseline",
    market_context: "suburban",
    care_level: "all",
  };
}

export default function HomePage() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<AnalysisResponse | null>(null);
  const [lastRequest, setLastRequest] = useState<AnalysisRequest | null>(null);
  const [savedAnalyses, setSavedAnalyses] = useState<SavedAnalysis[]>([]);
  const [draftRequest, setDraftRequest] = useState<AnalysisRequest | null>(null);
  const [saveStatus, setSaveStatus] = useState<string | null>(null);
  const lastRequestRef = useRef<AnalysisRequest | null>(null);
  const lastResultRef = useRef<AnalysisResponse | null>(null);

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
          ...(prev ?? buildDefaultRequest()),
          ...prefill,
        } as AnalysisRequest));
        window.localStorage.removeItem(PREFILL_ANALYSIS_KEY);
      }
    } catch {
      // Ignore invalid local cache
    }
  }, []);

  useEffect(() => {
    lastRequestRef.current = lastRequest;
  }, [lastRequest]);

  useEffect(() => {
    lastResultRef.current = result;
  }, [result]);

  useEffect(() => {
    const currentState = window.history.state;
    if (!currentState || currentState.view !== "results") {
      window.history.replaceState({ view: "form" }, "", "/");
    }

    function handlePopState(event: PopStateEvent) {
      const state = event.state as { view?: string; request?: AnalysisRequest; result?: AnalysisResponse } | null;

      if (state?.view === "results") {
        const nextRequest = state.request ?? lastRequestRef.current;
        const nextResult = state.result ?? lastResultRef.current;
        if (nextRequest) {
          setLastRequest(nextRequest);
          setDraftRequest({ ...nextRequest });
        }
        setError(null);
        setResult(nextResult ?? null);
        return;
      }

      setResult(null);
      setError(null);
      const fallbackRequest = state?.request ?? lastRequestRef.current;
      if (fallbackRequest) {
        setDraftRequest({ ...fallbackRequest });
      }
    }

    window.addEventListener("popstate", handlePopState);
    return () => window.removeEventListener("popstate", handlePopState);
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
      setDraftRequest({ ...req });
      window.history.pushState({ view: "results", request: req, result: data }, "", "/?view=results");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Analysis failed. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  function handleReset() {
    const requestToRestore = lastRequestRef.current;

    setResult(null);
    setError(null);
    if (requestToRestore) {
      setDraftRequest({ ...requestToRestore });
    }

    if (window.history.state?.view === "results") {
      window.history.back();
      return;
    }

    window.history.replaceState({ view: "form", request: requestToRestore ?? undefined }, "", "/");
  }

  return (
    <div className="min-h-screen bg-[#f7f7fc]">
      <header className="sticky top-0 z-40 border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-6xl items-center justify-between gap-3 px-4 py-3 sm:px-6">
          <div className="flex items-center gap-3">
            <div className="flex h-8 w-8 items-center justify-center rounded-full" style={{ background: "#172d57" }}>
              <span className="text-xs font-bold text-white">M</span>
            </div>
            <div>
              <h1 className="text-sm font-bold text-slate-950">Ministry Assessment Tool</h1>
              <p className="text-xs text-slate-400">Schools · Housing · Elder Care</p>
            </div>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-6xl px-4 py-8 sm:px-6">
        {loading ? (
          <LoadingSkeleton />
        ) : result ? (
          <ResultsDashboard result={result} request={lastRequest!} onReset={handleReset} onRerun={handleSubmit} />
        ) : (
          <div className="mx-auto max-w-2xl">
            <div className="mb-10 text-center">
              <h2 className="mb-3 text-3xl font-semibold tracking-tight text-slate-950">Ministry Assessment Tool</h2>
              <p className="text-base leading-relaxed text-slate-500">
                Select a ministry and analyze local demand, income fit, and competitive
                landscape using Census demographics plus ministry-specific competitor data.
              </p>
            </div>

            <div className="rounded-[28px] border border-slate-200 bg-white p-8 shadow-sm">
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
                <p className="mb-3 rounded-2xl border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700">
                  Database-backed history is disabled in this environment (USE_DB=false). Local saved projects above remain available in this browser.
                </p>
              )}
              <HistoryPanel
                onRestore={(req) => {
                  setDraftRequest((prev) => ({
                    ...(prev ?? buildDefaultRequest()),
                    ...req,
                  } as AnalysisRequest));
                }}
              />
            </div>

            {error && (
              <div className="mt-4 rounded-[28px] border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">
                <strong>Error:</strong> {error}
              </div>
            )}

            <div className="mt-10 grid grid-cols-1 gap-6 text-center text-sm text-slate-500 sm:grid-cols-3">
              <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
                <div className="mb-2 text-2xl">📍</div>
                <p className="mb-1 font-semibold text-slate-800">Geocode</p>
                <p>Address is geocoded to county-level with the Census Bureau API</p>
              </div>
              <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
                <div className="mb-2 text-2xl">📊</div>
                <p className="mb-1 font-semibold text-slate-800">Assess</p>
                <p>Population, income, and family data from ACS 5-year estimates</p>
              </div>
              <div className="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
                <div className="mb-2 text-2xl">🏫</div>
                <p className="mb-1 font-semibold text-slate-800">Score</p>
                <p>Module-aware scoring calibrated for schools, housing, or elder care</p>
              </div>
            </div>
          </div>
        )}
      </main>

      <footer className="mt-16 border-t border-slate-100 py-6 text-center text-xs text-slate-400">
        Data: US Census ACS 5-year estimates (2022) · NCES Private School Survey (2021–22) ·
        Catholic population: CARA state-level estimates
      </footer>
    </div>
  );
}
