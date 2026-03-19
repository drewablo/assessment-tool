import type { AnalysisRequest, AnalysisResponse } from "@/lib/types";

const SESSION_KEY = "dashboard_analysis_context";

export interface DashboardSessionContext {
  request: AnalysisRequest;
  result: AnalysisResponse;
}

export function openDashboard(request: AnalysisRequest, result: AnalysisResponse) {
  sessionStorage.setItem(SESSION_KEY, JSON.stringify({ request, result }));
  window.open("/dashboard", "_blank");
}

export function loadDashboardContext(): DashboardSessionContext | null {
  try {
    const raw = sessionStorage.getItem(SESSION_KEY);
    if (raw) {
      const parsed = JSON.parse(raw) as DashboardSessionContext;
      if (parsed.request && parsed.result) {
        return parsed;
      }
    }
  } catch {
    // ignore
  }
  return null;
}
