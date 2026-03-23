import type { AnalysisRequest, AnalysisResponse } from "@/lib/types";

const SESSION_KEY = "dashboard_analysis_context";

export interface DashboardSessionContext {
  request: AnalysisRequest;
  result: AnalysisResponse;
}

export function openDashboard(request: AnalysisRequest, result: AnalysisResponse) {
  const payload = JSON.stringify({ request, result });
  sessionStorage.setItem(SESSION_KEY, payload);
  // Also store in localStorage as fallback — sessionStorage may not propagate
  // to the new window in all browsers (e.g., incognito, some Safari versions).
  try {
    localStorage.setItem(SESSION_KEY, payload);
  } catch {
    // localStorage unavailable (e.g., storage full) — sessionStorage is primary
  }
  window.open("/dashboard", "_blank");
}

export function loadDashboardContext(): DashboardSessionContext | null {
  // Try sessionStorage first, then localStorage as fallback for cross-window scenarios.
  for (const store of [sessionStorage, localStorage]) {
    try {
      const raw = store.getItem(SESSION_KEY);
      if (raw) {
        const parsed = JSON.parse(raw) as DashboardSessionContext;
        if (parsed.request && parsed.result) {
          return parsed;
        }
      }
    } catch {
      // storage unavailable or corrupt — try next
    }
  }
  return null;
}
