import { AnalysisRequest, AnalysisResponse, AnalysisHistoryRecord, BoardReportPack, BenchmarkNarrative, DataFreshnessMetadata, OpportunityRecord, PipelineStatusResponse, PortfolioWorkspaceResponse, ScoringWeightsResponse, SchoolAuditExtractionResponse } from "./types";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
const API_KEY = process.env.NEXT_PUBLIC_API_KEY || "";

function apiHeaders(extra: Record<string, string> = {}): Record<string, string> {
  const headers: Record<string, string> = { ...extra };
  if (API_KEY) headers["X-API-Key"] = API_KEY;
  return headers;
}

async function parseApiError(res: Response, fallback: string): Promise<Error> {
  const error = await res.json().catch(() => ({ detail: fallback }));
  const detail = error?.detail;
  const message = typeof detail === "object" ? detail?.message : detail;
  return new Error(message || fallback);
}

export interface BoardPackExportResponse {
  trace_id?: string | null;
  school_name: string;
  analysis_address: string;
  board_report_pack?: BoardReportPack | null;
  benchmark_narrative?: BenchmarkNarrative | null;
  data_freshness?: DataFreshnessMetadata | null;
}

export async function runAnalysis(request: AnalysisRequest): Promise<AnalysisResponse> {
  const res = await fetch(`${API_URL}/api/analyze`, {
    method: "POST",
    headers: apiHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify(request),
  });

  if (!res.ok) {
    throw await parseApiError(res, `API error: ${res.status}`);
  }

  return res.json();
}

export async function exportCsv(request: AnalysisRequest): Promise<void> {
  const res = await fetch(`${API_URL}/api/export/csv`, {
    method: "POST",
    headers: apiHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify(request),
  });

  if (!res.ok) {
    throw await parseApiError(res, "CSV export failed");
  }

  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `feasibility_${request.school_name.replace(/\s+/g, "_")}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

export async function exportPdf(request: AnalysisRequest): Promise<void> {
  const res = await fetch(`${API_URL}/api/export/pdf`, {
    method: "POST",
    headers: apiHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify(request),
  });

  if (!res.ok) {
    throw await parseApiError(res, "PDF export failed");
  }

  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `feasibility_${request.school_name.replace(/\s+/g, "_")}.pdf`;
  a.click();
  URL.revokeObjectURL(url);
}

export async function fetchHistory(
  ministryType?: string,
  skip = 0,
  limit = 20,
): Promise<AnalysisHistoryRecord[]> {
  const params = new URLSearchParams({ skip: String(skip), limit: String(limit) });
  if (ministryType) params.set("ministry_type", ministryType);
  const res = await fetch(`${API_URL}/api/history?${params}`, { headers: apiHeaders() });
  if (!res.ok) {
    if (res.status === 501) {
      throw new Error("Database history is disabled (USE_DB=false).");
    }
    return [];
  }
  return res.json();
}

export async function deleteHistoryRecord(id: number): Promise<void> {
  const res = await fetch(`${API_URL}/api/history/${id}`, { method: "DELETE", headers: apiHeaders() });
  if (!res.ok && res.status !== 204) {
    throw new Error("Failed to delete history record");
  }
}

export async function extractSchoolAuditFinancials(files: File[]): Promise<SchoolAuditExtractionResponse> {
  const formData = new FormData();
  files.forEach((file) => formData.append("files", file));

  const res = await fetch(`${API_URL}/api/schools/stage2/extract-audits`, {
    method: "POST",
    headers: apiHeaders(),
    body: formData,
  });

  if (!res.ok) {
    throw await parseApiError(res, "Audit extraction failed");
  }

  return res.json();
}


export async function exportBoardPack(request: AnalysisRequest): Promise<BoardPackExportResponse> {
  const res = await fetch(`${API_URL}/api/export/board-pack`, {
    method: "POST",
    headers: apiHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify(request),
  });

  if (!res.ok) {
    throw await parseApiError(res, "Board-pack export failed");
  }

  return res.json();
}


export async function fetchPipelineStatus(): Promise<PipelineStatusResponse> {
  const res = await fetch(`${API_URL}/api/pipeline/status`, { headers: apiHeaders() });
  if (!res.ok) {
    throw await parseApiError(res, "Unable to load pipeline status.");
  }
  return res.json();
}

export async function fetchOpportunities(filters: {
  ministryType: string;
  state?: string;
  minScore: number;
  limit: number;
}): Promise<OpportunityRecord[]> {
  const params = new URLSearchParams({
    ministry_type: filters.ministryType,
    min_score: String(filters.minScore),
    limit: String(filters.limit),
  });

  if (filters.state) {
    params.set("state", filters.state);
  }

  const res = await fetch(`${API_URL}/api/opportunities?${params.toString()}`, { headers: apiHeaders() });
  if (!res.ok) {
    throw await parseApiError(res, "Unable to load opportunities.");
  }

  return res.json();
}

export async function fetchScoringWeights(): Promise<ScoringWeightsResponse> {
  const res = await fetch(`${API_URL}/api/scoring/weights`, { headers: apiHeaders() });
  if (!res.ok) {
    throw await parseApiError(res, "Unable to load methodology weights.");
  }

  return res.json();
}

export async function updatePortfolioWorkspaceCandidates(
  workspaceId: string,
  candidateLocations: PortfolioWorkspaceResponse["candidate_locations"],
): Promise<PortfolioWorkspaceResponse> {
  const res = await fetch(`${API_URL}/api/portfolio/workspaces/${workspaceId}`, {
    method: "PATCH",
    headers: apiHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify({ candidate_locations: candidateLocations }),
  });

  if (!res.ok) {
    throw await parseApiError(res, "Unable to save candidate location to workspace.");
  }

  return res.json();
}


export async function fetchPortfolioWorkspace(workspaceId: string): Promise<PortfolioWorkspaceResponse> {
  const res = await fetch(`${API_URL}/api/portfolio/workspaces/${workspaceId}`, { headers: apiHeaders() });
  if (!res.ok) {
    throw await parseApiError(res, "Unable to load portfolio workspace.");
  }

  return res.json();
}
