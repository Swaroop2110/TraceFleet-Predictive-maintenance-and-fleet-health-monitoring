import type { FleetStats, Incident, Vehicle } from "../types";
export interface Session { access_token: string; refresh_token: string; tenant_id: string; roles: string[]; permissions: string[]; }
export interface Page<T> { items: T[]; pagination: { total?: number; next_cursor?: string; has_more?: boolean }; }
let session: Session | null = JSON.parse(sessionStorage.getItem("tracefleet-session") || "null");
export function getSession() { return session; }
export function setSession(value: Session | null) { session = value; if (value) sessionStorage.setItem("tracefleet-session", JSON.stringify(value)); else sessionStorage.removeItem("tracefleet-session"); }
let refreshPromise: Promise<void> | null = null;
export async function request<T>(path: string, init: RequestInit = {}, retry = true): Promise<T> {
  const headers = new Headers(init.headers); headers.set("Content-Type", "application/json");
  if (session) headers.set("Authorization", `Bearer ${session.access_token}`);
  const response = await fetch(`/api/v1${path}`, { ...init, headers });
  if (response.status === 401 && session && retry && path !== "/auth/token") {
    if (!refreshPromise) refreshPromise = fetch("/api/v1/auth/refresh", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ refresh_token: session.refresh_token }) }).then(async r => { if (!r.ok) { setSession(null); window.dispatchEvent(new Event("session-expired")); throw new Error("Session expired. Sign in again."); } setSession(await r.json()); }).finally(() => { refreshPromise = null; });
    await refreshPromise; return request<T>(path, init, false);
  }
  if (!response.ok) { const data = await response.json().catch(() => ({})); throw new Error(data.error?.message || data.detail || `Request failed (${response.status})`); }
  return response.json() as Promise<T>;
}
export const apiClient = {
  login: (username: string, password: string, tenant_id: string) => request<Session>("/auth/token", { method: "POST", body: JSON.stringify({ username, password, tenant_id }) }),
  features: () => request<Record<string, { enabled: boolean }>>("/config/features"),
  listVehicles: async () => (await request<Page<Vehicle>>("/vehicles?limit=1000")).items,
  listIncidents: async () => (await request<Page<Incident>>("/incidents?limit=500")).items,
  acknowledgeIncident: (id: string) => request<Incident>(`/incidents/${id}/acknowledge`, { method: "POST" }),
  closeIncident: (id: string) => request<Incident>(`/incidents/${id}/close`, { method: "POST" }),
  incidentSummary: (id: string) => request<AISummary>(`/incidents/${id}/ai-summary`, { method: "POST", body: "{}" }),
  fleetAnalytics: () => request<FleetStats>("/analytics/fleet"),
};
export interface AISummary { available: boolean; unavailable_reason?: string; disclaimer: string; uncertainty: string; limitations: string; run_id: string; summary_sections: { heading: string; statements: { text: string; type: string; evidence_ids: string[] }[] }[]; validation: { passed: boolean }; }
export async function downloadExport(id: string, format: string) { const response = await fetch(`/api/v1/incidents/${id}/export?format=${format}`, { headers: { Authorization: `Bearer ${getSession()?.access_token}` } }); if (!response.ok) throw new Error("Export failed"); const url = URL.createObjectURL(await response.blob()); const a = document.createElement("a"); a.href = url; a.download = `${id}.${format}`; a.click(); URL.revokeObjectURL(url); }
