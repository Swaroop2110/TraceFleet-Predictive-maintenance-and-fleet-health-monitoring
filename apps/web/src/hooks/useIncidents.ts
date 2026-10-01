import { useCallback, useEffect, useState } from "react";
import { apiClient, getSession } from "../api/client";
import type { Incident } from "../types";
export function useIncidents(intervalMs = 10000, live = true) {
 const [incidents, setIncidents] = useState<Incident[]>([]); const [error, setError] = useState<string | null>(null);
 const refresh = useCallback(async () => { try { setIncidents(await apiClient.listIncidents()); setError(null); } catch (e) { setError(e instanceof Error ? e.message : "Unable to load incidents"); } }, []);
 useEffect(() => { let closed = false; let socket: WebSocket | null = null; let reconnect: number | undefined; void refresh(); const timer = window.setInterval(() => void refresh(), intervalMs);
  const connect = () => { const token = getSession()?.access_token; if (!live || !token || closed) return; const protocol = location.protocol === "https:" ? "wss" : "ws"; socket = new WebSocket(`${protocol}://${location.host}/api/v1/ws/live?token=${encodeURIComponent(token)}`);
   socket.onmessage = ({ data }) => { try { const next = JSON.parse(data); if (next.incident_id) setIncidents(current => [next, ...current.filter(i => i.incident_id !== next.incident_id)]); } catch { void refresh(); } };
   socket.onclose = () => { if (!closed) reconnect = window.setTimeout(connect, 3000); }; };
  connect(); return () => { closed = true; clearInterval(timer); clearTimeout(reconnect); socket?.close(); };
 }, [intervalMs, refresh, live]); return { incidents, error, refresh };
}
