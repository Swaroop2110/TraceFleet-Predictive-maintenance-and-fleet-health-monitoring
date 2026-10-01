import { useEffect, useState } from "react";
import { Activity, AlertTriangle, Car, Route } from "lucide-react";
import { apiClient } from "../../api/client";
import { Card } from "../../components/Card";
import type { FleetStats } from "../../types";

const colors: Record<string, string> = { CRITICAL: "bg-red-500", HIGH: "bg-orange-500", MEDIUM: "bg-yellow-400", LOW: "bg-blue-500", INFO: "bg-slate-500" };

export function AnalyticsPanel() {
  const [stats, setStats] = useState<FleetStats | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    const refresh = async () => { try { setStats(await apiClient.fleetAnalytics()); setError(null); } catch { setError("Analytics are temporarily unavailable."); } };
    void refresh();
    const timer = window.setInterval(() => void refresh(), 10000);
    return () => window.clearInterval(timer);
  }, []);
  if (!stats) return <main className="flex flex-1 items-center justify-center bg-slate-950 text-slate-400">{error || "Loading fleet analytics…"}</main>;
  const metrics = [
    ["Active Vehicles (24h)", stats.active_vehicles_24h, Car, "text-blue-400"],
    ["Average Speed", `${stats.avg_speed_kmh} km/h`, Activity, "text-emerald-400"],
    ["Total Distance", `${stats.total_distance_km} km`, Route, "text-violet-400"],
    ["Total Incidents", stats.total_incidents, AlertTriangle, "text-orange-400"],
  ] as const;
  return <main className="flex-1 overflow-y-auto bg-slate-950 p-8"><h2 className="mb-8 text-2xl font-bold text-white">Fleet analytics</h2>
    <div className="mb-8 grid grid-cols-1 gap-5 md:grid-cols-2 xl:grid-cols-4">{metrics.map(([label, value, Icon, color]) => <Card key={label} className="p-5"><div className="mb-4 flex items-center justify-between text-sm text-slate-400">{label}<Icon className={`h-5 w-5 ${color}`} /></div><p className="text-3xl font-bold text-white">{value}</p></Card>)}</div>
    <Card className="max-w-3xl p-6"><h3 className="mb-6 font-semibold text-slate-200">Incidents by severity</h3>{Object.entries(stats.incidents_by_severity).length ? <div className="space-y-4">{Object.entries(stats.incidents_by_severity).map(([severity, count]) => <div key={severity} className="flex items-center gap-3"><span className="w-20 text-xs font-bold text-slate-300">{severity}</span><div className="h-2 flex-1 overflow-hidden rounded bg-slate-800"><div className={`h-full rounded ${colors[severity] || colors.INFO}`} style={{ width: `${Math.max(3, count / Math.max(1, stats.total_incidents) * 100)}%` }} /></div><span className="w-6 text-right text-sm text-white">{count}</span></div>)}</div> : <p className="text-sm text-slate-500">No incidents recorded yet.</p>}</Card>
  </main>;
}
