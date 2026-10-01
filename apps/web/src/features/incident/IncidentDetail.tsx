import { useState } from "react";
import { Check, FileText, Sparkles, X } from "lucide-react";
import { apiClient } from "../../api/client";
import { Button } from "../../components/Button";
import { Card } from "../../components/Card";
import { cn } from "../../lib/utils";
import type { Incident } from "../../types";

interface IncidentDetailProps {
  incident: Incident;
  expanded: boolean;
  onSelect: () => void;
  onChanged: () => Promise<void>;
}

const severityStyle: Record<string, string> = {
  CRITICAL: "border-red-500/50 bg-red-500/15 text-red-300",
  HIGH: "border-orange-500/50 bg-orange-500/15 text-orange-300",
  MEDIUM: "border-yellow-500/50 bg-yellow-500/15 text-yellow-300",
  LOW: "border-blue-500/50 bg-blue-500/15 text-blue-300",
  INFO: "border-slate-500/50 bg-slate-500/15 text-slate-300",
};

export function IncidentDetail({ incident, expanded, onSelect, onChanged }: IncidentDetailProps) {
  const [summary, setSummary] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [changing, setChanging] = useState(false);
  const generateSummary = async (event: React.MouseEvent) => {
    event.stopPropagation();
    setLoading(true);
    try { setSummary((await apiClient.incidentSummary(incident.incident_id)).summary_sections.flatMap(s => s.statements.map(x => x.text)).join("\n")); }
    catch { setSummary("Evidence summary is currently unavailable."); }
    finally { setLoading(false); }
  };
  const changeStatus = async (action: "acknowledgeIncident" | "closeIncident", event: React.MouseEvent) => {
    event.stopPropagation();
    setChanging(true);
    try { await apiClient[action](incident.incident_id); await onChanged(); }
    finally { setChanging(false); }
  };
  return <Card onClick={onSelect} className={cn("cursor-pointer overflow-hidden border-l-4 p-4 transition-colors hover:bg-slate-800", expanded ? "border-l-slate-300" : "border-l-slate-700")}>
    <div className="flex items-start justify-between gap-2">
      <div><p className="font-mono text-sm font-bold text-slate-100">{incident.vehicle_id}</p><p className="mt-1 text-[11px] uppercase tracking-wider text-slate-500">{incident.incident_type}</p></div>
      <span className={cn("rounded-full border px-2 py-1 text-[10px] font-bold tracking-wider", severityStyle[incident.severity] || severityStyle.INFO)}>{incident.severity}</span>
    </div>
    <div className="mt-3 flex justify-between text-xs text-slate-500"><span>{new Date(incident.start_ts).toLocaleTimeString()}</span><span>{expanded ? "Collapse" : "Locate"}</span></div>
    {expanded && <div className="mt-4 space-y-3 border-t border-slate-800 pt-3" onClick={(event) => event.stopPropagation()}>
      <div className="space-y-1">{incident.factors.map((factor, index) => <div key={`${factor.factor_type}-${index}`} className="flex justify-between rounded bg-slate-950/60 px-2 py-1.5 text-xs"><span className="capitalize text-slate-300">{factor.factor_type.replace(/_/g, " ")}</span><span className="font-bold text-emerald-400">{Math.round(factor.confidence * 100)}%</span></div>)}</div>
      {!summary ? <Button className="w-full" variant="secondary" onClick={generateSummary} disabled={loading}><Sparkles className="mr-1 inline h-3.5 w-3.5" />{loading ? "Building summary…" : "Evidence summary"}</Button> : <div className="rounded-lg border border-indigo-500/20 bg-indigo-950/30 p-3 text-xs leading-relaxed text-indigo-100"><FileText className="mr-1 inline h-3.5 w-3.5" />{summary}</div>}
      <div className="flex gap-2"><Button className="flex-1" variant="secondary" onClick={(event) => changeStatus("acknowledgeIncident", event)} disabled={changing}><Check className="mr-1 inline h-3.5 w-3.5" />Acknowledge</Button><Button variant="danger" onClick={(event) => changeStatus("closeIncident", event)} disabled={changing}><X className="h-3.5 w-3.5" /></Button></div>
    </div>}
  </Card>;
}
