import { AlertCircle, BarChart2, Car, Map as MapIcon } from "lucide-react";
import { IncidentDetail } from "../features/incident/IncidentDetail";
import type { DashboardView } from "../store";
import type { Incident } from "../types";
import { cn } from "../lib/utils";

interface AppSidebarProps {
  currentView: DashboardView;
  onViewChange: (view: DashboardView) => void;
  incidents: Incident[];
  selectedIncidentId: string | null;
  onIncidentSelect: (incident: Incident) => void;
  onIncidentChange: () => Promise<void>;
}

export function AppSidebar({ currentView, onViewChange, incidents, selectedIncidentId, onIncidentSelect, onIncidentChange }: AppSidebarProps) {
  const activeIncidents = incidents.filter((incident) => incident.status === "OPEN");
  return <aside className="z-[1000] flex w-80 shrink-0 flex-col border-r border-white/10 bg-slate-950/85 shadow-2xl backdrop-blur-xl">
    <div className="border-b border-white/5 p-6">
      <div className="mb-6 flex items-center gap-2">
        <div className="rounded-lg bg-blue-600 p-2 shadow-[0_0_15px_rgba(37,99,235,0.4)]"><Car className="h-6 w-6 text-white" /></div>
        <h1 className="text-xl font-bold tracking-wider text-white">TraceFleet</h1>
      </div>
      <div className="flex gap-1 rounded-lg bg-slate-950 p-1">
        {(["MAP", "ANALYTICS"] as DashboardView[]).map((view) => <button key={view} onClick={() => onViewChange(view)} className={cn("flex flex-1 items-center justify-center gap-2 rounded-md px-3 py-2 text-sm font-semibold transition-colors", currentView === view ? "bg-slate-800 text-white shadow" : "text-slate-500 hover:text-slate-300")}>
          {view === "MAP" ? <MapIcon className="h-4 w-4" /> : <BarChart2 className="h-4 w-4" />}{view === "MAP" ? "Map" : "Analytics"}
        </button>)}
      </div>
    </div>
    <div className="custom-scrollbar flex-1 overflow-auto p-4">
      <h2 className="mb-4 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-slate-400"><AlertCircle className="h-4 w-4" />Active Incidents ({activeIncidents.length})</h2>
      <div className="space-y-3">
        {activeIncidents.map((incident) => <IncidentDetail key={incident.incident_id} incident={incident} expanded={selectedIncidentId === incident.incident_id} onSelect={() => onIncidentSelect(incident)} onChanged={onIncidentChange} />)}
        {!activeIncidents.length && <div className="rounded-xl border-2 border-dashed border-slate-800 py-12 text-center text-sm font-medium text-slate-600">No active incidents</div>}
      </div>
    </div>
  </aside>;
}
