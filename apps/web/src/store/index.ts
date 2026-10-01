import { useState } from "react";

export type DashboardView = "MAP" | "ANALYTICS";

export function useDashboardStore() {
  const [currentView, setCurrentView] = useState<DashboardView>("MAP");
  const [selectedIncidentId, setSelectedIncidentId] = useState<string | null>(null);
  return { currentView, setCurrentView, selectedIncidentId, setSelectedIncidentId };
}
