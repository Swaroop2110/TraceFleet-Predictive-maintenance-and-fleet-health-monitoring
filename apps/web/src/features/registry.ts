export interface View { id: string; label: string; permission: string; feature?: string; }
export const views: View[] = [
 { id: "MAP", label: "Fleet overview", permission: "vehicle:read" },
 { id: "INCIDENTS", label: "Live incidents", permission: "incident:read", feature: "incident_detection_rules" },
 { id: "ANALYTICS", label: "Analytics", permission: "analytics:read", feature: "clickhouse_sink" },
 { id: "OPS", label: "Operations", permission: "ops:read", feature: "ops_dashboard" },
 { id: "ADMIN", label: "Administration", permission: "admin:users", feature: "admin_ui" },
];
export function enabledViews(features: Record<string, { enabled: boolean }>, permissions: string[]) { return views.filter(view => permissions.includes(view.permission) && (!view.feature || features[view.feature]?.enabled)); }
