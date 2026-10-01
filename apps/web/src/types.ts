export interface Vehicle {
  id: string;
  lat: number;
  lon: number;
  speed: number;
  soc: number | null;
  heading?: number;
  last_ts?: string;
  dtc: string[];
}

export interface Factor {
  factor_id?: string;
  factor_type: string;
  score?: number;
  confidence: number;
  evidence_ids?: string[];
  components?: Record<string, unknown>;
}

export interface Incident {
  incident_id: string;
  vehicle_id: string;
  incident_type: string;
  severity: "INFO" | "LOW" | "MEDIUM" | "HIGH" | "CRITICAL" | string;
  status: "OPEN" | "ACKNOWLEDGED" | "CLOSED" | string;
  start_ts: string;
  end_ts?: string | null;
  detected_at?: string;
  factors: Factor[];
}

export interface FleetStats {
  active_vehicles_24h: number;
  avg_speed_kmh: number;
  total_distance_km: number;
  total_incidents: number;
  incidents_by_severity: Record<string, number>;
}
