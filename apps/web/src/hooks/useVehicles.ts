import { useCallback, useEffect, useState } from "react";
import { apiClient } from "../api/client";
import type { Vehicle } from "../types";

function normalizeVehicle(value: Vehicle): Vehicle {
  return {
    ...value,
    lat: Number(value.lat), lon: Number(value.lon), speed: Number(value.speed),
    soc: value.soc === null || value.soc === undefined ? null : Number(value.soc),
    heading: value.heading === undefined ? undefined : Number(value.heading),
    dtc: Array.isArray(value.dtc) ? value.dtc : [],
  };
}

export function useVehicles(intervalMs = 5000) {
  const [vehicles, setVehicles] = useState<Vehicle[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const refresh = useCallback(async () => {
    try {
      setVehicles((await apiClient.listVehicles()).map(normalizeVehicle));
      setError(null);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "Unable to load vehicles");
    } finally {
      setLoading(false);
    }
  }, []);
  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), intervalMs);
    return () => window.clearInterval(timer);
  }, [intervalMs, refresh]);
  return { vehicles, error, loading, refresh };
}
