import { useEffect } from "react";
import { MapContainer, Marker, Popup, TileLayer, useMap } from "react-leaflet";
import L from "leaflet";
import type { Incident, Vehicle } from "../../types";

interface FleetOverviewProps { vehicles: Vehicle[]; incidents: Incident[]; selectedIncidentId: string | null; }

function MapFocus({ vehicle }: { vehicle?: Vehicle }) {
  const map = useMap();
  useEffect(() => { if (vehicle) map.flyTo([vehicle.lat, vehicle.lon], 16, { duration: 0.8 }); }, [map, vehicle]);
  return null;
}

function vehicleIcon(speed: number) {
  const color = speed > 90 ? "#ef4444" : speed > 70 ? "#f97316" : "#38bdf8";
  return L.divIcon({ html: `<span style="display:block;width:18px;height:18px;border-radius:50%;border:2px solid white;background:${color};box-shadow:0 0 12px ${color}"></span>`, className: "", iconSize: [18, 18], iconAnchor: [9, 9] });
}

export function FleetOverview({ vehicles, incidents, selectedIncidentId }: FleetOverviewProps) {
  const selectedIncident = incidents.find((incident) => incident.incident_id === selectedIncidentId);
  const selectedVehicle = vehicles.find((vehicle) => vehicle.id === selectedIncident?.vehicle_id);
  const visibleVehicles = vehicles.filter((vehicle) => Number.isFinite(vehicle.lat) && Number.isFinite(vehicle.lon));
  return <main className="relative flex-1"><MapContainer center={[21.17, 72.83]} zoom={13} className="h-full w-full" zoomControl={false}>
    <TileLayer attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors' url="https://tile.openstreetmap.org/{z}/{x}/{y}.png" maxZoom={19} />
    <MapFocus vehicle={selectedVehicle} />
    {visibleVehicles.map((vehicle) => <Marker key={vehicle.id} position={[vehicle.lat, vehicle.lon]} icon={vehicleIcon(vehicle.speed)}><Popup><div className="min-w-36 p-1 text-sm"><strong>{vehicle.id}</strong><br />Speed: {vehicle.speed.toFixed(1)} km/h<br />Battery: {vehicle.soc === null ? "N/A" : `${vehicle.soc.toFixed(1)}%`}{vehicle.dtc.length > 0 && <><br />Faults: {vehicle.dtc.join(", ")}</>}</div></Popup></Marker>)}
  </MapContainer></main>;
}
