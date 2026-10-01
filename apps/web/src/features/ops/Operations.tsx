import { useEffect, useState } from "react";
import { request } from "../../api/client";
interface Pipeline { accepted_events_per_s: number; target_events_per_s: number; consumer_lag: { group: string; total: number; max_partition: number; partitions: number }[]; counters: Record<string, number>; latency: Record<string, number>; degraded: string[]; throughput?: {events_per_second: number; target_events_per_second: number}; }
const readable = (name: string) => name.replaceAll("_", " ").replace("total", "");
export function Operations() {
  const [data, setData] = useState<Pipeline | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    const refresh = () => request<Pipeline>("/ops/pipeline").then(next => { setData(next); setError(""); }).catch(e => setError(e.message));
    void refresh(); const timer = setInterval(refresh, 5000); return () => clearInterval(timer);
  }, []);
  return <section className="page"><p className="eyebrow">Platform health</p><h1>Operations</h1><p>Metrics refresh every five seconds. Kafka buffers telemetry while a downstream store recovers.</p>{error && <p role="alert" className="error">{error}</p>}{data && <>
    {!!data.degraded?.length && <p role="alert" className="error">{data.degraded.join(" · ")}</p>}
    <div className="panel"><h2>Ingestion throughput</h2><p>{data.throughput?.events_per_second.toLocaleString() || "0"} events/second · configured target {data.throughput?.target_events_per_second.toLocaleString()}</p></div>
    <div className="metric-grid">{Object.entries(data.counters || {}).map(([name, value]) => <div className="panel" key={name}><small>{readable(name)}</small><strong>{Number(value).toLocaleString()}</strong></div>)}</div>
    <div className="panel"><h2>Consumer lag</h2><div className="table-scroll"><table><thead><tr><th>Consumer group</th><th>Queued events</th><th>Largest partition</th><th>Partitions</th></tr></thead><tbody>{(data.consumer_lag || []).map(item => <tr key={item.group}><td>{item.group}</td><td>{item.total.toLocaleString()}</td><td>{item.max_partition.toLocaleString()}</td><td>{item.partitions}</td></tr>)}</tbody></table></div></div>
    <div className="panel"><h2>Latency</h2>{Object.entries(data.latency || {}).map(([name, value]) => <p key={name}>{readable(name)}: {Number.isFinite(value) && value > 0 ? `${(value * 1000).toFixed(1)} ms` : "No recent samples"}</p>)}</div>
  </>}</section>;
}
