import { useCallback, useEffect, useState } from "react";
import { request } from "../../api/client";
type Row = Record<string, unknown>;
interface Result { items?: Row[]; roles?: Record<string, Row>; }
const columns: Record<string, string[]> = { users: ["username", "display_name", "roles", "active", "last_login_at"], roles: ["role", "description", "permissions"], erasure: ["subject_id", "status", "reason", "created_at", "processed_at"], audit: ["timestamp", "actor", "action", "resource", "outcome"] };
const display = (value: unknown): string => Array.isArray(value) ? value.join(", ") : value == null ? "—" : typeof value === "boolean" ? value ? "Active" : "Inactive" : String(value);
export function Administration() {
  const [tab, setTab] = useState("users"); const [data, setData] = useState<Result>({}); const [error, setError] = useState("");
  const [username, setUsername] = useState(""); const [password, setPassword] = useState(""); const [vehicle, setVehicle] = useState(""); const [busy, setBusy] = useState(false);
  const refresh = useCallback(async () => { try { setData(await request<Result>(`/admin/${tab}`)); setError(""); } catch (e) { setError(e instanceof Error ? e.message : "Request failed"); } }, [tab]);
  useEffect(() => { setData({}); void refresh(); }, [refresh]);
  const submit = async (path: string, payload: unknown) => { setBusy(true); try { await request(path, { method: "POST", body: JSON.stringify(payload) }); setPassword(""); await refresh(); } catch (e) { setError(e instanceof Error ? e.message : "Request failed"); } finally { setBusy(false); } };
  const rows: Row[] = tab === "roles" ? Object.entries(data.roles || {}).map(([role, details]) => ({ role, ...details })) : data.items || [];
  return <section className="page"><p className="eyebrow">Tenant administration</p><h1>Administration</h1><div className="actions">{["users", "roles", "erasure", "audit"].map(t => <button key={t} aria-pressed={tab === t} onClick={() => setTab(t)}>{t[0].toUpperCase() + t.slice(1)}</button>)}</div>
    {error && <p role="alert" className="error">{error}</p>}
    {tab === "users" && <form className="actions" onSubmit={e => { e.preventDefault(); void submit("/admin/users", { username, password, roles: ["VIEWER"] }); }}><label>Username<input value={username} onChange={e => setUsername(e.target.value)} required /></label><label>Password · 12 characters minimum<input type="password" value={password} minLength={12} onChange={e => setPassword(e.target.value)} required /></label><button disabled={busy}>Create viewer</button></form>}
    {tab === "erasure" && <><p>Requests are completed only after deletion is verified in each store. Audit records remain available.</p><form className="actions" onSubmit={e => { e.preventDefault(); void submit("/admin/erasure", { subject_type: "vehicle", subject_id: vehicle, reason: "Requested through tenant administration" }); }}><label>Vehicle ID<input value={vehicle} onChange={e => setVehicle(e.target.value)} required /></label><button disabled={busy}>Request erasure</button></form></>}
    <div className="panel table-scroll"><table><thead><tr>{columns[tab].map(key => <th key={key}>{key.replaceAll("_", " ")}</th>)}</tr></thead><tbody>{rows.map((row, i) => <tr key={String(row.user_id || row.id || i)}>{columns[tab].map(key => <td key={key}>{display(row[key])}</td>)}</tr>)}</tbody></table>{!rows.length && <p>No records to display.</p>}</div>
    <button disabled={busy} onClick={() => void submit("/admin/config/reload", {})}>Reload validated configuration</button>
  </section>;
}
