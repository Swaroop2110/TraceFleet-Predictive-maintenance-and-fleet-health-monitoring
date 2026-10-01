import http from "k6/http";
import { check, sleep } from "k6";
const baseUrl = __ENV.TRACEFLEET_API_URL || "http://localhost:8000";
export const options = {
  stages: [{ duration: "15s", target: 10 }, { duration: "30s", target: 30 }, { duration: "15s", target: 0 }],
  thresholds: { http_req_failed: ["rate<0.01"], http_req_duration: ["p(95)<500", "p(99)<1000"] },
};
export function setup() {
 const auth = http.post(`${baseUrl}/api/v1/auth/token`, JSON.stringify({ username: "analyst", password: "tracefleet-demo", tenant_id: "tenant_default" }), {headers:{"Content-Type":"application/json"}});
 if (auth.status !== 200) throw new Error(`Login failed: ${auth.status}`);
 return {token:auth.json("access_token")};
}
export default function ({token}) {
 const response = http.get(`${baseUrl}/api/v1/incidents?limit=50`, {headers:{Authorization:`Bearer ${token}`}});
 check(response, {"authenticated incident query succeeds":r=>r.status===200});
 sleep(Number(__ENV.THINK_SECONDS || 3));
}
