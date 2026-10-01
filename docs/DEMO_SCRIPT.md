# Five-minute TraceFleet demo

Run `make demo` and open `http://localhost:5173`. Sign in as `analyst`, tenant `tenant_default`, password `tracefleet-demo`. Keep Grafana in a second tab. Development mode uses the deterministic AI provider unless a real provider is explicitly configured.

| Time | Show | Explain |
| --- | --- | --- |
| 0:00–0:45 | Fleet overview, then Live incidents | Telemetry flows through Kafka validation, deduplication and deterministic detection. Location precision follows role permissions. |
| 0:45–1:45 | Collision-like CRITICAL and overspeed HIGH cards | Signal chains determine severity. Default thresholds require at least two distinct signals, so the LOW matrix row is intentionally unreachable under this trigger. |
| 1:45–3:00 | Open a collision, inspect timeline, factors and linked evidence | Highlight a factor's evidence. Generate the cited narrative; click a citation and show validation and run provenance. Export JSON, CSV or PDF. AI separates facts/inferences and does not assign fault. |
| 3:00–3:40 | Acknowledge, then Analytics | Analyst can acknowledge and export; manager can close. Historical analytics may lag live alerts. |
| 3:40–4:30 | Sign in as manager; Operations/Grafana/Tempo | Show throughput, lag, pause state and sampled Kafka traces. Recovery scripts reconcile incident and telemetry counts. |
| 4:30–5:00 | Sign in as admin; users, roles, erasure and audit | RLS protects tenant rows, audit is append-only, and erasure completes only after each store verifies deletion. Close with measured benchmark results and the single-node deployment limit. |

Inject another scenario while the browser is connected:

```sh
docker compose run --rm simulator python apps/simulator/sim/main.py kafka:9092 --scenario collision_like --batches 1 --vehicle-prefix demo_live
```

The browser uses an authenticated tenant WebSocket, with polling/reconnection as fallback. Incidents move through DETECTED/RECONSTRUCTING before OPEN; a finite injected stream can wait for the configured reconstruction timeout. The optional pgvector feature is disabled. Do not present synthetic scenario accuracy, short soak results or laptop throughput as real-world safety validation or production capacity.
