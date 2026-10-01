# Operational runbook

## Start and verify

`cp .env.example .env && make up` builds services, creates topics, runs migrations and waits for health. `make demo` seeds and injects synthetic incidents without clearing existing data. Use tenant `tenant_default`, user `analyst`, password `tracefleet-demo`. On Windows, `Run-Demo.ps1` starts the stack and injects examples.

Run `make test lint typecheck`, then `make coverage` against the running stack. The integration suite uses uniquely named test vehicles/tenants. `make evaluate` scores hidden labels; `make chaos-recover` interrupts the local processor and broker and reconciles one critical incident and seven unique telemetry records. `make bench`, `make load-test`, and `make soak SOAK_MINUTES=30` produce measured reports. `make scan` produces Bandit, Python/npm audit and Trivy reports; CI runs Trivy when the local binary is unavailable.

## Health and incident response

- API liveness: `/api/v1/health/live`; readiness: `/api/v1/health/ready`.
- Operations: throughput, lag by partition, paused groups, rejection causes and latency.
- Prometheus: `localhost:9090`; Grafana: `localhost:3000`; Tempo: `localhost:3200`.
- `docker compose logs --tail=100 SERVICE` explains a stalled dependency or poison record.

An increasing lag while a group is paused means Kafka is buffering during downstream failure. Restore the store, then verify the group resumes and lag drains. Never advance offsets to clear a backlog. Inspect the DLQ before replaying malformed records. On a processor restart, Redis checkpoints restore windows/candidates; preserve the Redis persistence volume and avoid evicting checkpoints/erasure markers in production.

Critical incidents are published immediately. Reconstruction collects the configured post window, then finalizes on an event-time boundary or timeout. Acknowledge after investigation; manager/admin may close. Replay must preserve these human states. AI remains explanatory and cannot change severity or lifecycle.

## Retention and erasure

Preview with `make retention RETENTION_ARGS="--dry-run --verify"`. Apply with `make retention`; reports describe each store and mutation status. Kubernetes schedules retention daily and pending erasure every 15 minutes.

Submit erasure through Administration or `/api/v1/admin/erasure`, then `make erasure`. Completion requires Postgres pseudonymisation/deletion, zero raw telemetry/per-vehicle rollup/quarantine rows in ClickHouse, and Redis state/checkpoint/cache cleanup. Verify the report before marking a failed request completed. Erasure blocks subsequent ingestion through a minimal vehicle-ID ledger; protect that ledger. Tenant/fleet aggregate statistics without vehicle identity and append-only compliance audit remain. Kafka source data ages out under topic retention; individual records in an append-only Kafka log are not synchronously deleted by this job.

## Deploying Kubernetes

`make k8s-validate` renders both overlays. Supply managed datastore connections and replace placeholder secrets. Create `tracefleet-jwt-keys` with `jwt_private.pem`/`jwt_public.pem`; replicas must share signing keys. Build `tracefleet-web` with the production Docker target. Configure ingress/TLS and label datastore destinations to match network policies; managed external stores need explicit egress rules. Run migration before accepting traffic. Kubernetes manifests have been rendered locally; no cluster deployment or disaster-recovery drill is implied by rendering.

## Recovery limits

The local single-broker stack proves process restart/replay, not disk loss or multi-node failover. Production requires replicated Kafka/Redis/databases, backups, restore exercises and measured capacity at the deployment's scale. Test long soaks and resource limits on representative hardware. Development credentials, generated signing keys and permissive local ports must be replaced before external exposure.
