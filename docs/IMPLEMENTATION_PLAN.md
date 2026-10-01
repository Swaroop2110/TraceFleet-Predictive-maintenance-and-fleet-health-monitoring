# IMPLEMENTATION PLAN - TraceFleet

## Phase 0 – Foundation
**Tasks:**
- Set up repository skeleton (`apps/api`, `apps/stream-processor`, `apps/simulator`, `apps/web`, `packages/`, `config/`, `infra/`, `tests/`, `docs/`, `scripts/`).
- Create `packages/common` (feature manifest, config loader with Pydantic validation, logging, otel bootstrap).
- Set up FastAPI app factory with module registration (`apps/api/app/main.py`).
- Create React app with feature registry in `apps/web`.
- Set up Postgres + Alembic base migration.
- Create `docker-compose.yml` (postgres, api, web).
- Create `Makefile`, `.env.example`.
- Write `scripts/feature.py`.
- Set up CI skeleton.
**Risks:**
- Feature manifest implementation needs to be very robust, as it dictates the entire architecture.
**Gate Commands:**
- `make up` (starts stack)
- `curl http://localhost:8000/api/v1/health` (OK)
- Web shows nav from `/config/features`
- `python scripts/feature.py disable X` removes X from nav and OpenAPI
- Feature-matrix test passes

## Phase 1 – Data Pipeline MVP
**Tasks:**
- Set up Kafka (KRaft) + init topics (`infra/docker/kafka-init.sh`).
- Create telemetry schema package (`packages/schemas`).
- Build basic simulator (normal events, dev profile).
- Implement stream-processor `validator` role, quarantine topic handling, dedup.
- Implement canonical topic processing.
- Build `telemetry-sink` → ClickHouse.
- Implement Redis hot state.
- Add API endpoint `/vehicles/{id}/state`.
**Risks:**
- Kafka and ClickHouse integrations require correct driver setup and configuration.
- Dedup in Redis needs correct atomicity (SET NX EX).
**Gate Commands:**
- Events flow: simulator → Kafka → ClickHouse
- Invalid events land in quarantine with reason
- Duplicates suppressed (tested via script)
- Redis state readable via API

## Phase 2 – Incident MVP
**Tasks:**
- Implement Detector/correlation/severity/factor plug-ins + YAML config.
- Build `incident` role in stream-processor.
- Create `incident-persister`.
- Implement incident lifecycle + state machine.
- Implement timeline reconstruction logic.
- Add incident/timeline/evidence/acknowledge APIs.
- Add JWT auth + RBAC + tenant scoping.
- Build dashboard pages: Fleet Overview, Live Incidents, Incident Investigation.
- Simulator scenarios: collision_like, overspeed_chain, false_positive_hard_brake, diagnostic_only, duplicate_storm, out_of_order, late_arrival, schema_fault.
- Build ground-truth writer + `evaluate_reconstruction.py`.
**Risks:**
- Bounded memory window correlation needs careful tuning.
- Timestamps handling (event time vs ingested time).
**Gate Commands:**
- Injected scenario travels pipeline to dashboard.
- BDD scenarios 1–4 and 7 pass.
- Ground-truth accuracy ≥95%.

## Phase 3 – Scale
**Tasks:**
- Implement 100K vehicle seeding (`make seed`).
- Build multi-worker simulator with rate control and burst.
- Implement batching/tuning of Kafka consumers.
- Configure ClickHouse batch inserts + rollups.
- Add analytics APIs + Analytics page.
- Add Operations page.
- Expose back-pressure/lag metrics.
- Set up Prometheus + Grafana dashboards.
- Create load-test scripts.
**Risks:**
- Reaching 100K ev/s locally might be hardware-limited.
- Backpressure implementation.
**Gate Commands:**
- `docs/BENCHMARKS.md` generated with measured throughput.
- Lag stays bounded under load.
- API p95/p99 measured.
- <2 s dashboard and <5 s critical alert traced.

## Phase 4 – Intelligence
**Tasks:**
- Refine factor scoring.
- Add optional anomaly model plug-in + comparison script.
- Build AI summary module (Provider interface, prompt, validator, audit).
- Add AI panel in UI.
- Implement evidence export.
- Optional pgvector similarity (skip if no time, per instructions).
**Risks:**
- LLM validation needs strict regex/pattern matching to block hallucinations.
**Gate Commands:**
- AI safety suite passes.
- Summaries contain 100% evidence-traceable claims.
- `ai_run` audit rows reconstruct each run.
- Feature matrix test passes with `ai_summary` disabled.

## Phase 5 – Production Engineering
**Tasks:**
- Enable RLS in Postgres.
- Harden audit logs.
- Add privacy masking.
- Implement retention + erasure job.
- Configure OTel traces end-to-end.
- Add Grafana dashboards + alert rules.
- Write chaos scripts (kill consumer/broker/pod).
- Add soak test.
- Security scans setup.
- k8s manifests.
- CI complete, coverage ≥80%.
**Risks:**
- Chaos testing might reveal missed states in stream processing.
**Gate Commands:**
- Chaos test proves recovery with no unreconciled incident loss.
- Scans produce reports.
- `make test` green.
- Coverage gate passes.

## Phase 6 – Submission Polish
**Tasks:**
- Write README (quick start, architecture, screenshots, demo script).
- Create `docs/architecture/*` diagrams.
- Write ADRs, RUNBOOK, THREAT_MODEL, CAP/PACELC discussion.
- Complete demo script (5-minute flow).
- Implement `make demo`.
- Tick off PRD §20 acceptance checklist in `docs/ACCEPTANCE.md`.
**Risks:**
- Missing documentation chunks.
**Gate Commands:**
- Fresh clone → documented commands → working demo.
- PRD §20 acceptance checklist fully ticked.
