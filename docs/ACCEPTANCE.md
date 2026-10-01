# Plan acceptance and evidence

Checked 2026-10-01 against the supplied IMPLEMENTATION_PLAN.md. Checked items describe the local implementation and observed tests, not a production certification. The separate PRD §20 text was not supplied; the table maps the plan's stated requirements.

| Gate | Status | Evidence / repeatable command |
| --- | --- | --- |
| Repository/config/app/migrations/Compose foundation | [x] | `make up`; healthy local services; `make lint typecheck` |
| Manifest dependencies and feature removal | [x] | `test_feature_matrix.py`: immutable snapshots, dependency rejection and AI/ops/export routes absent when disabled; permission/feature browser navigation |
| Raw → canonical → ClickHouse and Redis | [x] | Real-store integration suite; event-time ordering and tenant-bound state |
| Quarantine with reasons and duplicate suppression | [x] | Schema-fault + duplicate-storm real-store assertions, callback/DLQ durability tests |
| Incident plug-ins, lifecycle, reconstruction and APIs | [x] | Collision/overspeed and negative matrix; evidence/factors/timeline; acknowledge/close/replay tests |
| JWT/RBAC/tenant isolation/privacy | [x] | Signed token type checks, role matrix, viewer masking, cross-tenant API/SQL rejection and cross-tenant erasure rejection |
| Browser investigation workflow | [x] | Four Playwright tests pass in development and production: analyst, viewer, critical push latency and admin operations/catalogues |
| Required BDD and ≥95% ground truth | [x] | Nine BDD cases; eight hidden-label scenarios score 100% precision/recall/accuracy/severity/signals/timing |
| 100K seeding and measured ingest/API benchmark | [x] | `seed-100k.json`; `benchmark.json`; `docs/BENCHMARKS.md` |
| Bounded lag and dashboard/critical-alert targets | [x] | Latest 2K/s pass peak 1,021 queued events, drain zero; browser dashboard 205 ms and critical alert 1,656 ms |
| Analytics/Operations/Prometheus/Grafana | [x] | API + browser checks; `prometheus-check.log` validates configuration and 16 rules |
| Optional anomaly comparison | [x] | `anomaly-comparison.json`; scores are supplemental and do not alter rule triggers/severity |
| Evidence-cited AI, safety, audit and exports | [x] | AI safety suite, grounded citations/cache/history and real JSON/CSV/PDF export tests |
| AI feature disabled | [x] | OpenAPI feature-off matrix; UI summary panel respects manifest/permissions |
| RLS and immutable audit | [x] | Restricted-role raw SQL tests: missing/other tenant sees zero; audit UPDATE/DELETE and public purge denied |
| Retention + verified cross-store erasure | [x] | Retention dry-run/mutation report; full erasure request job completion, zero identifying rows/caches, replay and tenant ownership tests; Kubernetes schedules |
| End-to-end processor traces and alerts | [x] | `trace-verification.json` includes validator/incident/sink/persister on one trace; rules validate |
| Chaos recovery without unreconciled loss | [x] | Consumer and broker reports each reconcile one CRITICAL incident and all seven unique records |
| Soak + load scripts and observed run | [x] | Five-minute soak, 26 samples, final lag zero; paced authenticated k6 run has zero failed requests |
| Security scan reports | [x] | Bandit/pip-audit/npm audit/Trivy local reports; no configured medium/high or high/critical findings |
| CI and ≥80% coverage | [x] | 114 real-store-inclusive tests pass; 80.36% coverage gate passes; CI runs quality, coverage, browser, recovery, trace and security jobs |
| Kubernetes deployment artifacts | [x] | Both overlays render; web proxy, mounted profiles, shared JWT secret, migration and scheduled compliance jobs included |
| README/screenshots/diagrams/ADRs/runbook/demo | [x] | README, docs/images, architecture/system.md, ADRs, EXTENDING.md, RUNBOOK.md and DEMO_SCRIPT.md |
| pgvector similarity | Skipped as permitted | Optional flag remains disabled |

## Limits of these checks

100K events/s is not claimed on this laptop; the measured dev ingestion target is 2K/s. The five-minute run is a local soak, not extended production endurance. Cluster deployment, multi-node/disk-loss recovery, restore drills, external Gemini and a hosted fresh-clone CI execution need their target environments. The mock AI provider was tested. Synthetic labels provide development coverage and do not establish real-world road-safety accuracy. Kafka source-log retention remains a privacy deployment policy; the cross-store job does not rewrite Kafka records.
