# Threat model

## Assets and boundaries

Protect tenant telemetry/location, incident evidence, human lifecycle decisions, authentication keys, owner database credentials and audit/AI provenance. Kafka producers, processor owners and maintenance jobs are trusted components. Browser tokens grant tenant-scoped permissions; client-supplied tenant IDs are not authority.

## Controls and evidence

| Threat | Implemented control | Verification |
| --- | --- | --- |
| Cross-tenant reads | Restricted Postgres role + RLS + transaction-local context; explicit API/ClickHouse tenant predicates; tenant Redis channels | Real-store API and raw SQL isolation tests |
| Unauthorized changes | RS256 signature/type checks and YAML RBAC; owner credentials confined to trusted writers/jobs | Role matrix and lifecycle integration tests |
| Location disclosure | Permission-based coordinate/geohash/VIN masking | Viewer API privacy test |
| Evidence loss/replay | Durable flush before commit, per-partition commits, pending dedup claims, checkpoint recovery, idempotent upserts | Consumer/broker interruption and reconciliation reports |
| Audit rewriting | UPDATE/DELETE grants revoked; append-only trigger; owner-only purge function with fixed search path | Restricted-role SQL rejection tests |
| Malicious AI text | Sanitized evidence package, known citations, numeric grounding, fact/inference separation, prohibited fault attribution | AI safety suite and saved validated runs |
| Retained subject identity | Verified cross-store erasure, checkpoint cleanup, replay suppression | Isolated real-store erasure test |
| Dependency vulnerabilities | Locked dependencies and automated Bandit/pip-audit/npm audit/Trivy reports | Local scan artifacts and CI jobs |
| Abuse/oversized requests | Shared token bucket, body-size limits, auth checks and security headers | Load test respects per-user quota; failures remain structured |

## Residual deployment risks

Single-node Compose has no node/disk-loss tolerance. Production must provide transport security, Kafka ACLs, managed secrets, replication, backups, restore drills and protected Redis durability. The owner role bypasses RLS by design and must never be used for browser-facing requests. Invalid upstream messages may contain identifying data in quarantine; protect access and apply retention/erasure. Kafka erasure is bounded by topic retention, so compliance policy must account for that source-log window. A joblib anomaly model is executable serialization and must come from a trusted build.

Tokens stored in the browser session remain sensitive to XSS; use strict origin controls/CSP and an appropriately secured authentication gateway for production. Refresh tokens have a signed expiry but do not provide a revocation-list deployment. Real external Gemini availability/cost and production OIDC integration were not exercised. A synthetic scenario score is a development metric, not validation of road safety or fault attribution.
