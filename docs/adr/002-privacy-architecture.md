# ADR 002: Privacy-aware vehicle access

## Status
Accepted, implemented and verified locally.

## Decision
Authentication is RS256 with access/refresh token types. YAML permissions govern API actions; Postgres uses a restricted runtime role and fail-closed RLS with transaction-local tenant context. Precise locations and raw evidence require separate permissions. Audit rows are append-only for the runtime role, with privileged retention restricted to the owner function.

Erasure removes incident-derived data, pseudonymises the registry vehicle, removes identifying telemetry/quarantine/vehicle rollups, and clears Redis checkpoints/state/caches. Verification queries each store. A minimal erased-vehicle ledger prevents replay from recreating identifying data. Compliance audit and aggregate fleet statistics remain under configured retention. Kafka source records expire under topic retention; the erasure job does not rewrite immutable source logs.

## Consequences
Owner credentials and Redis checkpoint/privacy storage are protected deployment assets. Production requires shared signing keys, TLS, managed secrets, protected datastore access and policy alignment with Kafka retention. Real-store isolation, role/privacy, immutable-audit and erasure/replay tests document the local controls; production legal compliance and infrastructure failover are separate deployment validations.
