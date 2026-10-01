# ADR 001: Postgres and ClickHouse

## Status
Accepted for the local demo architecture; production topology remains open.

## Context
TraceFleet has two different data shapes: mutable operational records (incidents, lifecycle, audit) and high-volume time-series telemetry. One database would simplify operations but would not serve both access patterns equally well.

## Decision
- PostgreSQL is the system of record for incidents, evidence, state history, and AI runs.
- ClickHouse serves historical telemetry and aggregate analytics.
- Kafka decouples event ingestion from validation, incident detection, and persistence.
- Redis holds hot state/pub-sub, replay checkpoints and an erasure ledger. Checkpoints and privacy markers require persistence and protected eviction policies. Postgres remains authoritative for incidents.

## CAP and PACELC considerations
The local Compose stack is single-node and uses replication factor one, so it does not provide broker-failure tolerance. A production Kafka deployment should use replicated partitions, `acks=all`, and a suitable minimum in-sync replica count.

Postgres transactions favor consistency for incident lifecycle changes. During a Postgres outage, incident writes should fail/retry rather than acknowledge success without persistence. ClickHouse analytics are fed asynchronously from Kafka and can lag behind the operational record, trading immediate consistency for ingestion throughput and query latency. Redis state may be rebuilt from telemetry and should not override Postgres incident truth.

In PACELC terms, under a partition the operational database favors consistency over availability for state-changing operations; for telemetry and analytics, the design favors continued ingestion and lower-latency reads with eventual convergence. When there is no partition, the design favors low-latency hot-state and analytics reads while accepting asynchronous propagation. This is a design target, not a guarantee demonstrated by the single-node demo.

## Consequences
- API code must keep operational queries tenant-scoped; PostgreSQL RLS is enabled for the restricted API role and checked by integration tests.
- Consumers must be idempotent because Kafka replay can repeat writes.
- Analytics may lag incident state and should display freshness where relevant.
- Backups, replication, restore drills, and measured lag/recovery objectives remain deployment work.
