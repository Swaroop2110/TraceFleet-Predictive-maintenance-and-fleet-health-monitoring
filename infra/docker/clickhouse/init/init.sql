-- ClickHouse schema for telemetry analytics.
--
-- ClickHouse holds the high-volume append-only stream; Postgres holds the
-- entities an operator mutates. See docs/adr/001-database-choice.md.
--
-- The TTL below must stay in step with `telemetry_warm_days` in
-- config/retention.yaml. It is written out here rather than templated because a
-- schema file that is generated at runtime cannot be reviewed in a diff.

CREATE DATABASE IF NOT EXISTS tracefleet;

USE tracefleet;

-- ---------------------------------------------------------------------------
-- Raw canonical events
-- ---------------------------------------------------------------------------
-- ReplacingMergeTree keyed on processed_at: a Kafka replay re-inserts the same
-- event_key, and the newer row wins on merge. That makes at-least-once delivery
-- safe without a dedup step in the query path. Queries that must not see a
-- pre-merge duplicate use FINAL.
--
-- ORDER BY starts with tenant_id so every tenant-scoped query prunes on the
-- first key column, then vehicle_id and ts because the dominant access pattern
-- is "one vehicle over a time range" for timeline reconstruction. See
-- docs/BENCHMARKS.md for the measurement behind this choice.
CREATE TABLE IF NOT EXISTS telemetry_events (
  tenant_id LowCardinality(String),
  fleet_id LowCardinality(String),
  vehicle_id String,
  event_key FixedString(64),
  seq UInt64,
  ts DateTime64(3, 'UTC'),
  ingested_at DateTime64(3, 'UTC'),
  processed_at DateTime64(3, 'UTC'),
  lat Float32,
  lon Float32,
  geohash String,
  speed_kmh Float32,
  acceleration_mps2 Float32,
  brake_force_pct Float32,
  heading_deg Float32,
  rpm UInt16,
  soc_pct Float32,
  odo_km Float64,
  dtc Array(LowCardinality(String)),
  evt LowCardinality(String),
  late UInt8,
  schema_version LowCardinality(String),
  lateness_ms Int32,
  -- Skip index on geohash so spatial filters do not scan whole partitions.
  INDEX idx_geohash geohash TYPE bloom_filter(0.01) GRANULARITY 4,
  INDEX idx_evt evt TYPE set(64) GRANULARITY 4
) ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMMDD(ts)
ORDER BY (tenant_id, vehicle_id, ts, event_key)
TTL toDateTime(ts) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- Per-minute fleet rollup
-- ---------------------------------------------------------------------------
-- Aggregating at insert time keeps the Analytics page off the raw table. A
-- 24-hour fleet average over billions of rows is a scan; over this table it is a
-- few thousand rows.
CREATE TABLE IF NOT EXISTS telemetry_1m_by_fleet (
  tenant_id LowCardinality(String),
  fleet_id LowCardinality(String),
  minute DateTime('UTC'),
  events AggregateFunction(count, UInt64),
  vehicles AggregateFunction(uniq, String),
  avg_speed AggregateFunction(avg, Float32),
  max_speed AggregateFunction(max, Float32),
  min_accel AggregateFunction(min, Float32),
  late_events AggregateFunction(sum, UInt8),
  distance_km AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(minute)
ORDER BY (tenant_id, fleet_id, minute)
TTL minute + INTERVAL 365 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_1m_by_fleet_mv
TO telemetry_1m_by_fleet
AS SELECT
  tenant_id,
  fleet_id,
  toStartOfMinute(ts) AS minute,
  countState() AS events,
  uniqState(vehicle_id) AS vehicles,
  avgState(speed_kmh) AS avg_speed,
  maxState(speed_kmh) AS max_speed,
  minState(acceleration_mps2) AS min_accel,
  sumState(late) AS late_events,
  -- Distance from speed over a one-second sampling interval. Approximate by
  -- design: integrating GPS positions at 1 Hz is noisier than integrating speed.
  sumState(toFloat64(speed_kmh) / 3600.0) AS distance_km
FROM telemetry_events
GROUP BY tenant_id, fleet_id, minute;

-- ---------------------------------------------------------------------------
-- Per-vehicle hourly rollup, for risk scoring
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS telemetry_1h_by_vehicle (
  tenant_id LowCardinality(String),
  fleet_id LowCardinality(String),
  vehicle_id String,
  hour DateTime('UTC'),
  events AggregateFunction(count, UInt64),
  avg_speed AggregateFunction(avg, Float32),
  max_speed AggregateFunction(max, Float32),
  min_accel AggregateFunction(min, Float32),
  max_brake AggregateFunction(max, Float32),
  distance_km AggregateFunction(sum, Float64),
  dtc_events AggregateFunction(sum, UInt8)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (tenant_id, vehicle_id, hour)
TTL hour + INTERVAL 365 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_1h_by_vehicle_mv
TO telemetry_1h_by_vehicle
AS SELECT
  tenant_id,
  fleet_id,
  vehicle_id,
  toStartOfHour(ts) AS hour,
  countState() AS events,
  avgState(speed_kmh) AS avg_speed,
  maxState(speed_kmh) AS max_speed,
  minState(acceleration_mps2) AS min_accel,
  maxState(brake_force_pct) AS max_brake,
  sumState(toFloat64(speed_kmh) / 3600.0) AS distance_km,
  sumState(toUInt8(length(dtc) > 0)) AS dtc_events
FROM telemetry_events
GROUP BY tenant_id, fleet_id, vehicle_id, hour;

-- ---------------------------------------------------------------------------
-- Ingest lag
-- ---------------------------------------------------------------------------
-- Measures event time against ingestion time, which is the number the <2 s
-- dashboard target is judged on. Kafka consumer lag is a different quantity and
-- is exported from the processor as a Prometheus gauge.
CREATE TABLE IF NOT EXISTS telemetry_ingest_lag (
  tenant_id LowCardinality(String),
  minute DateTime('UTC'),
  events AggregateFunction(count, UInt64),
  avg_lag_ms AggregateFunction(avg, Int32),
  max_lag_ms AggregateFunction(max, Int32),
  p95_lag_ms AggregateFunction(quantile(0.95), Int32),
  late_events AggregateFunction(sum, UInt8)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(minute)
ORDER BY (tenant_id, minute)
TTL minute + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_ingest_lag_mv
TO telemetry_ingest_lag
AS SELECT
  tenant_id,
  toStartOfMinute(processed_at) AS minute,
  countState() AS events,
  avgState(lateness_ms) AS avg_lag_ms,
  maxState(lateness_ms) AS max_lag_ms,
  quantileState(0.95)(lateness_ms) AS p95_lag_ms,
  sumState(late) AS late_events
FROM telemetry_events
GROUP BY tenant_id, minute;

-- ---------------------------------------------------------------------------
-- Incident daily rollup
-- ---------------------------------------------------------------------------
-- Written by the API when it serves analytics, from the Postgres record of
-- truth. Kept in ClickHouse so trend queries join against telemetry rollups
-- without a cross-database query.
CREATE TABLE IF NOT EXISTS incident_daily_rollup (
  tenant_id LowCardinality(String),
  fleet_id LowCardinality(String),
  day Date,
  severity LowCardinality(String),
  incident_type LowCardinality(String),
  incidents UInt64,
  avg_confidence Float32,
  updated_at DateTime('UTC')
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, fleet_id, day, severity, incident_type)
TTL day + INTERVAL 730 DAY;

-- ---------------------------------------------------------------------------
-- Quarantine
-- ---------------------------------------------------------------------------
-- Rejected payloads are retained briefly so a schema fault can be diagnosed from
-- the original bytes rather than from a log line about them.
CREATE TABLE IF NOT EXISTS telemetry_quarantine (
  received_at DateTime64(3, 'UTC'),
  reason_code LowCardinality(String),
  reason_detail String,
  vehicle_id String,
  source_topic LowCardinality(String),
  source_partition Int32,
  source_offset Int64,
  original_payload String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(received_at)
ORDER BY (reason_code, received_at)
TTL toDateTime(received_at) + INTERVAL 14 DAY;
