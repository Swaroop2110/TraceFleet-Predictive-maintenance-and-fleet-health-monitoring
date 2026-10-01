# Measured benchmarks

Measured on the local Docker environment. Target values are not claimed as achieved unless the measurements support them.

```json
{
  "profile_target_events_per_s": 2000.0,
  "measured_events_per_s": 2000.0,
  "percent_of_target": 100.0,
  "total_events": 122000,
  "errors": 0,
  "elapsed_seconds": 61.0,
  "workers": 1,
  "vehicles": 2000,
  "event_padding_bytes": 0,
  "per_worker": [
    {
      "worker_id": 0,
      "events": 122000,
      "errors": 0,
      "elapsed_s": 61.0,
      "achieved_rate": 2000.1,
      "target_rate": 2000.0,
      "percent_of_target": 100.0
    }
  ],
  "api": {
    "samples": 116,
    "p95_ms": 30.198833999747876,
    "p99_ms": 61.17895799980033,
    "scope": "authenticated incident list sampled during ingestion",
    "errors": []
  },
  "consumer_lag": {
    "samples": 116,
    "maximum": 1021.0,
    "last": 693.0,
    "source": "Prometheus-backed Operations API, scrape interval applies",
    "after_drain": 0.0,
    "drain_seconds": 13.34
  }
}
```

## Additional verified gates

- 100,000 vehicles seeded in 3.63 seconds.
- Required hidden-label scenario matrix: precision, recall, accuracy, severity, signal and timeline scores all 100% on the eight configured scenarios.
- Connected-browser dashboard and critical-alert measurements are recorded in `artifacts/browser-latency.json`.
- Consumer recovery: seven unique records and one CRITICAL incident reconciled after abrupt interruption; measured recovery includes Kafka's group rebalance interval. Broker restart is measured separately.
- Five-minute soak: 26 health/lag/memory samples, dependency health passed, final total consumer lag zero. This is a development soak, not a production endurance claim.
- Authenticated k6 load: 311 successful iterations, zero failed HTTP requests, p95 43.46 ms; paced within the shared per-user rate quota.
- Complete Kafka trace spans all five expected services in `artifacts/trace-verification.json`.

The stack used a Docker Desktop memory limit of approximately 7.75 GiB. These are local measurements with the dev profile and no padding; no 100,000-events/second production claim is made. Prometheus scrape intervals affect reported lag/drain timing.
