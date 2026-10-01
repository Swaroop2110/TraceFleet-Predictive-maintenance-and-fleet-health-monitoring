#!/usr/bin/env bash
set -euo pipefail

service="${1:-incident-processor}"
api_url="${TRACEFLEET_API_URL:-http://localhost:8000}"
echo "Stopping ${service} to exercise Kafka consumer-group recovery"
docker compose kill "${service}"
sleep 3
docker compose up -d "${service}"
for attempt in {1..20}; do
  if docker compose ps --status running "${service}" | grep -q "${service}"; then
    curl --fail --silent "${api_url}/api/v1/health" >/dev/null
    echo "${service} restarted and API health remained available"
    exit 0
  fi
  sleep 2
done
echo "${service} did not recover within 40 seconds" >&2
exit 1
