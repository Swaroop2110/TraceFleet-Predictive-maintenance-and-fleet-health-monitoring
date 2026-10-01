#!/usr/bin/env bash
# Create the TraceFleet topics with partition counts from the active scale profile.
#
# Partition counts come from config/profiles/<PROFILE>.yaml rather than being fixed,
# because partition count is the ceiling on consumer parallelism and the `full`
# profile needs 48 on the hot path where `dev` needs 6. Increasing partitions later
# is possible but redistributes keys, so it is set at creation.
#
# Idempotent: existing topics are left alone, and the script only ever raises a
# partition count (Kafka cannot reduce one).
set -euo pipefail

BROKER="${KAFKA_BROKER:-kafka:9092}"
PROFILE="${PROFILE:-dev}"
PROFILE_FILE="/config/profiles/${PROFILE}.yaml"

echo "kafka-init: broker=${BROKER} profile=${PROFILE}"

# Wait for the broker. The healthcheck already gates this, but kafka-init also runs
# standalone from `make topics`.
for attempt in $(seq 1 60); do
  if kafka-broker-api-versions --bootstrap-server "${BROKER}" >/dev/null 2>&1; then
    echo "kafka-init: broker ready after ${attempt}s"
    break
  fi
  if [ "${attempt}" -eq 60 ]; then
    echo "kafka-init: broker did not become ready" >&2
    exit 1
  fi
  sleep 1
done

# Read one `key: value` pair from the profile's kafka_partitions block. Done with
# grep/awk rather than a YAML parser because this runs in the Kafka image, which has
# no Python.
partitions_for() {
  local topic="$1" fallback="$2"
  if [ -f "${PROFILE_FILE}" ]; then
    local value
    value=$(awk -v t="${topic}:" '
      /^kafka_partitions:/ { inblock=1; next }
      inblock && /^[^[:space:]]/ { inblock=0 }
      inblock && $1 == t { print $2; exit }
    ' "${PROFILE_FILE}" | tr -d '[:space:]')
    if [ -n "${value}" ]; then
      echo "${value}"
      return
    fi
  fi
  echo "${fallback}"
}

# topic|fallback partitions|retention ms|extra config
#
# Retention differs by purpose. Telemetry is replayable for a day, which is the
# buffer that makes back-pressure safe. Incidents are kept a week because they are
# the record an operator acts on. Quarantine is kept two weeks so a schema fault can
# be diagnosed from the original bytes.
TOPICS=(
  "telemetry.raw|6|86400000|compression.type=producer"
  "telemetry.canonical|6|86400000|compression.type=producer"
  "telemetry.quarantine|3|1209600000|"
  "incidents.events|3|604800000|"
  "alerts.critical|3|604800000|"
  "telemetry.raw.dlq|1|2592000000|"
  "telemetry.canonical.dlq|1|2592000000|"
  "incidents.events.dlq|1|2592000000|"
)

existing=$(kafka-topics --bootstrap-server "${BROKER}" --list 2>/dev/null || true)

for entry in "${TOPICS[@]}"; do
  IFS='|' read -r topic fallback retention extra <<< "${entry}"
  desired=$(partitions_for "${topic}" "${fallback}")

  if echo "${existing}" | grep -qx "${topic}"; then
    current=$(kafka-topics --bootstrap-server "${BROKER}" --describe --topic "${topic}" 2>/dev/null \
              | awk '/PartitionCount/ {for (i=1;i<=NF;i++) if ($i=="PartitionCount:") print $(i+1)}' | head -1)
    current=${current:-0}
    if [ "${desired}" -gt "${current}" ] 2>/dev/null; then
      echo "kafka-init: ${topic} ${current} -> ${desired} partitions"
      kafka-topics --bootstrap-server "${BROKER}" --alter --topic "${topic}" --partitions "${desired}" || true
    else
      echo "kafka-init: ${topic} exists with ${current} partitions, leaving it"
    fi
    continue
  fi

  echo "kafka-init: creating ${topic} (${desired} partitions, retention ${retention}ms)"
  args=(--bootstrap-server "${BROKER}" --create --topic "${topic}"
        --partitions "${desired}" --replication-factor 1
        --config "retention.ms=${retention}"
        --config "min.insync.replicas=1")
  if [ -n "${extra}" ]; then
    args+=(--config "${extra}")
  fi
  kafka-topics "${args[@]}"
done

echo "kafka-init: topics ready"
kafka-topics --bootstrap-server "${BROKER}" --list
