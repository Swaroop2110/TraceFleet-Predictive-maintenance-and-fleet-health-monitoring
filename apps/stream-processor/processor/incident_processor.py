"""Incident role: ``telemetry.canonical`` to ``incidents.events``.

The lifecycle this implements is the heart of the product:

``DETECTED`` — the trigger rules were satisfied. Published immediately, because a
CRITICAL alert has a five-second budget and cannot wait for reconstruction.

``RECONSTRUCTING`` — the incident is collecting the ``post_window_s`` of telemetry
that follows the trigger. Evidence phases are not final yet.

``OPEN`` — the post window closed (or ``reconstruction_timeout_s`` elapsed) and the
evidence set is finalised. This is the state an operator acts on.

A late event arriving for an already-OPEN incident, still inside the lateness
window, causes **re-reconstruction**: the evidence is merged, the incident is
republished as an update flagged ``late_evidence_added``, and the API invalidates
its cached timeline. Silently ignoring it would leave the stored timeline
disagreeing with stored telemetry.

Two suppression rules keep the stream honest. ``min_distinct_signals`` plus
``must_include_any`` mean an isolated harsh brake is not an incident, and
``cooldown_seconds`` stops one long event from being republished as a stream of
new incidents.
"""

from __future__ import annotations

import copy
import json
import os
from collections import OrderedDict
from dataclasses import asdict
from datetime import datetime
from typing import Any

from packages.common import config as cfg
from packages.common import kafka as kafka_helpers
from packages.common import logging as tf_logging
from packages.common import metrics, otel
from packages.common.timeutil import add_seconds, now, parse_ts, seconds_between
from packages.schemas.models import IncidentEnvelope, IncidentPayload, IncidentStatus
from processor.consumers import BackPressure, BatchConsumer, decode_json_batch
from processor.correlation import CandidateIncident, CorrelationPolicyRegistry
from processor.detectors import DetectorRegistry
from processor.incident_builder import build_incident
from processor.window import WindowStore

LOGGER = tf_logging.get_logger(__name__)


class IncidentProcessor:
    """Correlates detector signals into incidents and publishes lifecycle events."""

    def __init__(
        self,
        broker: str | None = None,
        *,
        producer: Any = None,
        redis_client: Any = None,
        connect: bool = True,
    ) -> None:
        """``connect=False`` builds the detection logic without Kafka or Redis,
        which is how the unit tests drive :meth:`process_event` directly."""
        self.broker = broker or kafka_helpers.broker()
        self.rules = cfg.detection_rules()
        self.correlation = cfg.correlation()
        self.severity_config = cfg.severity()
        self.factors_config = cfg.factors()

        self.producer = producer or (
            kafka_helpers.build_producer(**{"bootstrap.servers": self.broker}) if connect else _NullProducer()
        )
        self.consumer = (
            BatchConsumer(
                topics=[kafka_helpers.TELEMETRY_CANONICAL],
                group_id=kafka_helpers.GROUP_INCIDENT,
                producer=self.producer,
                consumer=kafka_helpers.build_consumer(
                    kafka_helpers.GROUP_INCIDENT, **{"bootstrap.servers": self.broker}
                ),
            )
            if connect
            else None
        )

        self.windows = WindowStore(
            self.rules.window_seconds,
            max_vehicles=self.correlation.max_active_vehicles,
        )
        #: Candidates under construction, LRU-ordered so the store stays bounded.
        self.candidates: OrderedDict[str, CandidateIncident] = OrderedDict()
        #: Last trigger time per vehicle, for the cooldown gate.
        self.last_trigger: OrderedDict[str, datetime] = OrderedDict()
        #: Incidents already finalised, kept briefly so a late event can reopen them.
        self.recent_incidents: OrderedDict[str, CandidateIncident] = OrderedDict()

        self.redis = redis_client if redis_client is not None else (_redis_client() if connect else None)
        self._restored: set[str] = set()
        self._last_maintenance = now()
        #: Every envelope published, in order. Read by tests and the ops endpoint.
        self.published: list[IncidentEnvelope] = []

    # -- loop ------------------------------------------------------------
    def run(self) -> None:
        if self.consumer is None:
            raise RuntimeError("IncidentProcessor was constructed with connect=False")
        LOGGER.info(
            "incident processor starting",
            extra={
                "broker": self.broker,
                "detectors": DetectorRegistry.available(),
                "window_seconds": self.rules.window_seconds,
                "trigger": self.rules.incident_trigger.model_dump(),
            },
        )
        self.consumer.run(self.handle_batch)

    def handle_batch(self, messages: list[Any]) -> None:
        batch = decode_json_batch(messages)
        if self.redis is not None:
            try:
                erased = self.redis.smembers("tf:erased:vehicles")
                batch = [(m, p) for m, p in batch if not p or p["vehicle_id"] not in erased]
            except Exception as error:
                raise BackPressure("erasure ledger unavailable") from error
        for _, payload in batch:
            if payload:
                self._restore(payload)
        vehicles = {payload["vehicle_id"] for _, payload in batch if payload}
        stores = (self.windows._windows, self.candidates, self.last_trigger, self.recent_incidents)

        def clone_window(window):
            if window is None:
                return None
            saved = copy.copy(window)
            saved.events = list(window.events)
            saved.active_dtcs = set(window.active_dtcs)
            return saved

        snapshot = [
            {key: (clone_window(store.get(key)) if index == 0 else copy.deepcopy(store.get(key))) for key in vehicles}
            for index, store in enumerate(stores)
        ]
        try:
            with metrics.processing_seconds.labels(role="incident").time():
                for message, payload in batch:
                    if payload is None:
                        raise ValueError(f"undecodable canonical event at offset {message.offset()}")
                    with otel.consumer_span("incident.process", headers=message.headers()):
                        self.process_event(payload)
                self.run_maintenance()
                if self.producer.flush(30):
                    raise BackPressure("incident messages unflushed")
                if self.redis is not None:
                    pipeline = self.redis.pipeline()
                    for payload in {p["vehicle_id"]: p for _, p in batch if p}.values():
                        self._checkpoint(payload, client=pipeline)
                    try:
                        pipeline.execute()
                    except Exception as error:
                        raise BackPressure("incident checkpoint unavailable") from error
        except Exception:
            for store, saved in zip(stores, snapshot, strict=False):
                for key, value in saved.items():
                    if value is None:
                        store.pop(key, None)
                    else:
                        store[key] = value
            raise

    def _checkpoint(self, event: dict[str, Any], client: Any = None) -> None:
        if self.redis is None:
            return
        vehicle = event["vehicle_id"]
        state = {
            "events": self.windows.get(vehicle).events,
            "candidate": asdict(self.candidates[vehicle]) if vehicle in self.candidates else None,
            "recent": asdict(self.recent_incidents[vehicle]) if vehicle in self.recent_incidents else None,
            "last_trigger": self.last_trigger.get(vehicle),
        }

        def encode(value: Any) -> Any:
            if isinstance(value, datetime):
                return value.isoformat()
            if isinstance(value, set):
                return sorted(value)
            return str(value)

        try:
            (client if client is not None else self.redis).set(
                f"tf:{event['tenant_id']}:veh:{vehicle}:checkpoint", json.dumps(state, default=encode), ex=86400
            )
        except Exception as error:
            raise BackPressure("incident checkpoint unavailable") from error

    def _restore(self, event: dict[str, Any]) -> None:
        vehicle = event["vehicle_id"]
        if vehicle in self._restored or self.redis is None:
            return
        try:
            raw = self.redis.get(f"tf:{event['tenant_id']}:veh:{vehicle}:checkpoint")
        except Exception as error:
            raise BackPressure("incident checkpoint unavailable") from error
        if raw:
            from processor.detectors.registry import Signal

            state = json.loads(raw)
            for item in state["events"]:
                self.windows.get(vehicle).add_event(item)
            for label, store in (("candidate", self.candidates), ("recent", self.recent_incidents)):
                data = state.get(label)
                if not data:
                    continue
                for field in ("start_ts", "last_signal_ts", "triggered_at", "reconstruct_after"):
                    if data.get(field):
                        data[field] = parse_ts(data[field])
                data["status"] = IncidentStatus(data["status"])
                for field in ("seen_event_keys", "missing_fields"):
                    data[field] = set(data[field])
                data["signals"] = [Signal(**dict(item, ts=parse_ts(item["ts"]))) for item in data["signals"]]
                store[vehicle] = CandidateIncident(**data)
            if state.get("last_trigger"):
                self.last_trigger[vehicle] = parse_ts(state["last_trigger"])
        self._restored.add(vehicle)
        if len(self._restored) > self.correlation.max_active_vehicles:
            self._restored.intersection_update(self.windows.keys())

    # -- per-event -------------------------------------------------------
    def process_event(self, event: dict[str, Any]) -> IncidentPayload | None:
        vehicle_id = event["vehicle_id"]
        event["_ts"] = parse_ts(event["ts"])

        window = self.windows.get(vehicle_id)
        out_of_order = window.add_event(event)

        # The post-incident window is defined in event time, so it can close on an
        # ordinary event that raises no signals. Checking this only when a new
        # signal arrives would leave a quiet vehicle's incident stuck in
        # RECONSTRUCTING until the wall-clock timeout.
        self._finalize_if_due(vehicle_id, event["_ts"])

        signals = DetectorRegistry.evaluate_all(window, event, self.rules.rules)
        if not signals:
            return None

        candidate = self._candidate_for(vehicle_id, event, signals)
        self._record_data_quality(candidate, event, out_of_order)

        accepted = candidate.add_signals(signals)
        if not accepted:
            # Every signal was already recorded, so this is a replay of an event
            # already folded in. Nothing changed; republishing would be noise.
            return None

        if candidate.triggered:
            return self._handle_post_trigger(candidate, event)
        return self._evaluate_trigger(candidate, event)

    def _candidate_for(
        self,
        vehicle_id: str,
        event: dict[str, Any],
        signals: list[Any],
    ) -> CandidateIncident:
        """Find the candidate these signals belong to, or start a new one."""
        candidate = self.candidates.get(vehicle_id)
        if candidate is not None:
            grouped = any(
                CorrelationPolicyRegistry.should_group(candidate, signal, self.correlation) for signal in signals
            )
            if grouped:
                self.candidates.move_to_end(vehicle_id)
                return candidate
            # Outside the correlation window: the previous cluster is done.
            self._retire(vehicle_id, candidate)

        reopened = self._reopen_for_late_evidence(vehicle_id, signals)
        if reopened is not None:
            return reopened

        candidate = CandidateIncident(
            vehicle_id=vehicle_id,
            tenant_id=event.get("tenant_id", "tenant_default"),
            fleet_id=event.get("fleet_id", "fleet_default"),
            start_ts=signals[0].ts,
            last_signal_ts=signals[-1].ts,
            trace_id=event.get("trace_id"),
        )
        self.candidates[vehicle_id] = candidate
        self._enforce_candidate_capacity()
        return candidate

    def _reopen_for_late_evidence(
        self,
        vehicle_id: str,
        signals: list[Any],
    ) -> CandidateIncident | None:
        """Re-open a finalised incident when late evidence belongs to it.

        Only inside the lateness window. Past that the platform has promised the
        timeline is stable, and a revision would surprise an operator who already
        acted on it; such evidence is still stored in ClickHouse and visible as
        late telemetry.
        """
        previous = self.recent_incidents.get(vehicle_id)
        if previous is None:
            return None
        within_lateness = any(
            previous.start_ts <= signal.ts <= add_seconds(previous.last_signal_ts, self.correlation.lateness_window_s)
            for signal in signals
        )
        if not within_lateness:
            return None

        previous.late_evidence_added = True
        self.candidates[vehicle_id] = previous
        self.recent_incidents.pop(vehicle_id, None)
        metrics.late_evidence_added_total.inc()
        LOGGER.info(
            "re-reconstructing incident after late evidence",
            extra={"incident_id": previous.incident_id, "vehicle_id": vehicle_id},
        )
        return previous

    def _record_data_quality(
        self,
        candidate: CandidateIncident,
        event: dict[str, Any],
        out_of_order: bool,
    ) -> None:
        flags = event.get("flags") or {}
        if flags.get("late"):
            candidate.late_events += 1
        if flags.get("duplicate"):
            candidate.duplicates_removed += 1
        if flags.get("schema_transformed"):
            candidate.schema_transformed += 1
        if out_of_order:
            candidate.out_of_order_events += 1
        candidate.missing_fields.update(flags.get("missing_fields") or [])

    # -- trigger ---------------------------------------------------------
    def _evaluate_trigger(
        self,
        candidate: CandidateIncident,
        event: dict[str, Any],
    ) -> IncidentPayload | None:
        trigger = self.rules.incident_trigger
        distinct = candidate.distinct_signal_names() - {"anomaly_score"}
        required = set(trigger.must_include_any)

        if len(distinct) < trigger.min_distinct_signals:
            metrics.incidents_suppressed_total.labels(reason="below_min_distinct_signals").inc()
            return None
        if required and not (required & distinct):
            metrics.incidents_suppressed_total.labels(reason="missing_required_signal").inc()
            return None

        if self._in_cooldown(candidate):
            metrics.incidents_suppressed_total.labels(reason="cooldown").inc()
            LOGGER.debug(
                "incident suppressed by cooldown",
                extra={"vehicle_id": candidate.vehicle_id, "cooldown_s": trigger.cooldown_seconds},
            )
            return None

        candidate.triggered = True
        candidate.triggered_at = candidate.last_signal_ts
        candidate.status = IncidentStatus.DETECTED
        candidate.reconstruct_after = add_seconds(candidate.triggered_at, self.correlation.post_window_s)
        self.last_trigger[candidate.vehicle_id] = candidate.triggered_at

        payload = self._publish(candidate, "incident.created", IncidentStatus.DETECTED)
        metrics.incidents_detected_total.labels(severity=payload.severity.value).inc()

        # Immediately advance to RECONSTRUCTING. DETECTED exists as a distinct
        # published state so an alert consumer can fire from it; the incident does
        # not linger there.
        candidate.status = IncidentStatus.RECONSTRUCTING
        self._publish(
            candidate,
            "incident.state_changed",
            IncidentStatus.RECONSTRUCTING,
            previous=IncidentStatus.DETECTED,
            reason="post-incident window opened",
        )
        return payload

    def _in_cooldown(self, candidate: CandidateIncident) -> bool:
        cooldown = self.rules.incident_trigger.cooldown_seconds
        if cooldown <= 0:
            return False
        last = self.last_trigger.get(candidate.vehicle_id)
        if last is None:
            return False
        return seconds_between(last, candidate.last_signal_ts) < cooldown

    def _handle_post_trigger(
        self,
        candidate: CandidateIncident,
        event: dict[str, Any],
    ) -> IncidentPayload | None:
        """Merge evidence into a triggered incident, finalising when due."""
        if candidate.status in (IncidentStatus.DETECTED, IncidentStatus.RECONSTRUCTING):
            if self._reconstruction_due(candidate, event["_ts"]):
                return self._finalize(candidate)
            return self._publish(
                candidate, "incident.updated", candidate.status, reason="evidence added during reconstruction"
            )

        # Already OPEN and receiving more evidence: this is the late path.
        return self._publish(candidate, "incident.updated", candidate.status, reason="late evidence merged")

    def _finalize_if_due(self, vehicle_id: str, reference: datetime) -> IncidentPayload | None:
        """Close reconstruction for this vehicle if its post window has elapsed."""
        candidate = self.candidates.get(vehicle_id)
        if candidate is None or not candidate.triggered:
            return None
        if candidate.status not in (IncidentStatus.DETECTED, IncidentStatus.RECONSTRUCTING):
            return None
        if not self._reconstruction_due(candidate, reference):
            return None
        return self._finalize(candidate)

    def _reconstruction_due(self, candidate: CandidateIncident, reference: datetime) -> bool:
        if candidate.reconstruct_after is None:
            return True
        if reference >= candidate.reconstruct_after:
            return True
        # Wall-clock backstop: a vehicle that stops reporting must not leave its
        # incident stuck in RECONSTRUCTING forever.
        if candidate.triggered_at is not None:
            elapsed = seconds_between(candidate.triggered_at, now())
            return elapsed >= self.correlation.reconstruction_timeout_s
        return False

    def _finalize(self, candidate: CandidateIncident) -> IncidentPayload:
        """Close reconstruction and move the incident to OPEN."""
        previous = candidate.status
        candidate.status = IncidentStatus.OPEN
        payload = self._publish(
            candidate,
            "incident.state_changed",
            IncidentStatus.OPEN,
            previous=previous,
            reason="reconstruction complete",
        )
        if candidate.triggered_at is not None:
            metrics.incident_reconstruction_seconds.observe(max(0.0, seconds_between(candidate.triggered_at, now())))
        self._retire(candidate.vehicle_id, candidate)
        return payload

    def _retire(self, vehicle_id: str, candidate: CandidateIncident) -> None:
        """Move a candidate out of the active set, keeping it briefly for late events."""
        self.candidates.pop(vehicle_id, None)
        if candidate.triggered:
            self.recent_incidents[vehicle_id] = candidate
            while len(self.recent_incidents) > self.correlation.max_active_incidents:
                self.recent_incidents.popitem(last=False)

    def _enforce_candidate_capacity(self) -> None:
        while len(self.candidates) > self.correlation.max_active_incidents:
            vehicle_id, _dropped = self.candidates.popitem(last=False)
            # An untriggered candidate is just an unfinished cluster; dropping it
            # loses nothing durable. A triggered one was already published.
            metrics.incidents_suppressed_total.labels(reason="candidate_capacity").inc()
            LOGGER.debug("candidate evicted by capacity guard", extra={"vehicle_id": vehicle_id})

    # -- publish ---------------------------------------------------------
    def _publish(
        self,
        candidate: CandidateIncident,
        event_type: str,
        status: IncidentStatus,
        *,
        previous: IncidentStatus | None = None,
        reason: str | None = None,
    ) -> IncidentPayload:
        candidate.revision += 1
        payload = build_incident(
            candidate,
            rules=self.rules,
            severity_config=self.severity_config,
            factors_config=self.factors_config,
            correlation=self.correlation,
            status=status,
        )
        candidate.severity = payload.severity.value

        envelope = IncidentEnvelope(
            event_type=event_type,  # type: ignore[arg-type]
            occurred_at=now(),
            incident=payload,
            previous_status=previous,
            reason=reason,
        )
        encoded = envelope.model_dump_json().encode("utf-8")
        self.published.append(envelope)
        if len(self.published) > 1000:
            del self.published[:-1000]

        with otel.producer_span(
            "incident.publish",
            **{"incident.id": payload.incident_id, "incident.severity": payload.severity.value},
        ):
            headers = otel.inject_headers([("event_type", event_type.encode("utf-8"))])
            self.producer.produce(
                kafka_helpers.INCIDENTS_EVENTS,
                key=payload.incident_id,
                value=encoded,
                headers=headers,
            )

            if payload.severity.value == "CRITICAL" and event_type == "incident.created":
                # Published on its own low-volume topic and flushed immediately.
                # The five-second alert budget cannot absorb a linger delay behind
                # a backlog of telemetry-shaped messages.
                self.producer.produce(
                    kafka_helpers.ALERTS_CRITICAL,
                    key=payload.vehicle_id,
                    value=encoded,
                    headers=headers,
                )
                self.producer.flush(5)
                metrics.event_to_alert_seconds.observe(max(0.0, seconds_between(payload.start_ts, now())))

        if previous is not None:
            metrics.incident_state_transitions_total.labels(from_state=previous.value, to_state=status.value).inc()

        self._push_live(payload, event_type)
        self.producer.poll(0)
        return payload

    def _push_live(self, payload: IncidentPayload, event_type: str) -> None:
        """Publish to Redis pub/sub for the websocket layer.

        Separate from the Kafka path on purpose: the dashboard wants latency, the
        persister wants durability, and coupling them would make a slow browser
        client able to delay persistence.
        """
        if self.redis is None:
            return
        try:
            message = payload.model_dump_json()
            pipeline = self.redis.pipeline()
            pipeline.publish(f"tf:{payload.tenant_id}:incidents", message)
            key = f"tf:{payload.tenant_id}:incidents:active"
            if payload.status in (IncidentStatus.DETECTED, IncidentStatus.RECONSTRUCTING, IncidentStatus.OPEN):
                pipeline.zadd(key, {payload.incident_id: payload.start_ts.timestamp()})
            else:
                pipeline.zrem(key, payload.incident_id)
            pipeline.hset(
                f"tf:{payload.tenant_id}:incident:{payload.incident_id}:summary",
                mapping={
                    "severity": payload.severity.value,
                    "status": payload.status.value,
                    "vehicle_id": payload.vehicle_id,
                    "incident_type": payload.incident_type,
                    "confidence": str(payload.confidence),
                    "revision": str(payload.metadata.get("revision", 0)),
                },
            )
            pipeline.execute()
            metrics.event_to_dashboard_seconds.observe(max(0.0, seconds_between(payload.start_ts, now())))
        except Exception as error:
            LOGGER.warning("live push failed", extra={"error": str(error), "event_type": event_type})

    # -- maintenance -----------------------------------------------------
    def run_maintenance(self, interval_s: float = 5.0) -> int:
        """Finalise due reconstructions and evict idle state.

        Driven from the batch loop rather than a timer thread so all mutation of
        the candidate map happens on one thread and needs no locking.
        """
        reference = now()
        if seconds_between(self._last_maintenance, reference) < interval_s:
            return 0
        self._last_maintenance = reference

        if self.redis is not None:
            try:
                erased = self.redis.smembers("tf:erased:vehicles")
            except Exception as error:
                raise BackPressure("erasure ledger unavailable") from error
            for vehicle_id in erased:
                for store in (self.candidates, self.recent_incidents, self.last_trigger, self.windows._windows):
                    store.pop(vehicle_id, None)
                self._restored.discard(vehicle_id)
            self.published = [envelope for envelope in self.published if envelope.incident.vehicle_id not in erased]
        finalised = 0
        for vehicle_id in list(self.candidates):
            candidate = self.candidates.get(vehicle_id)
            if candidate is None or not candidate.triggered:
                continue
            if candidate.status not in (IncidentStatus.DETECTED, IncidentStatus.RECONSTRUCTING):
                continue
            if candidate.triggered_at is None:
                continue
            if seconds_between(candidate.triggered_at, reference) >= self.correlation.reconstruction_timeout_s:
                LOGGER.info(
                    "finalising incident on reconstruction timeout",
                    extra={"incident_id": candidate.incident_id, "vehicle_id": vehicle_id},
                )
                self._finalize(candidate)
                finalised += 1

        self.windows.evict_inactive(reference)
        while len(self.last_trigger) > self.correlation.max_active_vehicles:
            self.last_trigger.popitem(last=False)
        return finalised


class _NullProducer:
    """Stand-in producer for ``connect=False``. Records nothing, accepts everything."""

    def produce(self, *_: Any, **__: Any) -> None:
        pass

    def poll(self, *_: Any, **__: Any) -> int:
        return 0

    def flush(self, *_: Any, **__: Any) -> int:
        return 0


def _redis_client() -> Any:
    try:
        import redis

        return redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
    except Exception as error:  # pragma: no cover
        LOGGER.warning("redis unavailable at startup", extra={"error": str(error)})
        return None
