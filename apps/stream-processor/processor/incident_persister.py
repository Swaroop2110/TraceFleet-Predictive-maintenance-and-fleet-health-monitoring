"""Persister role: ``incidents.events`` to PostgreSQL.

Idempotency is the whole job. The topic is at-least-once, the processor
republishes an incident on every revision, and a consumer-group rebalance can
redeliver. So every write is an upsert keyed on ``idempotency_key`` — the
content-derived key from the PRD, not the incident id — and the dependent
evidence and factor rows are replaced rather than appended.

State transitions are recorded in ``incident_state_history`` and guarded: an
operator's ACKNOWLEDGED must not be reverted to OPEN by a replayed message. The
processor owns the detection lifecycle; the API owns the human lifecycle, and the
persister refuses to let one overwrite the other.

Schema is owned by Alembic. This role does not create tables.
"""

from __future__ import annotations

import time
from typing import Any

from packages.common import kafka as kafka_helpers
from packages.common import logging as tf_logging
from packages.common import metrics, otel
from packages.schemas.models import IncidentEnvelope, IncidentPayload, IncidentStatus
from processor.consumers import BackPressure, BatchConsumer, decode_json_batch

LOGGER = tf_logging.get_logger(__name__)

#: States the detection pipeline owns. Anything else was set by a human through
#: the API and must not be overwritten by a replayed detection message.
PROCESSOR_OWNED_STATES = {
    IncidentStatus.DETECTED.value,
    IncidentStatus.RECONSTRUCTING.value,
    IncidentStatus.OPEN.value,
}


class IncidentPersister:
    """Writes incident envelopes to Postgres idempotently."""

    def __init__(self, broker: str | None = None) -> None:
        self.broker = broker or kafka_helpers.broker()
        self.consumer = BatchConsumer(
            topics=[kafka_helpers.INCIDENTS_EVENTS],
            group_id=kafka_helpers.GROUP_PERSISTER,
            consumer=kafka_helpers.build_consumer(kafka_helpers.GROUP_PERSISTER, **{"bootstrap.servers": self.broker}),
        )
        self.conn: Any = None
        self.connect(required=True)

    # -- connection ------------------------------------------------------
    def connect(self, required: bool = False, attempts: int = 10) -> None:
        import psycopg2

        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                self.conn = psycopg2.connect(**_connection_kwargs())
                self.conn.autocommit = False
                LOGGER.info("connected to incident store")
                return
            except Exception as error:
                last_error = error
                delay = min(10.0, 0.5 * attempt)
                LOGGER.warning(
                    "postgres unavailable",
                    extra={"attempt": attempt, "of": attempts, "retry_in_s": delay, "error": str(error)},
                )
                time.sleep(delay)
        if required:
            raise RuntimeError(f"unable to connect to Postgres: {last_error}")

    def _ensure_connection(self) -> None:
        if self.conn is None or self.conn.closed:
            self.connect()
        if self.conn is None or self.conn.closed:
            raise BackPressure("postgres connection unavailable")

    # -- loop ------------------------------------------------------------
    def run(self) -> None:
        LOGGER.info("incident persister starting", extra={"broker": self.broker})
        self.consumer.run(self.handle_batch)

    def handle_batch(self, messages: list[Any]) -> None:
        """Persist a batch in one transaction.

        One transaction per batch, not per message: a partial batch that commits
        offsets would leave incidents missing, and rolling the whole batch back is
        safe precisely because every write is an upsert.
        """
        self._ensure_connection()
        with metrics.processing_seconds.labels(role="persister").time():
            try:
                for message, payload in decode_json_batch(messages):
                    if payload is None:
                        raise ValueError(f"undecodable incident envelope at offset {message.offset()}")
                    with otel.consumer_span("persister.write", headers=message.headers()):
                        self.persist(payload)
                self.conn.commit()
            except BackPressure:
                self._rollback()
                raise
            except Exception:
                self._rollback()
                raise

    def _rollback(self) -> None:
        try:
            if self.conn is not None and not self.conn.closed:
                self.conn.rollback()
        except Exception:  # pragma: no cover
            pass

    # -- writes ----------------------------------------------------------
    def persist(self, raw: dict[str, Any]) -> str:
        """Upsert one incident envelope. Returns the incident id."""
        envelope = _parse(raw)
        incident = envelope.incident
        with metrics.postgres_operation_seconds.labels(operation="persist_incident").time():  # noqa: SIM117
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "SELECT metadata_json->>'erased_at' FROM vehicle WHERE tenant_id=%s AND vehicle_id=%s",
                    (incident.tenant_id, incident.vehicle_id),
                )
                subject = cursor.fetchone()
                if subject and subject[0]:
                    return incident.incident_id
                previous_status = self._current_status(cursor, incident.idempotency_key)
                target_status = self._resolve_status(previous_status, incident.status.value)

                cursor.execute(
                    """
                    INSERT INTO incident (
                        incident_id, tenant_id, fleet_id, vehicle_id, incident_type, severity, status,
                        start_ts, end_ts, detected_at, idempotency_key, confidence, metadata_json
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (idempotency_key) DO UPDATE SET
                        severity      = EXCLUDED.severity,
                        status        = %s,
                        -- start_ts only ever moves earlier: a late event can
                        -- reveal the incident began before we first saw it.
                        start_ts      = LEAST(incident.start_ts, EXCLUDED.start_ts),
                        end_ts        = GREATEST(
                                            COALESCE(incident.end_ts, EXCLUDED.end_ts),
                                            COALESCE(EXCLUDED.end_ts, incident.end_ts)
                                        ),
                        confidence    = EXCLUDED.confidence,
                        metadata_json = EXCLUDED.metadata_json
                    RETURNING incident_id
                    """,
                    (
                        incident.incident_id,
                        incident.tenant_id,
                        incident.fleet_id,
                        incident.vehicle_id,
                        incident.incident_type,
                        incident.severity.value,
                        target_status,
                        incident.start_ts,
                        incident.end_ts,
                        incident.detected_at,
                        incident.idempotency_key,
                        incident.confidence,
                        _json(incident.metadata),
                        target_status,
                    ),
                )
                stored_id = cursor.fetchone()[0]

                self._replace_evidence(cursor, stored_id, incident)
                self._replace_factors(cursor, stored_id, incident)
                if previous_status != target_status:
                    self._record_transition(cursor, stored_id, previous_status, target_status, envelope)

        LOGGER.debug(
            "incident persisted",
            extra={
                "incident_id": stored_id,
                "event_type": envelope.event_type,
                "status": target_status,
                "severity": incident.severity.value,
            },
        )
        return stored_id

    def _current_status(self, cursor: Any, idempotency_key: str) -> str | None:
        cursor.execute("SELECT status FROM incident WHERE idempotency_key = %s", (idempotency_key,))
        row = cursor.fetchone()
        return row[0] if row else None

    @staticmethod
    def _resolve_status(previous: str | None, incoming: str) -> str:
        """Decide the stored status, protecting human decisions.

        Once an operator has acknowledged, closed, or invalidated an incident, a
        replayed detection message must not drag it back to OPEN. Detection-owned
        states are free to advance among themselves.
        """
        if previous is None:
            return incoming
        if previous not in PROCESSOR_OWNED_STATES:
            return previous
        return incoming

    def _replace_evidence(self, cursor: Any, incident_id: str, incident: IncidentPayload) -> None:
        """Upsert evidence rows and delete any that no longer apply.

        Evidence ids are deterministic, so an upsert is stable across replays. The
        trailing delete handles the case where re-reconstruction dropped an item.
        """
        if not incident.evidence:
            cursor.execute("DELETE FROM incident_evidence WHERE incident_id = %s", (incident_id,))
            return
        for item in incident.evidence:
            cursor.execute(
                """
                INSERT INTO incident_evidence (
                    evidence_id, incident_id, event_key, event_ts, signal, value, threshold, weight, phase, flags
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (evidence_id) DO UPDATE SET
                    value = EXCLUDED.value, threshold = EXCLUDED.threshold,
                    weight = EXCLUDED.weight, phase = EXCLUDED.phase, flags = EXCLUDED.flags
                """,
                (
                    item.evidence_id,
                    incident_id,
                    item.event_key,
                    item.event_ts,
                    item.signal,
                    item.value,
                    item.threshold,
                    item.weight,
                    item.phase,
                    _json(item.flags),
                ),
            )
        cursor.execute(
            "DELETE FROM incident_evidence WHERE incident_id = %s AND NOT (evidence_id = ANY(%s))",
            (incident_id, [item.evidence_id for item in incident.evidence]),
        )

    def _replace_factors(self, cursor: Any, incident_id: str, incident: IncidentPayload) -> None:
        from packages.common.ids import factor_id as build_factor_id

        if not incident.factors:
            cursor.execute("DELETE FROM incident_factor WHERE incident_id = %s", (incident_id,))
            return
        keep: list[str] = []
        for factor in incident.factors:
            identifier = build_factor_id(incident_id, factor.factor_type)
            keep.append(identifier)
            cursor.execute(
                """
                INSERT INTO incident_factor (
                    factor_id, incident_id, factor_type, score, confidence, evidence_ids, components
                ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (factor_id) DO UPDATE SET
                    score = EXCLUDED.score, confidence = EXCLUDED.confidence,
                    evidence_ids = EXCLUDED.evidence_ids, components = EXCLUDED.components
                """,
                (
                    identifier,
                    incident_id,
                    factor.factor_type,
                    factor.score,
                    factor.confidence,
                    _json(factor.evidence_ids),
                    _json(factor.components),
                ),
            )
        cursor.execute(
            "DELETE FROM incident_factor WHERE incident_id = %s AND NOT (factor_id = ANY(%s))",
            (incident_id, keep),
        )

    def _record_transition(
        self,
        cursor: Any,
        incident_id: str,
        previous: str | None,
        target: str,
        envelope: IncidentEnvelope,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO incident_state_history (incident_id, old_status, new_status, changed_at, changed_by, reason)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (incident_id, new_status, changed_at) DO NOTHING
            """,
            (incident_id, previous, target, envelope.occurred_at, envelope.actor, envelope.reason),
        )
        metrics.incident_state_transitions_total.labels(from_state=previous or "NEW", to_state=target).inc()


def _parse(raw: dict[str, Any]) -> IncidentEnvelope:
    """Accept an envelope, or a bare payload from an older producer."""
    if "incident" in raw and "event_type" in raw:
        return IncidentEnvelope.model_validate(raw)
    from packages.common.timeutil import now

    payload = IncidentPayload.model_validate(raw)
    return IncidentEnvelope(event_type="incident.updated", occurred_at=now(), incident=payload)


def _json(value: Any) -> Any:
    from psycopg2.extras import Json

    return Json(value)


def _connection_kwargs() -> dict[str, Any]:
    """Connect as the owner.

    The persister writes incidents for every tenant in one batch, so it cannot
    operate under a single-tenant RLS context. It is an internal pipeline
    component with no request surface; the API is the role that is constrained.
    """
    from packages.common.dsn import psycopg2_kwargs

    return psycopg2_kwargs(owner=True)
