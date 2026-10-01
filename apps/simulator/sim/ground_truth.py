"""Hidden ground-truth writer.

Labels go straight to ``ground_truth_incident`` in Postgres, bypassing Kafka
entirely. That separation is the whole point: if the labels travelled the same path
as the telemetry, the detection chain could in principle see them and the accuracy
figure in ``docs/BENCHMARKS.md`` would be measuring the platform against itself.

Writes are idempotent on a content-derived id, so re-running a seeded scenario
updates its label rather than appending a second copy and inflating the denominator.

A JSONL fallback exists for running the simulator without a database.
"""

from __future__ import annotations

import contextlib
import json
import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from packages.common import logging as tf_logging
from sim.scenarios.registry import GroundTruth

LOGGER = tf_logging.get_logger(__name__)

INSERT_SQL = """
INSERT INTO ground_truth_incident (
    id, run_id, scenario_id, tenant_id, vehicle_id, start_ts, end_ts,
    expected_signals, expected_severity, should_create_incident, event_keys
) VALUES (
    %(id)s, %(run_id)s, %(scenario_id)s, %(tenant_id)s, %(vehicle_id)s,
    %(start_ts)s, %(end_ts)s, %(expected_signals)s, %(expected_severity)s,
    %(should_create_incident)s, %(event_keys)s
)
ON CONFLICT (id) DO UPDATE SET
    expected_signals = EXCLUDED.expected_signals,
    expected_severity = EXCLUDED.expected_severity,
    should_create_incident = EXCLUDED.should_create_incident,
    event_keys = EXCLUDED.event_keys,
    end_ts = EXCLUDED.end_ts
"""


class GroundTruthWriter:
    """Persists scenario labels for evaluation."""

    def __init__(
        self,
        run_id: str,
        tenant_id: str = "tenant_default",
        *,
        jsonl_path: Path | None = None,
        enabled: bool = True,
    ) -> None:
        self.run_id = run_id
        self.tenant_id = tenant_id
        self.jsonl_path = jsonl_path
        self.enabled = enabled
        self._connection: Any = None
        self._degraded = False
        self.written = 0

    # -- connection ------------------------------------------------------
    def _connect(self) -> Any:
        if self._connection is not None and not getattr(self._connection, "closed", 1):
            return self._connection
        try:
            import psycopg2

            from packages.common.dsn import psycopg2_kwargs

            # Owner connection: ground truth spans tenants and is written by a
            # trusted offline component, not through the tenant-scoped API.
            self._connection = psycopg2.connect(**psycopg2_kwargs(owner=True))
            self._connection.autocommit = True
            return self._connection
        except Exception as error:
            if not self._degraded:
                LOGGER.warning(
                    "ground-truth database unavailable, falling back to JSONL",
                    extra={"error": str(error)},
                )
                self._degraded = True
            return None

    # -- writes ----------------------------------------------------------
    def write(self, truth: GroundTruth) -> bool:
        """Persist one label. Returns True when it reached the database."""
        if not self.enabled or truth is None:
            return False

        row = truth.to_row(self.run_id, self.tenant_id)
        connection = self._connect()
        if connection is not None:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(INSERT_SQL, _adapt(row))
                self.written += 1
                return True
            except Exception as error:
                LOGGER.warning("ground-truth write failed", extra={"error": str(error)})
                self._connection = None

        self._write_jsonl(row)
        return False

    def write_many(self, truths: Iterable[GroundTruth]) -> int:
        """Persist several labels in one transaction."""
        rows = [truth.to_row(self.run_id, self.tenant_id) for truth in truths if truth is not None]
        if not rows or not self.enabled:
            return 0

        connection = self._connect()
        if connection is not None:
            try:
                with connection.cursor() as cursor:
                    cursor.executemany(INSERT_SQL, [_adapt(row) for row in rows])
                self.written += len(rows)
                return len(rows)
            except Exception as error:
                LOGGER.warning("ground-truth batch write failed", extra={"error": str(error)})
                self._connection = None

        for row in rows:
            self._write_jsonl(row)
        return 0

    def _write_jsonl(self, row: dict[str, Any]) -> None:
        """Append to a JSONL file when the database is unreachable."""
        if self.jsonl_path is None:
            return
        try:
            self.jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            with self.jsonl_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, default=str) + "\n")
        except OSError as error:
            LOGGER.warning("ground-truth jsonl write failed", extra={"error": str(error)})

    def close(self) -> None:
        if self._connection is not None:
            with contextlib.suppress(Exception):
                self._connection.close()
            self._connection = None

    def __enter__(self) -> GroundTruthWriter:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def _adapt(row: dict[str, Any]) -> dict[str, Any]:
    """Adapt list fields to JSON for the JSONB columns."""
    from psycopg2.extras import Json

    adapted = dict(row)
    adapted["expected_signals"] = Json(row.get("expected_signals") or [])
    adapted["event_keys"] = Json(row.get("event_keys") or [])
    return adapted


def clear_run(run_id: str) -> int:
    """Delete a run's labels. Used to reset a demo."""
    try:
        import psycopg2

        from packages.common.dsn import psycopg2_kwargs

        with psycopg2.connect(**psycopg2_kwargs(owner=True)) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM ground_truth_incident WHERE run_id = %s", (run_id,))
                return cursor.rowcount
    except Exception as error:
        LOGGER.warning("ground-truth clear failed", extra={"error": str(error)})
        return 0


def default_jsonl_path() -> Path:
    return Path(os.getenv("GROUND_TRUTH_PATH", "artifacts/ground-truth.jsonl"))
