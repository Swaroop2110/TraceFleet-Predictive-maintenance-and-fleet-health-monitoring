"""TraceFleet stream-processing service.

One codebase, four roles, selected with ``--role`` at startup:

* ``validator``  — ``telemetry.raw`` to ``telemetry.canonical``, plus quarantine
* ``incident``   — ``telemetry.canonical`` to ``incidents.events``
* ``sink``       — ``telemetry.canonical`` to ClickHouse
* ``persister``  — ``incidents.events`` to PostgreSQL

Each role is a separate Kafka consumer group so they scale and fail
independently: a ClickHouse outage stalls the sink without blocking detection.
"""
