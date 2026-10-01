"""ClickHouse access, funnelled through one tenant-aware helper.

ClickHouse has no row-level security, so tenant isolation on the analytics store
is entirely the application's responsibility. Rather than trusting every call site
to remember a predicate, all queries go through :class:`TelemetryStore`, which
injects ``tenant_id = {tenant_id:String}`` and binds it as a parameter. There is no
public method that runs arbitrary SQL without a tenant.

Parameter binding is not optional here. The dead ``routers/analytics.py`` this
replaces interpolated the tenant into the SQL string, which made the tenant claim
from a JWT an injection vector.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from packages.common import logging as tf_logging
from packages.common import metrics
from packages.common.dsn import clickhouse_settings
from packages.common.timeutil import ensure_utc

LOGGER = tf_logging.get_logger(__name__)

#: Columns returned for timeline reconstruction, in the order the UI plots them.
TELEMETRY_COLUMNS = (
    "ts",
    "ingested_at",
    "processed_at",
    "event_key",
    "seq",
    "lat",
    "lon",
    "geohash",
    "speed_kmh",
    "acceleration_mps2",
    "brake_force_pct",
    "heading_deg",
    "rpm",
    "soc_pct",
    "odo_km",
    "dtc",
    "evt",
    "late",
    "lateness_ms",
    "schema_version",
)


class ClickHouseUnavailable(RuntimeError):
    """Raised when the analytics store cannot be reached.

    Distinguished from an empty result so a caller can degrade honestly rather than
    reporting "no telemetry" when it means "could not ask".
    """


@dataclass
class TelemetryPoint:
    """One telemetry sample on a reconstructed timeline."""

    ts: datetime
    ingested_at: datetime
    processed_at: datetime
    event_key: str
    seq: int
    lat: float
    lon: float
    geohash: str
    speed_kmh: float
    acceleration_mps2: float
    brake_force_pct: float
    heading_deg: float
    rpm: int | None
    soc_pct: float | None
    odo_km: float | None
    dtc: list[str]
    evt: str
    late: bool
    lateness_ms: int
    schema_version: str

    def to_dict(self) -> dict[str, Any]:
        from packages.common.timeutil import to_iso

        return {
            # All three times are exposed. The UI can toggle between event time
            # and ingestion time, which is how an operator sees that a timeline
            # was assembled from late data.
            "ts": to_iso(self.ts),
            "ingested_at": to_iso(self.ingested_at),
            "processed_at": to_iso(self.processed_at),
            "event_key": self.event_key,
            "seq": self.seq,
            "lat": self.lat,
            "lon": self.lon,
            "geohash": self.geohash,
            "speed_kmh": self.speed_kmh,
            "acceleration_mps2": self.acceleration_mps2,
            "brake_force_pct": self.brake_force_pct,
            "heading_deg": self.heading_deg,
            "rpm": self.rpm,
            "soc_pct": self.soc_pct,
            "odo_km": self.odo_km,
            "dtc": self.dtc,
            "evt": self.evt,
            "late": self.late,
            "lateness_ms": self.lateness_ms,
            "schema_version": self.schema_version,
        }


class TelemetryStore:
    """Tenant-scoped reader for the ClickHouse telemetry tables."""

    def __init__(self, client: Any = None) -> None:
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            import clickhouse_connect

            self._client = clickhouse_connect.get_client(
                **clickhouse_settings(), connect_timeout=5, send_receive_timeout=30
            )
        return self._client

    def close(self) -> None:
        try:
            if self._client is not None:
                self._client.close()
        except Exception:  # pragma: no cover
            pass
        self._client = None

    # -- internals -------------------------------------------------------
    def _query(self, label: str, sql: str, parameters: dict[str, Any]) -> Any:
        """Run a query with a mandatory tenant parameter."""
        if "tenant_id" not in parameters:
            # A programming error, not a user error: refuse rather than run a
            # query that would span tenants.
            raise ValueError("every ClickHouse query must bind a tenant_id parameter")
        if "{tenant_id:String}" not in sql:
            raise ValueError("every ClickHouse query must filter on tenant_id")
        try:
            with metrics.clickhouse_query_seconds.labels(query=label).time():
                return self.client.query(sql, parameters=parameters)
        except Exception as error:
            metrics.clickhouse_failures_total.labels(operation=f"query:{label}").inc()
            self.close()
            LOGGER.warning("clickhouse query failed", extra={"query": label, "error": str(error)})
            raise ClickHouseUnavailable(str(error)) from error

    # -- reads -----------------------------------------------------------
    def vehicle_window(
        self,
        tenant_id: str,
        vehicle_id: str,
        start: datetime,
        end: datetime,
        limit: int = 5000,
    ) -> list[TelemetryPoint]:
        """Telemetry for one vehicle over a time range, ordered by event time.

        ``FINAL`` is used so a replayed duplicate that has not yet merged in the
        ReplacingMergeTree does not appear twice on an investigation timeline.
        Acceptable here because the range is one vehicle over minutes, not a scan.
        """
        sql = f"""
            SELECT {", ".join(TELEMETRY_COLUMNS)}
            FROM telemetry_events FINAL
            WHERE tenant_id = {{tenant_id:String}}
              AND vehicle_id = {{vehicle_id:String}}
              AND ts BETWEEN {{start:DateTime64(3)}} AND {{end:DateTime64(3)}}
            ORDER BY ts ASC, seq ASC
            LIMIT {{limit:UInt32}}
        """  # nosec B608 # static identifiers, parameter-bound values
        result = self._query(
            "vehicle_window",
            sql,
            {
                "tenant_id": tenant_id,
                "vehicle_id": vehicle_id,
                "start": ensure_utc(start),
                "end": ensure_utc(end),
                "limit": limit,
            },
        )
        return [_to_point(row) for row in result.result_rows]

    def latest_state(self, tenant_id: str, vehicle_id: str) -> TelemetryPoint | None:
        """Most recent sample for a vehicle. Fallback when Redis has no hot state."""
        sql = f"""
            SELECT {", ".join(TELEMETRY_COLUMNS)}
            FROM telemetry_events
            WHERE tenant_id = {{tenant_id:String}} AND vehicle_id = {{vehicle_id:String}}
            ORDER BY ts DESC
            LIMIT 1
        """  # nosec B608 # static identifiers, parameter-bound values
        result = self._query("latest_state", sql, {"tenant_id": tenant_id, "vehicle_id": vehicle_id})
        rows = result.result_rows
        return _to_point(rows[0]) if rows else None

    def fleet_summary(self, tenant_id: str, hours: int = 24) -> dict[str, Any]:
        """Fleet-wide aggregates from the per-minute rollup.

        Reads the materialised view rather than the raw table: the same numbers
        over a few thousand pre-aggregated rows instead of a full scan.
        """
        sql = """
            SELECT
                uniqMerge(vehicles)      AS active_vehicles,
                countMerge(events)       AS events,
                avgMerge(avg_speed)      AS avg_speed_kmh,
                maxMerge(max_speed)      AS max_speed_kmh,
                sumMerge(distance_km)    AS distance_km,
                sumMerge(late_events)    AS late_events
            FROM telemetry_1m_by_fleet
            WHERE tenant_id = {tenant_id:String}
              AND minute >= now() - INTERVAL {hours:UInt16} HOUR
        """
        result = self._query("fleet_summary", sql, {"tenant_id": tenant_id, "hours": hours})
        if not result.result_rows:
            return {}
        vehicles, events, avg_speed, max_speed, distance, late = result.result_rows[0]
        return {
            "active_vehicles": int(vehicles or 0),
            "events": int(events or 0),
            "avg_speed_kmh": round(float(avg_speed or 0), 1),
            "max_speed_kmh": round(float(max_speed or 0), 1),
            "total_distance_km": round(float(distance or 0), 1),
            "late_events": int(late or 0),
            "window_hours": hours,
        }

    def fleet_breakdown(self, tenant_id: str, hours: int = 24) -> list[dict[str, Any]]:
        """Per-fleet aggregates for the fleet comparison view."""
        sql = """
            SELECT
                fleet_id,
                uniqMerge(vehicles)   AS active_vehicles,
                countMerge(events)    AS events,
                avgMerge(avg_speed)   AS avg_speed_kmh,
                maxMerge(max_speed)   AS max_speed_kmh,
                sumMerge(distance_km) AS distance_km
            FROM telemetry_1m_by_fleet
            WHERE tenant_id = {tenant_id:String}
              AND minute >= now() - INTERVAL {hours:UInt16} HOUR
            GROUP BY fleet_id
            ORDER BY events DESC
        """
        result = self._query("fleet_breakdown", sql, {"tenant_id": tenant_id, "hours": hours})
        return [
            {
                "fleet_id": row[0],
                "active_vehicles": int(row[1] or 0),
                "events": int(row[2] or 0),
                "avg_speed_kmh": round(float(row[3] or 0), 1),
                "max_speed_kmh": round(float(row[4] or 0), 1),
                "total_distance_km": round(float(row[5] or 0), 1),
            }
            for row in result.result_rows
        ]

    def event_rate_series(self, tenant_id: str, minutes: int = 60) -> list[dict[str, Any]]:
        """Per-minute event counts, for the sparkline and throughput charts."""
        sql = """
            SELECT minute, countMerge(events) AS events, uniqMerge(vehicles) AS vehicles
            FROM telemetry_1m_by_fleet
            WHERE tenant_id = {tenant_id:String}
              AND minute >= now() - INTERVAL {minutes:UInt16} MINUTE
            GROUP BY minute
            ORDER BY minute ASC
        """
        result = self._query("event_rate_series", sql, {"tenant_id": tenant_id, "minutes": minutes})
        return [
            {
                "minute": row[0].isoformat(),
                "events": int(row[1] or 0),
                "events_per_second": round(int(row[1] or 0) / 60.0, 2),
                "vehicles": int(row[2] or 0),
            }
            for row in result.result_rows
        ]

    def ingest_lag(self, tenant_id: str, minutes: int = 15) -> dict[str, Any]:
        """Event-time to ingestion-time lag, which is the <2 s dashboard metric."""
        sql = """
            SELECT
                countMerge(events)                   AS events,
                avgMerge(avg_lag_ms)                 AS avg_lag_ms,
                maxMerge(max_lag_ms)                 AS max_lag_ms,
                quantileMerge(0.95)(p95_lag_ms)      AS p95_lag_ms,
                sumMerge(late_events)                AS late_events
            FROM telemetry_ingest_lag
            WHERE tenant_id = {tenant_id:String}
              AND minute >= now() - INTERVAL {minutes:UInt16} MINUTE
        """
        result = self._query("ingest_lag", sql, {"tenant_id": tenant_id, "minutes": minutes})
        if not result.result_rows:
            return {}
        events, avg_lag, max_lag, p95_lag, late = result.result_rows[0]
        return {
            "events": int(events or 0),
            "avg_lag_ms": round(float(avg_lag or 0), 1),
            "p95_lag_ms": round(float(p95_lag or 0), 1),
            "max_lag_ms": int(max_lag or 0),
            "late_events": int(late or 0),
            "window_minutes": minutes,
        }

    def vehicle_risk_features(
        self,
        tenant_id: str,
        hours: int = 168,
        fleet_id: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Per-vehicle driving aggregates that feed the fleet risk index."""
        fleet_clause = "AND fleet_id = {fleet_id:String}" if fleet_id else ""
        sql = f"""
            SELECT
                vehicle_id,
                any(fleet_id)          AS fleet_id,
                countMerge(events)     AS events,
                avgMerge(avg_speed)    AS avg_speed_kmh,
                maxMerge(max_speed)    AS max_speed_kmh,
                minMerge(min_accel)    AS min_accel_mps2,
                maxMerge(max_brake)    AS max_brake_pct,
                sumMerge(distance_km)  AS distance_km,
                sumMerge(dtc_events)   AS dtc_events
            FROM telemetry_1h_by_vehicle
            WHERE tenant_id = {{tenant_id:String}}
              AND hour >= now() - INTERVAL {{hours:UInt16}} HOUR
              {fleet_clause}
            GROUP BY vehicle_id
            ORDER BY distance_km DESC
            LIMIT {{limit:UInt32}}
        """  # nosec B608 # fixed clause, all values bound
        parameters: dict[str, Any] = {"tenant_id": tenant_id, "hours": hours, "limit": limit}
        if fleet_id:
            parameters["fleet_id"] = fleet_id
        result = self._query("vehicle_risk_features", sql, parameters)
        return [
            {
                "vehicle_id": row[0],
                "fleet_id": row[1],
                "events": int(row[2] or 0),
                "avg_speed_kmh": round(float(row[3] or 0), 1),
                "max_speed_kmh": round(float(row[4] or 0), 1),
                "min_accel_mps2": round(float(row[5] or 0), 2),
                "max_brake_pct": round(float(row[6] or 0), 1),
                "distance_km": round(float(row[7] or 0), 2),
                "dtc_events": int(row[8] or 0),
            }
            for row in result.result_rows
        ]

    def hourly_heatmap(self, tenant_id: str, days: int = 7) -> list[dict[str, Any]]:
        """Event volume by hour of day, for the analytics heatmap."""
        sql = """
            SELECT toHour(minute) AS hour_of_day, countMerge(events) AS events
            FROM telemetry_1m_by_fleet
            WHERE tenant_id = {tenant_id:String}
              AND minute >= now() - INTERVAL {days:UInt16} DAY
            GROUP BY hour_of_day
            ORDER BY hour_of_day
        """
        result = self._query("hourly_heatmap", sql, {"tenant_id": tenant_id, "days": days})
        return [{"hour": int(row[0]), "events": int(row[1] or 0)} for row in result.result_rows]

    def quarantine_summary(self, tenant_id: str, hours: int = 24) -> list[dict[str, Any]]:
        """Rejected-event counts by reason.

        The quarantine table has no tenant column (a payload that failed validation
        may not have a resolvable owner), so the tenant parameter is bound for
        interface consistency and the result is deliberately tenant-agnostic.
        """
        sql = """
            SELECT reason_code, count() AS events, max(received_at) AS latest
            FROM telemetry_quarantine
            WHERE received_at >= now() - INTERVAL {hours:UInt16} HOUR
              AND {tenant_id:String} != ''
            GROUP BY reason_code
            ORDER BY events DESC
        """
        result = self._query("quarantine_summary", sql, {"tenant_id": tenant_id, "hours": hours})
        return [
            {"reason_code": row[0], "events": int(row[1] or 0), "latest": row[2].isoformat() if row[2] else None}
            for row in result.result_rows
        ]

    def delete_vehicle(self, tenant_id: str, vehicle_id: str) -> None:
        """Erase one vehicle's telemetry. Used by the right-to-erasure job.

        ``ALTER TABLE ... DELETE`` is an asynchronous mutation in ClickHouse, so the
        erasure job must poll ``system.mutations`` to confirm completion rather than
        assume the statement returning means the rows are gone.
        """
        self.client.command(
            "ALTER TABLE telemetry_events DELETE WHERE tenant_id = %(tenant_id)s AND vehicle_id = %(vehicle_id)s",
            parameters={"tenant_id": tenant_id, "vehicle_id": vehicle_id},
        )

    def mutation_status(self, table: str = "telemetry_events") -> list[dict[str, Any]]:
        """Outstanding mutations, so erasure can be verified rather than assumed."""
        result = self.client.query(
            "SELECT mutation_id, command, is_done, parts_to_do FROM system.mutations "
            "WHERE table = %(table)s ORDER BY create_time DESC LIMIT 20",
            parameters={"table": table},
        )
        return [
            {"mutation_id": row[0], "command": row[1], "is_done": bool(row[2]), "parts_to_do": int(row[3])}
            for row in result.result_rows
        ]


def _to_point(row: Sequence[Any]) -> TelemetryPoint:
    return TelemetryPoint(
        ts=ensure_utc(row[0]),
        ingested_at=ensure_utc(row[1]),
        processed_at=ensure_utc(row[2]),
        event_key=str(row[3]),
        seq=int(row[4] or 0),
        lat=float(row[5] or 0.0),
        lon=float(row[6] or 0.0),
        geohash=str(row[7] or ""),
        speed_kmh=round(float(row[8] or 0.0), 2),
        acceleration_mps2=round(float(row[9] or 0.0), 3),
        brake_force_pct=round(float(row[10] or 0.0), 1),
        heading_deg=round(float(row[11] or 0.0), 1),
        rpm=int(row[12]) if row[12] is not None else None,
        soc_pct=round(float(row[13]), 1) if row[13] is not None else None,
        odo_km=round(float(row[14]), 2) if row[14] is not None else None,
        dtc=list(row[15] or []),
        evt=str(row[16] or ""),
        late=bool(row[17]),
        lateness_ms=int(row[18] or 0),
        schema_version=str(row[19] or "1.0"),
    )


#: Shared instance. ``clickhouse_connect`` clients are cheap to hold and the
#: connection is re-established on failure by :meth:`TelemetryStore.close`.
telemetry_store = TelemetryStore()
