import json
from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from processor.consumers import BackPressure, BatchConsumer, decode_json_batch
from processor.dedup import BoundedLRU, DedupGate
from processor.incident_persister import IncidentPersister
from processor.incident_processor import IncidentProcessor
from processor.normalizer import Normalizer, Ownership, QuarantineError, TenantResolver, validate
from processor.sink_clickhouse import Sink
from processor.window import VehicleWindow, WindowStore
from sim.scenarios.registry import generate

from packages.common.ids import event_key
from packages.common.kafka import ReliableProducer
from packages.common.timeutil import now, to_iso
from packages.schemas.models import QuarantineReason


def message(payload, partition=0, offset=0, topic="telemetry.canonical"):
    m = MagicMock()
    m.value.return_value = json.dumps(payload).encode()
    m.partition.return_value = partition
    m.offset.return_value = offset
    m.topic.return_value = topic
    m.headers.return_value = []
    m.error.return_value = None
    return m


def canonical_events(name="collision_like", vehicle="veh_test"):
    resolver = TenantResolver()
    resolver.seed({vehicle: Ownership("tenant_default", "fleet_default")})
    normalizer = Normalizer(resolver)
    return [
        normalizer.to_canonical(validate(e)[0]).model_dump(mode="json")
        for e in generate(name, vehicle, start_time=now()).events
    ]


def test_commit_covers_every_partition_with_highest_offset():
    consumer = MagicMock()
    batch = BatchConsumer(["telemetry.canonical"], "test", consumer=consumer)
    batch._commit([message({}, 0, 4), message({}, 1, 7), message({}, 0, 6)])
    partitions = consumer.commit.call_args.kwargs["offsets"]
    assert {(p.partition, p.offset) for p in partitions} == {(0, 7), (1, 8)}


def test_commit_failure_is_backpressure():
    consumer = MagicMock()
    consumer.commit.side_effect = RuntimeError("broker down")
    batch = BatchConsumer(["telemetry.canonical"], "test", consumer=consumer)
    with pytest.raises(BackPressure):
        batch._commit([message({})])


def test_dead_letter_failure_does_not_commit():
    consumer = MagicMock()
    producer = MagicMock()
    producer.flush.return_value = 1
    batch = BatchConsumer(["telemetry.canonical"], "test", consumer=consumer, producer=producer)

    def fail(_):
        raise ValueError("invalid")

    with pytest.raises(BackPressure):
        batch._isolate_poison([message({})], fail, ValueError("invalid"))
    consumer.commit.assert_not_called()


def test_sink_flushes_small_batch_before_handler_returns():
    sink = object.__new__(Sink)
    sink._client = MagicMock()
    sink._pending = []
    sink._last_flush = 0
    sink._consecutive_failures = 0
    sink.batch_size = 50000
    sink.handle_batch([message(canonical_events()[0])])
    assert sink._client.insert.call_count == 1
    assert sink._pending == []


def test_sink_retries_without_buffer_amplification():
    sink = object.__new__(Sink)
    client = MagicMock()
    client.insert.side_effect = RuntimeError("store down")
    sink._client = client
    sink._pending = []
    sink._last_flush = 0
    sink._consecutive_failures = 0
    sink.batch_size = 1000
    events = [message(canonical_events()[0])]
    for _ in range(2):
        sink._client = client
        with pytest.raises(BackPressure):
            sink.handle_batch(events)
        assert len(sink._pending) == 1


def test_async_delivery_error_is_not_mistaken_for_success():
    client = MagicMock()
    client.flush.return_value = 0
    producer = ReliableProducer(client)
    producer.produce("topic", value=b"data")
    client.produce.call_args.kwargs["on_delivery"]("timeout", None)
    with pytest.raises(RuntimeError, match="delivery failed"):
        producer.flush()


def test_decode_preserves_bad_payload_for_quarantine():
    m = message({})
    m.value.return_value = b"not json"
    assert decode_json_batch([m])[0][1] is None


@pytest.mark.parametrize(
    "name",
    ["normal", "false_positive_hard_brake", "diagnostic_only", "duplicate_storm", "out_of_order", "late_arrival"],
)
def test_negative_scenarios_do_not_create_incidents(name):
    processor = IncidentProcessor(connect=False)
    for event in canonical_events(name):
        processor.process_event(event)
    assert not processor.published


def test_processor_restores_reconstruction_after_restart():
    values = {}
    cache = MagicMock()
    cache.get.side_effect = lambda k: values.get(k)
    cache.set.side_effect = lambda k, v, **kw: values.__setitem__(k, v)
    original = IncidentProcessor(connect=False, redis_client=cache)
    events = canonical_events()
    for event in events:
        original.process_event(event)
    original._checkpoint(events[-1])
    recovered = IncidentProcessor(connect=False, redis_client=cache)
    recovered._restore(events[-1])
    assert recovered.candidates["veh_test"].incident_id == original.candidates["veh_test"].incident_id
    assert len(recovered.candidates["veh_test"].signals) == len(original.candidates["veh_test"].signals)
    final = dict(
        events[-1],
        ts=to_iso(now() + timedelta(seconds=90)),
        event_key="finish",
        speed_kmh=20,
        acceleration_mps2=0,
        brake_force_pct=0,
    )
    recovered.process_event(final)
    assert recovered.published[-1].incident.status.value == "OPEN"


def test_batch_failure_restores_incident_memory():
    producer = MagicMock()
    producer.flush.side_effect = RuntimeError("broker lost")
    processor = IncidentProcessor(connect=False, producer=producer)
    with pytest.raises(RuntimeError):
        processor.handle_batch([message(e) for e in canonical_events()])
    assert not processor.candidates
    assert not processor.last_trigger


def test_lru_capacity_and_tenant_scoped_dedup():
    lru = BoundedLRU(capacity=2)
    assert lru.add("a")
    assert not lru.add("a")
    lru.add("b")
    lru.add("c")
    assert len(lru) == 2
    gate = DedupGate()
    assert gate.check("a", "key").is_new
    assert gate.check("b", "key").is_new
    assert gate.check("a", "key").is_duplicate


def test_dedup_redis_failure_falls_back_and_recovers():
    client = MagicMock()
    client.set.side_effect = RuntimeError("down")
    gate = DedupGate(client)
    assert gate.check("a", "x").is_new and gate.degraded
    assert gate.check("a", "x").is_duplicate
    client.set.side_effect = None
    client.set.return_value = True
    assert gate.check("a", "y").is_new and not gate.degraded


def test_persister_does_not_overwrite_human_decisions():
    for state in ("ACKNOWLEDGED", "CLOSED", "INVALIDATED"):
        assert IncidentPersister._resolve_status(state, "OPEN") == state
    assert IncidentPersister._resolve_status("RECONSTRUCTING", "OPEN") == "OPEN"


def test_window_orders_late_events_and_evicts_expired_data():
    window = VehicleWindow(30)
    start = now()
    for seconds in (0, 3, 1, 35):
        window.add_event({"ts": to_iso(start + timedelta(seconds=seconds)), "speed_kmh": seconds})
    assert [e["speed_kmh"] for e in window.events] == [35]
    assert window.late_inserts == 1
    store = WindowStore(30, max_vehicles=2)
    store.get("a")
    store.get("b")
    store.get("c")
    assert "a" not in store and len(store) == 2


def test_schema_faults_have_specific_reasons():
    result = generate("schema_fault", "bad").events
    reasons = []
    for raw in result:
        with pytest.raises(QuarantineError) as error:
            validate(raw)
        reasons.append(error.value.reason)
    assert QuarantineReason.INVALID_VIN in reasons
    assert QuarantineReason.UNKNOWN_SCHEMA_VERSION in reasons


def test_canonical_identity_and_lateness_do_not_change_vehicle_time():
    vehicle = "veh_late"
    raw = validate(generate("normal", vehicle).events[0])[0]
    resolver = TenantResolver()
    resolver.seed({vehicle: Ownership("t", "f")})
    event = Normalizer(resolver).to_canonical(raw, ingested_at=raw.ts + timedelta(seconds=45))
    assert event.ts == raw.ts and event.flags["late"]
    assert event.event_key == event_key(vehicle, raw.seq, raw.ts)
    assert event.tenant_id == "t" and event.lateness_ms == 45000
