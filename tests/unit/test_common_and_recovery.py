import asyncio
import json
from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from processor.consumers import BackPressure, BatchConsumer
from processor.detectors.anomaly_score import robust_z_score, window_features
from sim.vehicle_model import build_fleet, generate_vin, step, to_event

from packages.common import config as cfg
from packages.common import geo, tokens
from packages.common.dsn import libpq_url, psycopg2_kwargs, redacted_url, sqlalchemy_url
from packages.common.logging import JsonFormatter, bind, clear, get_context, unbind
from packages.common.passwords import hash_password, needs_rehash, verify_password
from packages.common.registry import Registry, RegistryError
from packages.common.schemas_compat import jsonify
from packages.common.timeutil import now
from tests.unit.test_pipeline_guarantees import message


def test_password_policy_hashes_and_validates_without_plaintext():
    stored = hash_password("correct horse", iterations=1000)
    assert "correct horse" not in stored
    assert verify_password("correct horse", stored)
    assert not verify_password("wrong", stored)
    assert not verify_password("correct horse", "broken")
    assert needs_rehash(stored, iterations=2000)
    with pytest.raises(ValueError):
        hash_password("")


def test_jwt_verification_enforces_type_and_signature():
    access = tokens.issue_token(subject="u", tenant_id="t", roles=["ANALYST"])
    claims = tokens.verify_token(access)
    assert claims["tenant_id"] == "t"
    refresh = tokens.issue_token(subject="u", tenant_id="t", roles=["ANALYST"], token_type="refresh")
    with pytest.raises(tokens.TokenError):
        tokens.verify_token(refresh)
    assert tokens.verify_token(refresh, expected_type="refresh")["sub"] == "u"
    with pytest.raises(tokens.TokenError):
        tokens.verify_token(access[:-12] + "tampered")
    document = tokens.jwks()
    assert document["keys"][0]["alg"] == "RS256"
    assert "d" not in document["keys"][0]


def test_dsn_driver_and_secret_redaction(monkeypatch):
    monkeypatch.setenv("POSTGRES_URL", "postgresql+psycopg2://app:secret@db:5432/fleet")
    assert libpq_url() == "postgresql://app:secret@db:5432/fleet"
    assert sqlalchemy_url().startswith("postgresql+psycopg2://")
    assert psycopg2_kwargs()["dsn"] == libpq_url()
    assert "secret" not in redacted_url()


def test_geohash_preserves_permitted_cell_and_rejects_invalid_position():
    value = geo.encode(21.17, 72.83, 7)
    lat, lon = geo.decode(value)
    assert abs(lat - 21.17) < 0.002 and abs(lon - 72.83) < 0.002
    assert geo.truncate(value, 5) == value[:5]
    masked = geo.mask_coordinate(21.17, 72.83, 5)
    assert geo.encode(*masked, 5) == value[:5]
    with pytest.raises(ValueError):
        geo.encode(100, 10)


def test_logging_context_is_independent_and_secrets_are_redacted():
    import logging

    clear()
    bind(tenant="a", password="never-print")
    unbind("tenant")
    assert "tenant" not in get_context()
    rendered = JsonFormatter("test").format(logging.LogRecord("test", 20, __file__, 1, "message", (), None))
    assert "never-print" not in rendered and json.loads(rendered)["service"] == "test"
    clear()


def test_registry_rejects_collisions_and_supports_removal():
    registry = Registry("test")

    def target():
        return 1

    registry.add("one", target)
    assert registry.get("one")() == 1
    assert next(iter(registry)).key == "one"
    with pytest.raises(RegistryError):
        registry.add("one", lambda: 2)
    registry.unregister("one")
    assert registry.get_or_none("one") is None
    with pytest.raises(RegistryError):
        registry.get("one")


def test_config_rejects_invalid_threshold_combinations():
    with pytest.raises(ValueError):
        cfg.CorrelationConfig(post_window_s=300, reconstruction_timeout_s=30)
    with pytest.raises(ValueError):
        cfg.FactorConfig(weight=0.2, min_value=100, max_value=10)
    with pytest.raises(ValueError):
        cfg.DetectionRulesConfig(rules={}, incident_trigger={"must_include_any": ["unknown"]})
    assert cfg.roles().allows(["ANALYST"], "incident:acknowledge")
    assert not cfg.roles().allows(["VIEWER"], "incident:close")


def test_seeded_kinematics_remain_valid_and_repeatable():
    from packages.schemas.models import RawTelemetryV1

    left = build_fleet(10, seed=42)
    right = build_fleet(10, seed=42)
    assert [v.vin for v in left] == [v.vin for v in right]
    for i in range(30):
        for a, b in zip(left, right, strict=True):
            step(a)
            step(b)
            assert a.speed_kmh == b.speed_kmh
            event = to_event(a, now() + timedelta(seconds=i))
            RawTelemetryV1.model_validate(event)
    assert len(generate_vin(123)) == 17


def test_anomaly_features_and_scores_are_explainable():
    from processor.window import VehicleWindow

    from packages.common.timeutil import to_iso

    window = VehicleWindow(30)
    start = now()
    for i in range(5):
        window.add_event(
            {
                "ts": to_iso(start + timedelta(seconds=i)),
                "speed_kmh": i * 30,
                "acceleration_mps2": i * 3,
                "heading_deg": i * 60,
            }
        )
    features = window_features(window)
    score, names = robust_z_score(features, {})
    assert 0 <= score <= 1 and names and features["max_heading_rate"] == 60
    assert robust_z_score({}, {}) == (0, [])


def test_backpressure_pauses_retries_and_commits_only_after_success(monkeypatch):
    from processor import consumers

    monkeypatch.setattr(consumers.time, "sleep", lambda _: None)
    client = MagicMock()
    client.assignment.return_value = ["partition"]
    consumer = BatchConsumer(["topic"], "test", consumer=client)
    calls = []

    def work(batch):
        calls.append(batch)
        if len(calls) == 1:
            raise BackPressure("store down")

    consumer._process_with_retry([message({})], work)
    assert len(calls) == 2
    client.pause.assert_called_once()
    client.resume.assert_called_once()
    client.commit.assert_called_once()


def test_poison_message_is_durable_before_offset_commit(monkeypatch):
    from processor import consumers

    monkeypatch.setattr(consumers.time, "sleep", lambda _: None)
    client = MagicMock()
    producer = MagicMock()
    producer.flush.return_value = 0
    batch = BatchConsumer(["topic"], "test", consumer=client, producer=producer, max_retries=0)

    def reject(_):
        raise ValueError("bad record")

    batch._process_with_retry([message({})], reject)
    producer.produce.assert_called_once()
    producer.flush.assert_called_once()
    client.commit.assert_called_once()
    payload = json.loads(producer.produce.call_args.kwargs["value"])
    assert payload["error_type"] == "ValueError"


def test_websocket_fanout_is_once_per_tenant_and_drops_failed_peers():
    from apps.api.app.ws.manager import ConnectionManager

    class Socket:
        def __init__(self, fail=False):
            self.messages = []
            self.fail = fail

        async def accept(self):
            pass

        async def send_text(self, text):
            if self.fail:
                raise RuntimeError("gone")
            self.messages.append(text)

    async def check():
        manager = ConnectionManager()
        a = Socket()
        b = Socket()
        other = Socket()
        broken = Socket(True)
        for socket in (a, b, broken):
            await manager.connect(socket, "a")
        await manager.connect(other, "b")
        assert await manager.broadcast("event", "a") == 2
        assert a.messages == b.messages == ["event"] and other.messages == []
        assert manager.client_count("a") == 2
        await manager.shutdown()
        assert manager.client_count() == 0

    asyncio.run(check())


def test_json_conversion_is_stable_for_dates_and_decimals():
    from decimal import Decimal

    value = jsonify({"value": Decimal("1.25"), "ts": now(), "set": {"a"}})
    assert value["value"] == 1.25 and isinstance(value["ts"], str)


def test_kafka_trace_headers_preserve_parent_across_publish():
    from packages.common import otel

    otel.configure("unit-trace")
    with otel.producer_span("source"):
        trace_id = otel.current_trace_id()
        headers = otel.inject_headers()
    with otel.consumer_span("validator", headers=headers):
        assert otel.current_trace_id() == trace_id
        canonical_headers = otel.inject_headers()
    with otel.producer_span("canonical", headers=canonical_headers):
        assert otel.current_trace_id() == trace_id


def test_operations_handles_empty_histogram_without_nonfinite_json(monkeypatch):
    from apps.api.app.modules import ops

    monkeypatch.setattr(ops, "_prometheus_query", lambda _: [{"value": [0, "NaN"]}])
    assert ops._scalar("histogram") == 0
