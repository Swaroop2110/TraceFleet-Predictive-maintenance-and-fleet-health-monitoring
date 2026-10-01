"""Correlation contract and the in-flight candidate incident.

Correlation answers one question: does this new signal extend the incident the
vehicle is already accumulating, or start a new one? Getting it wrong in either
direction is visible to an operator — merge too eagerly and two separate events
become one confusing timeline, merge too little and a single collision arrives
as four incidents.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from packages.common.registry import Registry
from packages.schemas.models import IncidentStatus


@dataclass
class CandidateIncident:
    """Mutable incident under construction inside the processor.

    Holds only what correlation and triggering need. It is converted to an
    :class:`~packages.schemas.models.IncidentPayload` at publish time.
    """

    vehicle_id: str
    tenant_id: str
    fleet_id: str
    start_ts: datetime
    last_signal_ts: datetime
    signals: list[Any] = field(default_factory=list)
    status: IncidentStatus = IncidentStatus.DETECTED
    incident_id: str | None = None
    idempotency_key: str | None = None
    incident_type: str | None = None
    severity: str | None = None
    #: Set once the trigger threshold has been crossed and the incident published.
    triggered: bool = False
    triggered_at: datetime | None = None
    #: When reconstruction may finalise: the trigger time plus post_window_s.
    reconstruct_after: datetime | None = None
    #: Event keys already folded in, so a replayed event cannot inflate evidence.
    seen_event_keys: set[str] = field(default_factory=set)
    late_evidence_added: bool = False
    trace_id: str | None = None
    #: Counters feeding the incident's data-quality block.
    late_events: int = 0
    out_of_order_events: int = 0
    duplicates_removed: int = 0
    missing_fields: set[str] = field(default_factory=set)
    schema_transformed: int = 0
    #: Revision number, incremented on every published update.
    revision: int = 0

    def distinct_signal_names(self) -> set[str]:
        return {signal.name for signal in self.signals}

    def add_signals(self, signals: list[Any]) -> list[Any]:
        """Append signals not already recorded, returning the ones accepted.

        Deduplication is by (event_key, name): a replayed event re-runs the
        detectors and would otherwise add a second copy of the same evidence.
        """
        accepted: list[Any] = []
        for signal in signals:
            marker = f"{signal.event_key}:{signal.name}"
            if marker in self.seen_event_keys:
                continue
            self.seen_event_keys.add(marker)
            self.signals.append(signal)
            accepted.append(signal)
        if accepted:
            self.signals.sort(key=lambda item: (item.ts, item.name))
            self.start_ts = min(self.start_ts, self.signals[0].ts)
            self.last_signal_ts = max(self.last_signal_ts, self.signals[-1].ts)
        return accepted


class CorrelationPolicy(Protocol):
    """Signature every correlation policy implements."""

    def __call__(
        self,
        candidate: CandidateIncident,
        signal: Any,
        config: Any,
    ) -> bool: ...


correlation_registry: Registry[CorrelationPolicy] = Registry("CorrelationPolicyRegistry")


def register_policy(name: str, **metadata: Any) -> Callable[[CorrelationPolicy], CorrelationPolicy]:
    return correlation_registry.register(name, **metadata)


def discover_policies() -> list[str]:
    import processor.correlation as package

    return correlation_registry.discover(package.__name__, list(package.__path__), skip=("registry",))


class CorrelationPolicyRegistry:
    """Facade the incident processor calls."""

    @staticmethod
    def available() -> list[str]:
        return correlation_registry.keys()

    @staticmethod
    def should_group(
        candidate: CandidateIncident,
        signal: Any,
        config: Any,
        policy_name: str | None = None,
    ) -> bool:
        """Whether ``signal`` belongs to ``candidate`` under the active policy."""
        name = policy_name or getattr(config, "policy", None) or "same_vehicle_time_window"
        policy = correlation_registry.get(name)
        return policy(candidate, signal, config)
