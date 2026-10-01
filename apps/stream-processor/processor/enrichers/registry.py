"""Enricher contract and registry.

An enricher adds derived fields to a canonical event before it is published.
Enrichers run in a declared order and must be idempotent: a Kafka replay will
run them again over the same event, and the result has to be identical or the
event_key stops identifying a single row.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from packages.common.registry import Registry


class Enricher(Protocol):
    """Signature every enricher implements. Mutates and returns the event dict."""

    def __call__(self, event: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]: ...


enricher_registry: Registry[Enricher] = Registry("EnricherRegistry")

#: Explicit order. ``data_quality_flags`` runs last so it can observe whatever
#: earlier enrichers could not populate.
ENRICHER_ORDER = ("geohash", "data_quality_flags")


def register_enricher(name: str, **metadata: Any) -> Callable[[Enricher], Enricher]:
    return enricher_registry.register(name, **metadata)


def discover_enrichers() -> list[str]:
    import processor.enrichers as package

    return enricher_registry.discover(package.__name__, list(package.__path__), skip=("registry",))


class EnricherRegistry:
    """Facade the normaliser calls."""

    @staticmethod
    def available() -> list[str]:
        return enricher_registry.keys()

    @staticmethod
    def enrich(event: dict[str, Any], context: dict[str, Any] | None = None) -> dict[str, Any]:
        """Run every enricher in order."""
        ctx = context or {}
        ordered = [name for name in ENRICHER_ORDER if name in enricher_registry]
        ordered += [name for name in enricher_registry.keys() if name not in ENRICHER_ORDER]  # noqa: SIM118
        for name in ordered:
            event = enricher_registry.get(name)(event, ctx)
        return event
