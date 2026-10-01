"""Evidence exporter contract and registry.

An export is an evidence bundle that may leave the platform and end up attached to a
claim or a report. So every format includes the same three things: the evidence with
its thresholds, the data-quality block, and the disclaimer that thresholds are
configuration defaults rather than safety limits. An export that omits the caveats
becomes a document that overstates what the platform determined.

Adding a format means adding one file with ``@register_exporter``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from packages.common.registry import Registry


@dataclass
class ExportResult:
    content: bytes
    media_type: str
    extension: str

    def filename(self, incident_id: str) -> str:
        return f"{incident_id}-evidence.{self.extension}"


class Exporter(Protocol):
    """Contract every exporter implements."""

    def export(self, bundle: dict[str, Any]) -> ExportResult: ...


exporter_registry: Registry[Exporter] = Registry("ExporterRegistry")


def register_exporter(name: str, **metadata: Any) -> Callable[[Exporter], Exporter]:
    return exporter_registry.register(name, **metadata)


def discover_exporters() -> list[str]:
    import apps.api.app.services.exporters as package

    return exporter_registry.discover(package.__name__, list(package.__path__), skip=("registry",))


def available_formats() -> list[str]:
    discover_exporters()
    return exporter_registry.keys()


def export(bundle: dict[str, Any], fmt: str) -> ExportResult:
    discover_exporters()
    return exporter_registry.get(fmt)(bundle)


#: Text that must appear in every exported bundle.
DISCLAIMER = (
    "TraceFleet detects patterns in vehicle telemetry against configured thresholds. "
    "Thresholds are configuration defaults, not real-world safety limits. Severity is "
    "a triage classification. This document does not determine fault, liability, or "
    "cause, and contains no assessment of any person."
)
