"""LLM provider contract and registry.

The AI layer explains deterministic evidence. It never decides whether an incident
happened, never changes state, and has no tools. That ordering is a product
requirement: a rule crossing can be explained with a number and a threshold, a
model's opinion cannot.

Providers are plug-ins so the platform runs identically with Gemini, Mistral, a
deterministic mock, or with nothing at all. ``LLM_PROVIDER`` selects one; without
the selected provider's API key the resolver falls back to the mock so a fresh clone still
demonstrates the full UI path.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from packages.common import logging as tf_logging
from packages.common.registry import Registry

LOGGER = tf_logging.get_logger(__name__)


@dataclass
class Statement:
    """One claim in a summary, labelled and cited.

    ``kind`` separates what the evidence *shows* from what an analyst might
    *infer* from it. Presenting an inference as a fact is the specific failure the
    product must not commit, so the distinction is structural rather than stylistic.
    """

    text: str
    kind: str  # "fact" | "inference"
    evidence_ids: list[str] = field(default_factory=list)
    #: Set by the validator when a statement is rejected.
    rejected: bool = False
    rejection_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "text": self.text,
            "type": self.kind,
            "evidence_ids": list(self.evidence_ids),
        }
        if self.rejected:
            payload["rejected"] = True
            payload["rejection_reason"] = self.rejection_reason
        return payload


@dataclass
class Section:
    heading: str
    statements: list[Statement] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"heading": self.heading, "statements": [item.to_dict() for item in self.statements]}


@dataclass
class SummaryResult:
    """A provider's output, before validation."""

    sections: list[Section] = field(default_factory=list)
    uncertainty: str = ""
    limitations: str = ""
    model: str = "unknown"
    provider: str = "unknown"
    raw_output: str = ""
    latency_ms: int = 0
    tokens: dict[str, int] = field(default_factory=dict)
    available: bool = True
    unavailable_reason: str | None = None

    def all_statements(self) -> list[Statement]:
        return [statement for section in self.sections for statement in section.statements]

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary_sections": [section.to_dict() for section in self.sections],
            "uncertainty": self.uncertainty,
            "limitations": self.limitations,
            "model": self.model,
            "provider": self.provider,
            "available": self.available,
            "unavailable_reason": self.unavailable_reason,
        }


class ProviderError(Exception):
    """Raised when a provider cannot produce a parseable result."""


class LLMProvider(Protocol):
    """Contract every provider implements."""

    name: str

    def summarize(self, package: dict[str, Any], prompt: str) -> SummaryResult: ...

    def is_available(self) -> bool: ...


provider_registry: Registry[type] = Registry("LLMProviderRegistry")


def register_provider(name: str, **metadata: Any) -> Callable[[type], type]:
    return provider_registry.register(name, **metadata)


def discover_providers() -> list[str]:
    import apps.api.app.services.ai.providers as package

    return provider_registry.discover(package.__name__, list(package.__path__))


def resolve_provider(preferred: str | None = None) -> LLMProvider:
    """Instantiate the configured provider, falling back when it is unusable.

    The fallback chain is deliberate. A missing API key is the normal state for a
    fresh clone, and the demo should still show a complete narrative with working
    citations — so it degrades to the deterministic mock rather than returning an
    error. The response always names the provider that actually ran, so a mock
    summary is never mistaken for a model's.
    """
    discover_providers()
    requested = (preferred or os.getenv("LLM_PROVIDER") or "gemini").lower()

    candidate = provider_registry.get_or_none(requested)
    if candidate is not None:
        instance = candidate()
        if instance.is_available():
            return instance
        LOGGER.info(
            "requested LLM provider is unavailable, falling back",
            extra={"requested": requested, "reason": getattr(instance, "unavailable_reason", None)},
        )
    else:
        LOGGER.warning(
            "unknown LLM provider requested",
            extra={"requested": requested, "available": provider_registry.keys()},
        )

    for fallback in ("mock", "null"):
        alternative = provider_registry.get_or_none(fallback)
        if alternative is not None:
            instance = alternative()
            if instance.is_available():
                return instance

    raise ProviderError("no LLM provider is available")


def provider_inventory() -> dict[str, Any]:
    """Registered providers and whether each is usable. Surfaced by /config/plugins."""
    discover_providers()
    inventory: dict[str, Any] = {}
    for name, entry in provider_registry.entries().items():
        try:
            instance = entry.target()
            inventory[name] = {
                "module": entry.module,
                "available": instance.is_available(),
                "model": getattr(instance, "model", None),
                "reason": getattr(instance, "unavailable_reason", None),
                **entry.metadata,
            }
        except Exception as error:  # pragma: no cover
            inventory[name] = {"module": entry.module, "available": False, "reason": str(error)}
    inventory["_selected"] = (os.getenv("LLM_PROVIDER") or "gemini").lower()
    return inventory
