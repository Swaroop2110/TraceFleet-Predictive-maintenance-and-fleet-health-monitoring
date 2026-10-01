"""AI summary layer.

Deterministic evidence in, cited narrative out, with a mandatory validation pass in
between. Nothing here can change incident state.
"""

from apps.api.app.services.ai.registry import (
    LLMProvider,
    ProviderError,
    Section,
    Statement,
    SummaryResult,
    discover_providers,
    provider_inventory,
    provider_registry,
    register_provider,
    resolve_provider,
)

__all__ = [
    "LLMProvider",
    "ProviderError",
    "Section",
    "Statement",
    "SummaryResult",
    "discover_providers",
    "provider_inventory",
    "provider_registry",
    "register_provider",
    "resolve_provider",
]
