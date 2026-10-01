"""Event enricher plug-ins."""

from processor.enrichers.registry import (
    ENRICHER_ORDER,
    Enricher,
    EnricherRegistry,
    discover_enrichers,
    enricher_registry,
    register_enricher,
)

__all__ = [
    "ENRICHER_ORDER",
    "Enricher",
    "EnricherRegistry",
    "discover_enrichers",
    "enricher_registry",
    "register_enricher",
]

discover_enrichers()
