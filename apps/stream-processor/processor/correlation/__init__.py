"""Correlation policy plug-ins: deciding which signals belong to one incident."""

from processor.correlation.registry import (
    CandidateIncident,
    CorrelationPolicy,
    CorrelationPolicyRegistry,
    correlation_registry,
    discover_policies,
    register_policy,
)

__all__ = [
    "CandidateIncident",
    "CorrelationPolicy",
    "CorrelationPolicyRegistry",
    "correlation_registry",
    "discover_policies",
    "register_policy",
]

discover_policies()
