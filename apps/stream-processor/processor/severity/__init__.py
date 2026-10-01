"""Severity policy plug-ins."""

from processor.severity.registry import (
    SeverityPolicy,
    SeverityPolicyRegistry,
    discover_severity_policies,
    register_severity_policy,
    severity_registry,
)

__all__ = [
    "SeverityPolicy",
    "SeverityPolicyRegistry",
    "discover_severity_policies",
    "register_severity_policy",
    "severity_registry",
]

discover_severity_policies()
