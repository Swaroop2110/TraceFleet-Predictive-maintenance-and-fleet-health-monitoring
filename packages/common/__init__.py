"""Shared utilities every TraceFleet service depends on.

Nothing in this package may import from ``apps.*``. The dependency arrow points
one way so the API, the stream processor, and the simulator can each be built
and tested in isolation.
"""

from packages.common.features import Features, feature_registry

__all__ = ["Features", "feature_registry"]
