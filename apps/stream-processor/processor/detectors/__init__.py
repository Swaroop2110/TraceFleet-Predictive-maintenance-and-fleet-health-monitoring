"""Detector plug-ins.

Importing this package discovers and registers every detector module inside it.
"""

from processor.detectors.registry import (
    Detector,
    DetectorRegistry,
    Signal,
    detector_registry,
    discover_detectors,
    register_detector,
)

__all__ = [
    "Detector",
    "DetectorRegistry",
    "Signal",
    "detector_registry",
    "discover_detectors",
    "register_detector",
]

# Eager discovery: a broken plug-in should fail at startup, not mid-batch.
discover_detectors()
