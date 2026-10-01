"""Optional statistical anomaly detector (feature ``anomaly_model``, off by default).

This detector never overrides the rules. It contributes an additional signal so
an operator can see that a window looked statistically unusual, but the
deterministic rules remain authoritative for whether an incident exists. That
ordering is a product requirement, not an implementation convenience: a rule
crossing can be explained with a number and a threshold, a model score cannot.

Two scoring paths:

* If ``models/anomaly.joblib`` exists (written by ``scripts/train_anomaly.py``)
  its Isolation Forest is used.
* Otherwise a robust z-score over window features, which needs no dependency
  beyond the standard library and is what runs in CI.
"""

from __future__ import annotations

import os
import statistics
from pathlib import Path
from typing import Any

from packages.common import feature_registry
from processor.detectors._helpers import event_ts
from processor.detectors.registry import Signal, register_detector

_MODEL: Any = None
_MODEL_LOAD_ATTEMPTED = False


def _model_path() -> Path:
    from packages.common.config import project_root

    return Path(os.getenv("ANOMALY_MODEL_PATH") or project_root() / "models" / "anomaly.joblib")


def _load_model() -> Any:
    """Load the trained model once; absence is normal, not an error."""
    global _MODEL, _MODEL_LOAD_ATTEMPTED
    if _MODEL_LOAD_ATTEMPTED:
        return _MODEL
    _MODEL_LOAD_ATTEMPTED = True
    path = _model_path()
    if not path.exists():
        return None
    try:
        import joblib

        _MODEL = joblib.load(path)
    except Exception:
        # A missing or incompatible scikit-learn build must not stop detection.
        _MODEL = None
    return _MODEL


def window_features(window: Any) -> dict[str, float]:
    """Features describing recent driving behaviour.

    Jerk (the rate of change of acceleration) and heading rate are included
    because they capture erratic control inputs that instantaneous speed and
    acceleration thresholds miss.
    """
    events = list(getattr(window, "events", []))
    if len(events) < 3:
        return {}

    speeds = [float(item.get("speed_kmh") or 0.0) for item in events]
    accels = [float(item.get("acceleration_mps2") or 0.0) for item in events]
    headings = [float(item.get("heading_deg") or 0.0) for item in events]

    jerks: list[float] = []
    heading_rates: list[float] = []
    for index in range(1, len(events)):
        elapsed = (event_ts(events[index]) - event_ts(events[index - 1])).total_seconds()
        if elapsed <= 0:
            continue
        jerks.append(abs(accels[index] - accels[index - 1]) / elapsed)
        delta = abs((headings[index] - headings[index - 1]) % 360.0)
        heading_rates.append((360.0 - delta if delta > 180.0 else delta) / elapsed)

    return {
        "speed_variance": statistics.pvariance(speeds) if len(speeds) > 1 else 0.0,
        "speed_mean": statistics.fmean(speeds),
        "accel_variance": statistics.pvariance(accels) if len(accels) > 1 else 0.0,
        "max_jerk": max(jerks, default=0.0),
        "mean_heading_rate": statistics.fmean(heading_rates) if heading_rates else 0.0,
        "max_heading_rate": max(heading_rates, default=0.0),
    }


def robust_z_score(features: dict[str, float], params: dict[str, Any]) -> tuple[float, list[str]]:
    """Median-absolute-deviation score against configured reference statistics.

    MAD rather than standard deviation because the reference distribution is
    contaminated by the very outliers being scored; a single hard brake would
    inflate a standard deviation enough to hide the next one.
    """
    reference: dict[str, list[float]] = params.get("reference") or {
        # median, MAD, from a normal-profile simulator run.
        "speed_variance": [12.0, 8.0],
        "accel_variance": [0.8, 0.6],
        "max_jerk": [1.2, 0.9],
        "max_heading_rate": [3.0, 2.5],
    }

    contributions: list[tuple[float, str]] = []
    for name, stats in reference.items():
        if name not in features:
            continue
        median, mad = float(stats[0]), max(float(stats[1]), 1e-6)
        z = abs(features[name] - median) / (1.4826 * mad)
        contributions.append((z, name))

    if not contributions:
        return 0.0, []
    contributions.sort(reverse=True)
    # Squash to [0,1] so the score is comparable with rule-derived weights.
    peak = contributions[0][0]
    score = min(1.0, peak / float(params.get("z_saturation", 6.0)))
    return score, [name for _, name in contributions[:3]]


@register_detector("anomaly_score", description="Statistical outlier score over window features", optional=True)
def detect_anomaly(
    window: Any,
    event: dict[str, Any],
    params: dict[str, Any],
    weight: float,
) -> Signal | None:
    if not feature_registry.is_enabled("anomaly_model"):
        return None

    features = window_features(window)
    if not features:
        return None

    threshold = float(params.get("score_gt", 0.7))
    model = _load_model()
    if model is not None:
        try:
            ordered = [features[key] for key in sorted(features)]
            # decision_function is negative for outliers; map to [0,1].
            raw = float(model.decision_function([ordered])[0])
            score = max(0.0, min(1.0, 0.5 - raw))
            top_features = sorted(features, key=lambda key: features[key], reverse=True)[:3]
            source = "isolation_forest"
        except Exception:
            score, top_features = robust_z_score(features, params)
            source = "robust_z_score_fallback"
    else:
        score, top_features = robust_z_score(features, params)
        source = "robust_z_score"

    if score <= threshold:
        return None

    return Signal(
        name="anomaly_score",
        event_key=event["event_key"],
        value=round(score, 4),
        threshold=threshold,
        ts=event_ts(event),
        weight=weight,
        detail={
            "scorer": source,
            "top_features": top_features,
            "features": {k: round(v, 4) for k, v in features.items()},
        },
    )
