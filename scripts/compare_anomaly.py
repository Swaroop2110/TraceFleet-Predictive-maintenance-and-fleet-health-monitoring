"""Compare optional statistical scoring with rule outcomes on labelled scenarios."""

import json
from pathlib import Path

from processor.detectors.anomaly_score import robust_z_score, window_features
from processor.incident_processor import IncidentProcessor
from processor.window import VehicleWindow
from sim.scenarios.registry import generate


def main():
    rows = []
    for name in ["normal", "collision_like", "overspeed_chain", "false_positive_hard_brake", "diagnostic_only"]:
        result = generate(name, "compare_" + name)
        engine = IncidentProcessor(connect=False)
        window = VehicleWindow(30)
        scores = []
        for raw in result.events:
            event = dict(
                raw,
                event_key=str(raw["seq"]),
                tenant_id="tenant_default",
                fleet_id="fleet_default",
                flags={},
                geohash="te7t",
            )
            window.add_event(event)
            score, contributors = robust_z_score(window_features(window), {})
            scores.append(score)
            engine.process_event(event)
        rows.append(
            {
                "scenario": name,
                "rule_incident": bool(engine.published),
                "expected": result.ground_truth.should_create_incident,
                "max_optional_score": max(scores, default=0),
                "last_score_contributors": contributors,
            }
        )
    report = {
        "method": "robust median absolute deviation",
        "decision_policy": "supplemental scores do not count toward incident trigger or severity",
        "rows": rows,
    }
    Path("artifacts").mkdir(exist_ok=True)
    Path("artifacts/anomaly-comparison.json").write_text(json.dumps(report, indent=2) + "\n")
    assert all(r["rule_incident"] == r["expected"] for r in rows)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
