"""Score detection against hidden ground truth, including negative scenarios."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from packages.common.timeutil import parse_ts


def read_records(path: Path) -> list[dict[str, Any]]:
    text = path.read_text().strip()
    if not text:
        return []
    return (
        json.loads(text) if text.startswith("[") else [json.loads(line) for line in text.splitlines() if line.strip()]
    )


def evaluate(ground_truth: list[dict[str, Any]], incidents: list[dict[str, Any]]) -> dict[str, Any]:
    used: set[str] = set()
    tp = fp = fn = tn = severity_matches = signal_matches = timeline_matches = 0
    details = []
    for truth in ground_truth:
        vehicle = truth.get("vehicle_id")
        vehicles = truth.get("vehicle_ids") or [vehicle]
        candidates = [i for i in incidents if i.get("vehicle_id") in vehicles and str(i["incident_id"]) not in used]
        start, end = truth.get("start_ts"), truth.get("end_ts")
        if start and end:
            candidates = [
                i
                for i in candidates
                if parse_ts(i["start_ts"]) <= parse_ts(end)
                and parse_ts(i.get("end_ts") or i["start_ts"]) >= parse_ts(start)
            ]
        expected = truth.get("should_create_incident", truth.get("expected_incident", False))
        matched = candidates[0] if candidates else None
        if expected:
            if matched:
                tp += 1
                used.add(str(matched["incident_id"]))
                severity_matches += matched.get("severity") == truth.get("expected_severity")
                observed = set((matched.get("metadata") or {}).get("signal_names", []))
                signal_matches += set(truth.get("expected_signals") or []).issubset(observed)
                timeline_matches += bool(
                    start and end and parse_ts(start) <= parse_ts(matched["start_ts"]) <= parse_ts(end)
                )
            else:
                fn += 1
        elif matched:
            fp += 1
            used.add(str(matched["incident_id"]))
        else:
            tn += 1
        details.append(
            {
                "scenario": truth.get("scenario_id"),
                "vehicle_id": vehicle,
                "expected_incident": expected,
                "matched": matched.get("incident_id") if matched else None,
            }
        )
    labelled_vehicles = {v for g in ground_truth for v in (g.get("vehicle_ids") or [g.get("vehicle_id")])}
    fp += sum(str(i["incident_id"]) not in used and i.get("vehicle_id") in labelled_vehicles for i in incidents)
    expected_count = tp + fn
    return {
        "true_positive": tp,
        "true_negative": tn,
        "false_positive": fp,
        "false_negative": fn,
        "precision": tp / (tp + fp) if tp + fp else 1.0,
        "recall": tp / expected_count if expected_count else 1.0,
        "accuracy": (tp + tn) / max(1, tp + tn + fp + fn),
        "severity_match": severity_matches / expected_count if expected_count else 1.0,
        "signal_match": signal_matches / expected_count if expected_count else 1.0,
        "timeline_accuracy": timeline_matches / expected_count if expected_count else 1.0,
        "details": details,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("ground_truth", type=Path, nargs="?")
    parser.add_argument("incidents", type=Path, nargs="?")
    parser.add_argument("--report", type=Path)
    parser.add_argument("--run-id", help="Score only the specified hidden-label run")
    parser.add_argument("--minimum", type=float, default=0.95)
    args = parser.parse_args()
    if args.ground_truth and args.incidents:
        truth, incidents = read_records(args.ground_truth), read_records(args.incidents)
    else:
        import psycopg2
        from psycopg2.extras import RealDictCursor

        from packages.common.dsn import psycopg2_kwargs

        with psycopg2.connect(**psycopg2_kwargs(owner=True)) as db, db.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM ground_truth_incident WHERE (%s IS NULL OR run_id=%s)", (args.run_id, args.run_id)
            )
            truth = [dict(row) for row in cur.fetchall()]
            cur.execute(
                "SELECT incident_id,vehicle_id,start_ts,end_ts,severity,metadata_json AS metadata FROM incident"
            )
            incidents = [dict(row) for row in cur.fetchall()]
    result = evaluate(truth, incidents)
    output = json.dumps(result, indent=2, default=str)
    print(output)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(output + "\n")
    if any(
        result[key] < args.minimum
        for key in ("precision", "recall", "accuracy", "severity_match", "signal_match", "timeline_accuracy")
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
