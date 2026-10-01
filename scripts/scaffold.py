"""Non-destructive project scaffold verifier.

The original bootstrap script rewrote implemented files with placeholders on
every run.  This verifier is intentionally safe: it reports missing project
paths and points feature creation through ``feature.py`` instead.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_PATHS = [
    "apps/api/app/main.py",
    "apps/simulator/sim/main.py",
    "apps/stream-processor/processor/main.py",
    "apps/web/src/App.tsx",
    "config/features.yaml",
    "docs/IMPLEMENTATION_PLAN.md",
    "tests",
]


def main() -> None:
    missing = [path for path in REQUIRED_PATHS if not (ROOT / path).exists()]
    if missing:
        raise SystemExit("Missing scaffold paths:\n" + "\n".join(f"- {path}" for path in missing))
    print("TraceFleet scaffold is complete. Use 'python scripts/feature.py new <feature_id>' to add a feature safely.")


if __name__ == "__main__":
    main()
