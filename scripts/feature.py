"""Safely inspect and evolve TraceFleet's feature manifest."""

import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
FEATURE_PATH = ROOT / "config" / "features.yaml"
FEATURE_ID = re.compile(r"^[a-z][a-z0-9_]*$")


def load_features(yaml_path: Path = FEATURE_PATH) -> dict:
    with yaml_path.open(encoding="utf-8") as file:
        return (yaml.safe_load(file) or {}).get("features", {})


def validate(features: dict) -> None:
    for feature_id, details in features.items():
        for dependency in details.get("depends_on", []):
            if dependency not in features:
                raise ValueError(f"{feature_id} depends on unknown feature {dependency}")
            if details.get("enabled") and not features[dependency].get("enabled"):
                raise ValueError(f"Cannot enable {feature_id}: dependency {dependency} is disabled")


def save_features(features: dict, yaml_path: Path = FEATURE_PATH) -> None:
    validate(features)
    with yaml_path.open("w", encoding="utf-8") as file:
        yaml.safe_dump({"features": features}, file, sort_keys=False)


def list_features(features: dict) -> None:
    print(f"{'FEATURE_ID':<30} | {'ENABLED':<10} | DEPENDS_ON")
    print("-" * 72)
    for feature_id, details in features.items():
        print(f"{feature_id:<30} | {details.get('enabled', False)!s:<10} | {', '.join(details.get('depends_on', []))}")


def set_feature(features: dict, feature_id: str, enabled: bool) -> None:
    if feature_id not in features:
        raise ValueError(f"Feature {feature_id} not found")
    if not enabled:
        dependents = [
            name
            for name, details in features.items()
            if feature_id in details.get("depends_on", []) and details.get("enabled")
        ]
        if dependents:
            raise ValueError(f"Cannot disable {feature_id}; enabled features depend on it: {', '.join(dependents)}")
    features[feature_id]["enabled"] = enabled


def create_feature(features: dict, feature_id: str) -> None:
    if not FEATURE_ID.match(feature_id):
        raise ValueError(
            "Feature ids must start with a lowercase letter and contain only lowercase letters, digits, and underscores"
        )
    if feature_id in features:
        raise ValueError(f"Feature {feature_id} already exists")
    module_path = ROOT / "apps" / "api" / "app" / "modules" / f"{feature_id}.py"
    component_path = (
        ROOT / "apps" / "web" / "src" / "features" / feature_id / f"{feature_id.title().replace('_', '')}.tsx"
    )
    if module_path.exists() or component_path.exists():
        raise ValueError("Refusing to overwrite an existing module or frontend feature")
    features[feature_id] = {
        "enabled": False,
        "depends_on": [],
        "description": f"{feature_id.replace('_', ' ').title()} feature",
    }
    module_path.write_text(
        f'from fastapi import APIRouter\n\nFEATURE_FLAGS = ("{feature_id}",)\nrouter = APIRouter(prefix="/{feature_id.replace("_", "-")}", tags=["{feature_id}"])\n\n@router.get("")\ndef get_{feature_id}():\n    return {{"status": "not configured"}}\n',
        encoding="utf-8",
    )
    component_path.parent.mkdir(parents=True, exist_ok=True)
    component_path.write_text(
        f"export function {feature_id.title().replace('_', '')}() {{\n  return <section>{feature_id.replace('_', ' ').title()} is disabled until configured.</section>;\n}}\n",
        encoding="utf-8",
    )


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python scripts/feature.py [list | enable <id> | disable <id> | new <id>]")
    command, *arguments = sys.argv[1:]
    features = load_features()
    try:
        if command == "list" and not arguments:
            list_features(features)
            return
        if command in {"enable", "disable", "new"} and len(arguments) == 1:
            if command == "new":
                create_feature(features, arguments[0])
            else:
                set_feature(features, arguments[0], command == "enable")
            save_features(features)
            print(f"{command.title()}d feature: {arguments[0]}")
            return
        raise ValueError("Invalid command")
    except ValueError as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()
