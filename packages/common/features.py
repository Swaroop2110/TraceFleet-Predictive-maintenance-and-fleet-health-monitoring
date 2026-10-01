"""Feature configuration used by the API, processor, and developer tooling."""

import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


class Features:
    def __init__(self, config_path: str | Path | None = None):
        project_root = Path(__file__).resolve().parents[2]
        configured_path = config_path or os.getenv("FEATURES_CONFIG")
        self.config_path = Path(configured_path) if configured_path else project_root / "config" / "features.yaml"
        self._features: dict[str, Any] = {}
        self.load()

    def load(self) -> None:
        if not self.config_path.exists():
            return
        with self.config_path.open("r", encoding="utf-8") as file:
            data = yaml.safe_load(file) or {}
        self._features = data.get("features", {})
        self._validate_dependencies()

    def _validate_dependencies(self) -> None:
        for feature_id, details in self._features.items():
            if details.get("enabled", False):
                for dep in details.get("depends_on", []):
                    dep_feature = self._features.get(dep, {})
                    if not dep_feature.get("enabled", False):
                        raise RuntimeError(f"Feature '{feature_id}' depends on disabled feature '{dep}'")

    def is_enabled(self, feature_id: str) -> bool:
        feature = self._features.get(feature_id, {})
        return feature.get("enabled", False)

    def get_all(self) -> dict[str, Any]:
        return deepcopy(self._features)

    def reload(self) -> None:
        self.load()


feature_registry = Features()
