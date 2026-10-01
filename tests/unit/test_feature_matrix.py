from pathlib import Path

import pytest

from packages.common.features import Features


def test_feature_registry_returns_a_copy_and_honours_enabled_state(tmp_path):
    config = tmp_path / "features.yaml"
    config.write_text("features:\n  telemetry: {enabled: true, depends_on: []}\n", encoding="utf-8")
    registry = Features(config)
    features = registry.get_all()
    features["telemetry"]["enabled"] = False
    assert registry.is_enabled("telemetry") is True


def test_feature_registry_rejects_enabled_features_with_disabled_dependencies(tmp_path):
    config = tmp_path / "features.yaml"
    config.write_text(
        "features:\n  core: {enabled: false, depends_on: []}\n  ui: {enabled: true, depends_on: [core]}\n",
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="depends on disabled"):
        Features(config)


@pytest.mark.parametrize(
    "disabled,route",
    [
        ("ai_summary", "/api/v1/incidents/{incident_id}/ai-summary"),
        ("ops_dashboard", "/api/v1/ops/pipeline"),
        ("evidence_export", "/api/v1/incidents/{incident_id}/export"),
    ],
)
def test_disabled_feature_removes_routes_from_openapi(tmp_path, monkeypatch, disabled, route):
    import yaml

    from apps.api.app import main

    monkeypatch.setattr(main, "_sync_redis", lambda: None)
    data = yaml.safe_load(Path("config/features.yaml").read_text())
    data["features"][disabled]["enabled"] = False
    # Disable dependants as well, as the CLI does.
    changed = True
    while changed:
        changed = False
        for details in data["features"].values():
            if details["enabled"] and any(
                not data["features"][dep]["enabled"] for dep in details.get("depends_on", [])
            ):
                details["enabled"] = False
                changed = True
    path = tmp_path / "features.yaml"
    path.write_text(yaml.safe_dump(data))
    app = main.create_app(features=Features(path))
    assert route not in app.openapi()["paths"]
    assert "/api/v1/health" in app.openapi()["paths"]


def test_manifest_dependencies_cannot_be_mutated_through_snapshot(tmp_path):
    file = tmp_path / "features.yaml"
    file.write_text("features:\n  core: {enabled: true, depends_on: []}\n  ui: {enabled: true, depends_on: [core]}\n")
    registry = Features(file)
    registry.get_all()["ui"]["depends_on"].clear()
    assert registry.get_all()["ui"]["depends_on"] == ["core"]
