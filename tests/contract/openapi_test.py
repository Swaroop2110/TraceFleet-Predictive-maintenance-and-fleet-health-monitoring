from apps.api.app.main import app


def test_openapi_exposes_enabled_dynamic_modules_and_core_contract():
    paths = app.openapi()["paths"]
    assert "/api/v1/health" in paths
    assert "/api/v1/vehicles" in paths
    assert "/api/v1/incidents" in paths
    assert "/api/v1/incidents/{incident_id}/ai-summary" in paths
    assert "/api/v1/analytics/fleet" in paths
