"""JSON export: the complete bundle, machine-readable."""

from __future__ import annotations

import json
from typing import Any

from apps.api.app.services.exporters.registry import DISCLAIMER, ExportResult, register_exporter
from packages.common.schemas_compat import jsonify


@register_exporter("json", description="Complete evidence bundle as JSON")
def export_json(bundle: dict[str, Any]) -> ExportResult:
    payload = jsonify({**bundle, "disclaimer": DISCLAIMER})
    # Indented and key-sorted so two exports of the same incident diff cleanly,
    # which matters when a bundle is attached to a case file.
    content = json.dumps(payload, indent=2, sort_keys=True, default=str).encode("utf-8")
    return ExportResult(content=content, media_type="application/json", extension="json")
