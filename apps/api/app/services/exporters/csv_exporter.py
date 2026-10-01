"""CSV export: evidence rows for a spreadsheet."""

from __future__ import annotations

import csv
import io
from typing import Any

from apps.api.app.services.exporters.registry import DISCLAIMER, ExportResult, register_exporter

EVIDENCE_COLUMNS = (
    "evidence_id",
    "event_ts",
    "phase",
    "signal",
    "value",
    "threshold",
    "weight",
)
FACTOR_COLUMNS = ("factor_type", "score", "confidence", "evidence_ids")


@register_exporter("csv", description="Evidence and factor rows as CSV")
def export_csv(bundle: dict[str, Any]) -> ExportResult:
    buffer = io.StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    incident = bundle.get("incident", {})

    # Incident context first. A bare evidence table detached from its incident is
    # ambiguous once the file is out of the platform.
    writer.writerow(["# TraceFleet evidence export"])
    writer.writerow(["# " + DISCLAIMER])
    writer.writerow([])
    writer.writerow(["incident_id", incident.get("incident_id", "")])
    writer.writerow(["vehicle_id", incident.get("vehicle_id", "")])
    writer.writerow(["fleet_id", incident.get("fleet_id", "")])
    writer.writerow(["incident_type", incident.get("incident_type", "")])
    writer.writerow(["severity", incident.get("severity", "")])
    writer.writerow(["status", incident.get("status", "")])
    writer.writerow(["start_ts", incident.get("start_ts", "")])
    writer.writerow(["end_ts", incident.get("end_ts", "")])
    writer.writerow(["detected_at", incident.get("detected_at", "")])
    writer.writerow(["confidence", incident.get("confidence", "")])
    writer.writerow(["exported_at", bundle.get("exported_at", "")])
    writer.writerow(["exported_by", bundle.get("exported_by", "")])
    writer.writerow([])

    writer.writerow(["# EVIDENCE"])
    writer.writerow(list(EVIDENCE_COLUMNS))
    for item in bundle.get("evidence", []):
        writer.writerow([_cell(item.get(column)) for column in EVIDENCE_COLUMNS])
    writer.writerow([])

    writer.writerow(["# CONTRIBUTING FACTORS"])
    writer.writerow(list(FACTOR_COLUMNS))
    for factor in bundle.get("factors", []):
        row = [_cell(factor.get(column)) for column in FACTOR_COLUMNS[:-1]]
        row.append(";".join(factor.get("evidence_ids") or []))
        writer.writerow(row)
    writer.writerow([])

    writer.writerow(["# DATA QUALITY"])
    for key, value in (bundle.get("data_quality") or {}).items():
        writer.writerow([key, _cell(value)])

    phases = bundle.get("phases") or []
    if phases:
        writer.writerow([])
        writer.writerow(["# TIMELINE PHASES"])
        writer.writerow(["name", "start_ts", "end_ts", "duration_s", "point_count"])
        for phase in phases:
            writer.writerow(
                [
                    phase.get("name", ""),
                    phase.get("start_ts", ""),
                    phase.get("end_ts", ""),
                    _cell(phase.get("duration_s")),
                    _cell(phase.get("point_count")),
                ]
            )

    # utf-8-sig: Excel misreads plain UTF-8 CSV, and these files get opened in Excel.
    return ExportResult(
        content=buffer.getvalue().encode("utf-8-sig"),
        media_type="text/csv",
        extension="csv",
    )


def _cell(value: Any) -> str:
    """Render a value, neutralising spreadsheet formula injection.

    A field beginning with =, +, -, or @ is executed as a formula by Excel and
    Sheets. Prefixing with a single quote makes it literal text. The values here are
    engine-generated, but an export is exactly the path where an untrusted string
    would reach a spreadsheet.
    """
    if value is None:
        return ""
    if isinstance(value, (list, tuple)):
        return ";".join(str(item) for item in value)
    if isinstance(value, dict):
        import json

        text = json.dumps(value, sort_keys=True, default=str)
    else:
        text = str(value)
    if text[:1] in ("=", "+", "-", "@", "\t", "\r"):
        return f"'{text}"
    return text
