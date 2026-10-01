"""PDF export: a printable evidence report.

Written as a minimal PDF by hand rather than pulling in a rendering library. The
document is a fixed layout of short text lines, so a few hundred lines of PDF
primitives avoids adding a dependency (and its CVE surface) for one optional format.

If a richer report is ever needed, replacing this file with a ReportLab
implementation is the whole change: the registry contract stays the same.
"""

from __future__ import annotations

from typing import Any

from apps.api.app.services.exporters.registry import DISCLAIMER, ExportResult, register_exporter

#: US Letter at 72 dpi.
PAGE_WIDTH = 612
PAGE_HEIGHT = 792
MARGIN = 54
LINE_HEIGHT = 14
TITLE_SIZE = 16
HEADING_SIZE = 11
BODY_SIZE = 9
#: Characters that fit on a line at BODY_SIZE in Helvetica, approximately.
WRAP_WIDTH = 96


@register_exporter("pdf", description="Printable evidence report as PDF")
def export_pdf(bundle: dict[str, Any]) -> ExportResult:
    incident = bundle.get("incident", {})
    lines = _compose(bundle, incident)
    return ExportResult(
        content=_render(lines),
        media_type="application/pdf",
        extension="pdf",
    )


def _compose(bundle: dict[str, Any], incident: dict[str, Any]) -> list[tuple[str, str]]:
    """Build (style, text) pairs for the document body."""
    lines: list[tuple[str, str]] = [
        ("title", "TraceFleet Incident Evidence Report"),
        ("body", ""),
        ("heading", "Incident"),
    ]
    for label, key in (
        ("Incident ID", "incident_id"),
        ("Vehicle", "vehicle_id"),
        ("Fleet", "fleet_id"),
        ("Type", "incident_type"),
        ("Severity", "severity"),
        ("Status", "status"),
        ("Start (event time)", "start_ts"),
        ("End (event time)", "end_ts"),
        ("Detected at", "detected_at"),
        ("Evidence confidence", "confidence"),
    ):
        lines.append(("body", f"{label}: {incident.get(key, '-')}"))

    lines += [("body", ""), ("heading", "Evidence")]
    for item in bundle.get("evidence", []):
        lines.append(
            (
                "body",
                f"[{item.get('evidence_id', '')[:12]}] {item.get('event_ts', '')} "
                f"{item.get('phase', '')} | {item.get('signal', '')}: "
                f"{item.get('value', '')} (threshold {item.get('threshold', '-')})",
            )
        )
    if not bundle.get("evidence"):
        lines.append(("body", "No evidence rows retained for this incident."))

    lines += [("body", ""), ("heading", "Contributing factors")]
    for factor in bundle.get("factors", []):
        lines.append(
            (
                "body",
                f"{factor.get('factor_type', '')}: score {factor.get('score', '-')}, "
                f"confidence {factor.get('confidence', '-')}, "
                f"{len(factor.get('evidence_ids') or [])} evidence item(s)",
            )
        )
    if not bundle.get("factors"):
        lines.append(("body", "No contributing factors scored."))

    phases = bundle.get("phases") or []
    if phases:
        lines += [("body", ""), ("heading", "Reconstructed phases")]
        for phase in phases:
            lines.append(
                (
                    "body",
                    f"{phase.get('name', '')}: {phase.get('duration_s', 0)}s, "
                    f"{phase.get('point_count', 0)} telemetry point(s)",
                )
            )

    lines += [("body", ""), ("heading", "Data quality")]
    quality = bundle.get("data_quality") or {}
    if quality:
        for key, value in quality.items():
            lines.append(("body", f"{key}: {value}"))
    else:
        lines.append(("body", "Not recorded."))

    summary = bundle.get("ai_summary")
    if summary:
        lines += [("body", ""), ("heading", "AI narrative (draft, human review required)")]
        for section in summary.get("summary_sections", []):
            lines.append(("body", f"-- {section.get('heading', '')}"))
            for statement in section.get("statements", []):
                citations = ", ".join(statement.get("evidence_ids", []))
                lines.append(("body", f"  [{statement.get('type', '')}] {statement.get('text', '')} ({citations})"))
        if summary.get("uncertainty"):
            lines.append(("body", f"Uncertainty: {summary['uncertainty']}"))
        if summary.get("limitations"):
            lines.append(("body", f"Limitations: {summary['limitations']}"))

    lines += [
        ("body", ""),
        ("heading", "Provenance"),
        ("body", f"Exported at: {bundle.get('exported_at', '-')}"),
        ("body", f"Exported by: {bundle.get('exported_by', '-')}"),
        ("body", ""),
        ("heading", "Disclaimer"),
        ("body", DISCLAIMER),
    ]
    return lines


def _render(lines: list[tuple[str, str]]) -> bytes:
    """Assemble a multi-page PDF from the composed lines."""
    pages: list[list[tuple[str, str]]] = []
    current: list[tuple[str, str]] = []
    usable_height = PAGE_HEIGHT - (2 * MARGIN)
    max_lines = int(usable_height // LINE_HEIGHT)

    for style, text in lines:
        for wrapped in _wrap(text):
            if len(current) >= max_lines:
                pages.append(current)
                current = []
            current.append((style, wrapped))
    if current:
        pages.append(current)
    if not pages:
        pages = [[("body", "No content.")]]

    objects: list[bytes] = []
    font_object = 3 + (len(pages) * 2)

    # 1: catalog, 2: pages tree, then page/content pairs, then the font.
    kids = " ".join(f"{3 + (index * 2)} 0 R" for index in range(len(pages)))
    objects.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    objects.append(f"<< /Type /Pages /Count {len(pages)} /Kids [{kids}] >>".encode("ascii"))

    for index, page_lines in enumerate(pages):
        content = _content_stream(page_lines)
        page_object = 3 + (index * 2)
        objects.append(
            (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {PAGE_WIDTH} {PAGE_HEIGHT}] "
                f"/Resources << /Font << /F1 {font_object} 0 R >> >> "
                f"/Contents {page_object + 1} 0 R >>"
            ).encode("ascii")
        )
        objects.append(b"<< /Length " + str(len(content)).encode("ascii") + b" >>\nstream\n" + content + b"\nendstream")

    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    out = bytearray(b"%PDF-1.4\n")
    offsets: list[int] = []
    for number, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n".encode("ascii") + body + b"\nendobj\n"

    xref_offset = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode("ascii")
    out += b"0000000000 65535 f \n"
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode("ascii")
    out += (f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n").encode("ascii")
    return bytes(out)


def _content_stream(page_lines: list[tuple[str, str]]) -> bytes:
    parts = [b"BT\n"]
    y = PAGE_HEIGHT - MARGIN
    for style, text in page_lines:
        size = {"title": TITLE_SIZE, "heading": HEADING_SIZE}.get(style, BODY_SIZE)
        parts.append(f"/F1 {size} Tf\n".encode("ascii"))
        parts.append(f"1 0 0 1 {MARGIN} {y} Tm\n".encode("ascii"))
        parts.append(b"(" + _escape(text) + b") Tj\n")
        y -= LINE_HEIGHT
    parts.append(b"ET")
    return b"".join(parts)


def _escape(text: str) -> bytes:
    """Escape text for a PDF string literal.

    Backslash, parentheses, and non-Latin-1 characters all break a raw string
    literal, so they are escaped or replaced rather than emitted.
    """
    escaped = (
        str(text).replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)").replace("\n", " ").replace("\r", " ")
    )
    return escaped.encode("latin-1", errors="replace")


def _wrap(text: str) -> list[str]:
    """Wrap on word boundaries, hard-splitting anything longer than a line."""
    if not text:
        return [""]
    words = str(text).split()
    if not words:
        return [""]
    lines: list[str] = []
    current = ""
    for word in words:
        while len(word) > WRAP_WIDTH:
            if current:
                lines.append(current)
                current = ""
            lines.append(word[:WRAP_WIDTH])
            word = word[WRAP_WIDTH:]
        candidate = f"{current} {word}".strip()
        if len(candidate) <= WRAP_WIDTH:
            current = candidate
        else:
            lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines
