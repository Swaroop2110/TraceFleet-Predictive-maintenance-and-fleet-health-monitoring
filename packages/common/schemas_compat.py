"""Small conversions between database and JSON representations.

``Numeric`` columns come back from SQLAlchemy as :class:`~decimal.Decimal`, which
``json.dumps`` refuses to serialise. Converting at the boundary — once, here —
avoids either sprinkling ``float()`` through every serialiser or installing a
custom JSON encoder that would silently coerce values elsewhere.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID


def decimal_to_float(value: Any) -> float | None:
    """Convert a Decimal (or anything numeric) to float, preserving None."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def json_value(value: Any) -> Any:
    """Coerce one value into something JSON-serialisable."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def jsonify(payload: Any) -> Any:
    """Recursively coerce a structure into JSON-serialisable form."""
    if isinstance(payload, dict):
        return {key: jsonify(value) for key, value in payload.items()}
    if isinstance(payload, (list, tuple)):
        return [jsonify(item) for item in payload]
    return json_value(payload)
