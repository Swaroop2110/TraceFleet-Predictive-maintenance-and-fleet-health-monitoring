"""Deterministic post-validation of a generated summary.

Mandatory, and independent of the model. The prompt asks for correct behaviour;
this decides whether the output is publishable. Four checks, each targeting a
specific failure mode:

1. **Citation existence.** Every cited id must be in the supplied package. Catches a
   fabricated reference — the most convincing kind of hallucination, because a
   plausible-looking id makes an invented claim look verified.

2. **Numeric grounding.** Every number in a statement must match a value in the
   cited evidence, within tolerance. Catches the subtler failure where the citation
   is real but the figure attached to it is wrong.

3. **Forbidden language.** Fault, blame, liability, and driver-state assertions are
   rejected outright. The platform does not determine liability, and a narrative
   that implies it becomes a legal problem regardless of any disclaimer.

4. **Uncited material claims.** A statement asserting something about the incident
   with no citation is removed.

Rejected statements are stripped, not silently corrected — a repaired statement
would be the validator's claim rather than the model's, and the audit trail records
exactly what was removed and why. If too much fails, the whole summary is withheld:
a mostly-rejected narrative is less useful than an honest "evidence insufficient".
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from apps.api.app.services.ai.registry import Section, Statement, SummaryResult
from packages.common import logging as tf_logging
from packages.common import metrics

LOGGER = tf_logging.get_logger(__name__)

#: Terms that assign fault, assert liability, or speculate about a person.
#: Word-boundary matched so "faulty sensor" is not caught by "fault".
FORBIDDEN_TERMS: tuple[str, ...] = (
    "fault",
    "at fault",
    "blame",
    "blameworthy",
    "liable",
    "liability",
    "negligent",
    "negligence",
    "responsible for",
    "caused the accident",
    "culpable",
    "guilty",
    "reckless",
    "recklessly",
    "careless",
    "carelessly",
    "dangerous driving",
    "drunk",
    "intoxicated",
    "impaired",
    "under the influence",
    "fatigued",
    "asleep",
    "distracted",
    "inattentive",
    "texting",
    "on the phone",
    "illegal",
    "unlawful",
    "violation",
    "broke the law",
    "criminal",
    "should be disciplined",
    "should be fired",
    "incompetent",
)

#: Numeric tolerance for a cited value. Generous enough for sensible rounding
#: (108.0 reported as 108), tight enough to catch a materially different figure.
NUMERIC_TOLERANCE_RATIO = 0.02
NUMERIC_TOLERANCE_ABSOLUTE = 0.51

#: Below this proportion of surviving statements the summary is withheld.
MIN_SURVIVING_RATIO = 0.5
MIN_SURVIVING_STATEMENTS = 2

#: Numbers in text, including decimals and thousands separators.
_NUMBER_PATTERN = re.compile(r"(?<![\w.])(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)(?![\w])")
#: Evidence references, e.g. E1 or [E-3].
_CITATION_PATTERN = re.compile(r"\[?E-?(\d+)\]?")

#: Words that make a sentence a material claim rather than a framing remark.
_ASSERTIVE_HINTS = (
    "recorded",
    "measured",
    "reached",
    "exceeded",
    "detected",
    "showed",
    "shows",
    "indicates",
    "indicated",
    "was",
    "were",
    "dropped",
    "increased",
    "decreased",
    "occurred",
    "reported",
    "registered",
    "km/h",
    "m/s",
    "%",
    "threshold",
)

#: Figures that are almost always structural, not measurements.
_STRUCTURAL_NUMBERS = {0.0, 1.0, 2.0, 3.0, 4.0, 5.0}


@dataclass
class ValidationResult:
    passed: bool
    statements_total: int = 0
    statements_accepted: int = 0
    statements_rejected: int = 0
    rejections: list[dict[str, Any]] = field(default_factory=list)
    cited_evidence_ids: list[str] = field(default_factory=list)
    unknown_citations: list[str] = field(default_factory=list)
    withheld_reason: str | None = None
    checks: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "statements_total": self.statements_total,
            "statements_accepted": self.statements_accepted,
            "statements_rejected": self.statements_rejected,
            "rejections": self.rejections,
            "cited_evidence_ids": self.cited_evidence_ids,
            "unknown_citations": self.unknown_citations,
            "withheld_reason": self.withheld_reason,
            "checks": self.checks,
            "evidence_traceable": self.statements_rejected == 0 and self.passed,
        }


def validate(
    result: SummaryResult,
    package: dict[str, Any],
    label_map: dict[str, str],
) -> tuple[SummaryResult, ValidationResult]:
    """Validate and filter a summary. Returns the filtered result and the report."""
    known_labels = {item["evidence_id"] for item in package.get("timeline", [])}
    values_by_label = _values_by_label(package)
    package_values = _package_level_values(package)

    report = ValidationResult(passed=True)
    counts = {
        "unknown_citation": 0,
        "uncited_claim": 0,
        "forbidden_language": 0,
        "ungrounded_number": 0,
    }
    cited: set[str] = set()
    unknown: set[str] = set()

    kept_sections: list[Section] = []
    for section in result.sections:
        kept_statements: list[Statement] = []
        for statement in section.statements:
            report.statements_total += 1
            labels = _normalise_citations(statement, known_labels, unknown)
            failure = _check_statement(statement, labels, known_labels, values_by_label, package_values, counts)

            if failure is None:
                statement.evidence_ids = [label_map.get(label, label) for label in labels]
                cited.update(labels)
                kept_statements.append(statement)
                report.statements_accepted += 1
                continue

            report.statements_rejected += 1
            report.rejections.append(
                {
                    "text": statement.text[:300],
                    "type": statement.kind,
                    "reason": failure["reason"],
                    "detail": failure["detail"],
                }
            )
            metrics.ai_validation_failures_total.labels(reason=failure["reason"]).inc()

        if kept_statements:
            kept_sections.append(Section(heading=_clean_heading(section.heading), statements=kept_statements))

    result.sections = kept_sections
    result.uncertainty = _scrub_free_text(result.uncertainty, counts)
    result.limitations = _scrub_free_text(result.limitations, counts)

    report.checks = counts
    report.cited_evidence_ids = sorted(label_map.get(label, label) for label in cited)
    report.unknown_citations = sorted(unknown)

    _apply_threshold(result, report)

    LOGGER.info(
        "ai summary validated",
        extra={
            "incident_id": package.get("incident_id"),
            "provider": result.provider,
            "statements_total": report.statements_total,
            "statements_accepted": report.statements_accepted,
            "statements_rejected": report.statements_rejected,
            "checks": counts,
            "passed": report.passed,
        },
    )
    return result, report


def _apply_threshold(result: SummaryResult, report: ValidationResult) -> None:
    """Withhold the summary if too little survived."""
    if report.statements_total == 0:
        report.passed = False
        report.withheld_reason = "the provider returned no statements"
    elif report.statements_accepted < MIN_SURVIVING_STATEMENTS:
        report.passed = False
        report.withheld_reason = (
            f"only {report.statements_accepted} statement(s) passed validation; "
            "summary unavailable — evidence insufficient"
        )
    elif report.statements_accepted / report.statements_total < MIN_SURVIVING_RATIO:
        report.passed = False
        report.withheld_reason = (
            f"{report.statements_rejected} of {report.statements_total} statements failed "
            "validation; summary unavailable — evidence insufficient"
        )

    if not report.passed:
        # Withheld entirely. A narrative assembled from the fragments that happened
        # to pass would read as authoritative while omitting whatever was rejected.
        result.sections = []
        result.available = False
        result.unavailable_reason = report.withheld_reason


def _check_statement(
    statement: Statement,
    labels: list[str],
    known_labels: set[str],
    values_by_label: dict[str, list[float]],
    package_values: set[float],
    counts: dict[str, int],
) -> dict[str, str] | None:
    """Run every check against one statement. Returns the first failure, or None."""
    text = statement.text.strip()
    if not text:
        return {"reason": "empty_statement", "detail": "statement text was empty"}

    term = _forbidden_term(text)
    if term:
        counts["forbidden_language"] += 1
        return {"reason": "forbidden_language", "detail": f"contains prohibited term {term!r}"}

    invalid = [label for label in labels if label not in known_labels]
    if invalid:
        counts["unknown_citation"] += 1
        return {
            "reason": "unknown_citation",
            "detail": f"cites evidence not in the package: {', '.join(invalid)}",
        }

    if not labels and _is_material_claim(text):
        counts["uncited_claim"] += 1
        return {"reason": "uncited_claim", "detail": "asserts a fact about the incident with no citation"}

    ungrounded = _ungrounded_numbers(text, labels, values_by_label, package_values)
    if ungrounded:
        counts["ungrounded_number"] += 1
        return {
            "reason": "ungrounded_number",
            "detail": f"value(s) {ungrounded} do not appear in the cited evidence",
        }

    return None


def _normalise_citations(
    statement: Statement,
    known_labels: set[str],
    unknown: set[str],
) -> list[str]:
    """Collect citations from the structured field and from inline ``[E1]`` markers.

    Both are read because models commonly do one, the other, or both, and a
    citation present only inline is still a citation the operator can follow.
    """
    labels: list[str] = []
    for raw in statement.evidence_ids or []:
        label = _canonical_label(str(raw))
        if label:
            labels.append(label)
    for match in _CITATION_PATTERN.finditer(statement.text):
        label = f"E{int(match.group(1))}"
        if label not in labels:
            labels.append(label)
    for label in labels:
        if label not in known_labels:
            unknown.add(label)
    return labels


def _canonical_label(value: str) -> str | None:
    match = _CITATION_PATTERN.fullmatch(value.strip())
    return f"E{int(match.group(1))}" if match else None


def _values_by_label(package: dict[str, Any]) -> dict[str, list[float]]:
    """Numbers legitimately associated with each evidence label."""
    values: dict[str, list[float]] = {}
    for item in package.get("timeline", []):
        label = item["evidence_id"]
        bucket: list[float] = []
        for key in ("value", "threshold"):
            if item.get(key) is not None:
                bucket.append(float(item[key]))
        for flag_value in (item.get("flags") or {}).values():
            if isinstance(flag_value, (int, float)) and not isinstance(flag_value, bool):
                bucket.append(float(flag_value))
        values[label] = bucket

    for factor in package.get("factors", []):
        for label in factor.get("evidence_ids", []):
            if label in values:
                for key in ("score", "confidence"):
                    if factor.get(key) is not None:
                        values[label].append(float(factor[key]))
    return values


def _package_level_values(package: dict[str, Any]) -> set[float]:
    """Numbers stated by the package itself rather than by one evidence item.

    Data-quality counts, the overall confidence, and the size of the evidence set
    are all supplied facts. A statement such as "7 duplicate events were suppressed"
    is properly grounded even though 7 is not a sensor reading, so these are added to
    the allowed set. Without them the check rejects correct, useful prose — which is
    worse than not checking, because it teaches the reader to ignore rejections.
    """
    allowed: set[float] = set()

    for value in (package.get("data_quality") or {}).values():
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            allowed.add(float(value))
        elif isinstance(value, list):
            allowed.add(float(len(value)))

    for key in ("confidence", "evidence_count", "total_evidence_available"):
        if isinstance(package.get(key), (int, float)):
            allowed.add(float(package[key]))

    timeline = package.get("timeline", [])
    allowed.add(float(len(timeline)))
    allowed.add(float(len({item.get("signal") for item in timeline})))
    allowed.add(float(len({item.get("phase") for item in timeline if item.get("phase")})))

    factors = package.get("factors", [])
    allowed.add(float(len(factors)))
    for factor in factors:
        for key in ("score", "confidence"):
            if isinstance(factor.get(key), (int, float)):
                allowed.add(float(factor[key]))

    return allowed


def _ungrounded_numbers(
    text: str,
    labels: list[str],
    values_by_label: dict[str, list[float]],
    package_values: set[float],
) -> list[float]:
    """Numbers in the text that match nothing in the supplied package.

    Checked against the cited evidence first and the package's own figures second.
    The rule being enforced is "every number traces to something supplied", not
    "every number is a sensor reading".
    """
    allowed: list[float] = list(package_values)
    for label in labels:
        allowed.extend(values_by_label.get(label, []))
    if not allowed:
        return []

    ungrounded: list[float] = []
    for match in _NUMBER_PATTERN.finditer(text):
        raw = match.group(1).replace(",", "")
        try:
            number = float(raw)
        except ValueError:
            continue
        # Small integers are usually counts, ordinals, or list positions rather
        # than measurements; flagging them would reject correct prose.
        if number in _STRUCTURAL_NUMBERS:
            continue
        if _looks_like_timestamp(text, match.start()):
            continue
        if not any(_close_enough(number, candidate) for candidate in allowed):
            ungrounded.append(number)
    return ungrounded


def _close_enough(value: float, candidate: float) -> bool:
    tolerance = max(NUMERIC_TOLERANCE_ABSOLUTE, abs(candidate) * NUMERIC_TOLERANCE_RATIO)
    return abs(value - candidate) <= tolerance


def _looks_like_timestamp(text: str, position: int) -> bool:
    """Whether a number sits inside an ISO timestamp.

    Timestamps are copied verbatim from the package and would otherwise be parsed
    as a stream of unexplained numbers.
    """
    window = text[max(0, position - 12) : position + 12]
    return bool(re.search(r"\d{4}-\d{2}-\d{2}|\d{2}:\d{2}", window))


def _forbidden_term(text: str) -> str | None:
    lowered = text.lower()
    for term in FORBIDDEN_TERMS:
        if re.search(rf"(?<!\w){re.escape(term)}(?!\w)", lowered):
            return term
    return None


def _is_material_claim(text: str) -> bool:
    """Whether an uncited statement asserts something needing support."""
    lowered = text.lower()
    if _NUMBER_PATTERN.search(text):
        return True
    return any(hint in lowered for hint in _ASSERTIVE_HINTS)


def _scrub_free_text(text: str, counts: dict[str, int]) -> str:
    """Remove prohibited language from the uncertainty and limitations prose.

    These fields are narrative rather than claims, so they are scrubbed rather than
    rejected — but they must not be a way around the language rules.
    """
    if not text:
        return ""
    term = _forbidden_term(text)
    if term is None:
        return text.strip()
    counts["forbidden_language"] += 1
    metrics.ai_validation_failures_total.labels(reason="forbidden_language_free_text").inc()
    return (
        "Wording removed by the output validator because it implied fault or "
        "liability, which this platform does not assess."
    )


def _clean_heading(heading: str) -> str:
    cleaned = re.sub(r"[^\w\s\-/()]", "", str(heading)).strip()
    return cleaned[:120] or "Summary"


def required_disclaimer() -> str:
    """Banner the UI must show alongside any generated narrative."""
    return (
        "AI-generated draft — human review required. Statements are derived from "
        "detected telemetry signals only. This platform does not determine fault, "
        "liability, or cause."
    )
