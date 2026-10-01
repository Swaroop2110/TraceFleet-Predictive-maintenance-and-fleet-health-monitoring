"""Deterministic provider used for tests and for running without an API key.

Composes the narrative directly from the evidence package. Because it is
deterministic it makes the AI panel demonstrable on a fresh clone, and it gives the
validator a known-good input so a validator regression is distinguishable from a
model regression.

It is written to be a *well-behaved* provider: every statement cites real evidence,
numbers come from the package, fact and inference are separated, and no fault
language appears. The adversarial cases live in ``tests/ai_safety`` where a hostile
provider is simulated deliberately.
"""

from __future__ import annotations

from typing import Any

from apps.api.app.services.ai.registry import Section, Statement, SummaryResult, register_provider


@register_provider("mock", description="Deterministic evidence-derived narrative, no external calls")
class MockProvider:
    name = "mock"
    model = "tracefleet-deterministic-v2"
    unavailable_reason = None

    def is_available(self) -> bool:
        return True

    def summarize(self, package: dict[str, Any], prompt: str) -> SummaryResult:
        timeline = package.get("timeline", [])
        factors = package.get("factors", [])
        quality = package.get("data_quality", {})

        sections: list[Section] = [
            self._detection_section(package, timeline),
            self._sequence_section(timeline),
        ]
        if factors:
            sections.append(self._factors_section(factors, timeline))
        sections.append(self._quality_section(quality, package))

        return SummaryResult(
            sections=[section for section in sections if section.statements],
            uncertainty=self._uncertainty(package, timeline),
            limitations=self._limitations(quality, package),
            model=self.model,
            provider=self.name,
            raw_output="(generated locally from the evidence package)",
            tokens={},
        )

    # -- sections --------------------------------------------------------
    def _detection_section(self, package: dict[str, Any], timeline: list[dict[str, Any]]) -> Section:
        statements: list[Statement] = []
        all_labels = [item["evidence_id"] for item in timeline]

        if timeline:
            distinct = sorted({item["signal"] for item in timeline})
            statements.append(
                Statement(
                    text=(
                        f"The engine recorded {len(timeline)} evidence item(s) across "
                        f"{len(distinct)} distinct signal type(s) for vehicle "
                        f"{package.get('vehicle_id')}."
                    ),
                    kind="fact",
                    evidence_ids=all_labels,
                )
            )
            statements.append(
                Statement(
                    text=(
                        f"The incident was classified {package.get('severity')} and typed "
                        f"{package.get('incident_type')} by the configured severity matrix."
                    ),
                    kind="fact",
                    evidence_ids=all_labels,
                )
            )

        trigger = [item for item in timeline if item.get("phase") == "trigger"]
        if trigger:
            names = sorted({item["signal"] for item in trigger})
            statements.append(
                Statement(
                    text=f"The trigger phase contains {', '.join(names)}.",
                    kind="fact",
                    evidence_ids=[item["evidence_id"] for item in trigger],
                )
            )
        return Section(heading="What was detected", statements=statements)

    def _sequence_section(self, timeline: list[dict[str, Any]]) -> Section:
        statements: list[Statement] = []
        for item in timeline:
            value = item.get("value")
            threshold = item.get("threshold")
            unit = item.get("unit") or ""
            if value is None:
                continue
            # Formatted from the package values only, so the validator's numeric
            # check has something to match.
            if threshold is not None:
                text = (
                    f"During the {item.get('phase') or 'incident'} phase, "
                    f"{item['signal']} measured {_fmt(value)} {unit} against a "
                    f"configured threshold of {_fmt(threshold)} {unit}."
                ).replace("  ", " ")
            else:
                text = (
                    f"During the {item.get('phase') or 'incident'} phase, "
                    f"{item['signal']} measured {_fmt(value)} {unit}."
                ).replace("  ", " ")
            statements.append(Statement(text=text.strip(), kind="fact", evidence_ids=[item["evidence_id"]]))
        return Section(heading="Sequence of events", statements=statements)

    def _factors_section(
        self,
        factors: list[dict[str, Any]],
        package_timeline: list[dict[str, Any]],
    ) -> Section:
        statements: list[Statement] = []
        for factor in factors:
            score = factor.get("score")
            confidence = factor.get("confidence")
            if score is None:
                continue
            citations = factor.get("evidence_ids") or self._labels_for_signal(
                package_timeline, factor.get("type_key", "")
            )
            if not citations:
                continue
            statements.append(
                Statement(
                    text=(
                        f"{factor['type'].capitalize()} scored {_fmt(score)} with "
                        f"{_fmt(confidence)} confidence in evidence completeness."
                    ),
                    kind="fact",
                    evidence_ids=citations,
                )
            )
        if factors:
            leading = factors[0]
            # Falls back to citing the signals matching this factor when the factor
            # carries no evidence ids. A statement with no citation is removed by the
            # validator, so emitting one would be a self-inflicted rejection.
            citations = leading.get("evidence_ids") or self._labels_for_signal(
                package_timeline, leading.get("type_key", "")
            )
            if citations:
                statements.append(
                    Statement(
                        text=(
                            f"{leading['type'].capitalize()} is the largest scored contributing "
                            f"factor in this evidence set."
                        ),
                        kind="inference",
                        evidence_ids=citations,
                    )
                )
        return Section(heading="Contributing factors", statements=statements)

    @staticmethod
    def _labels_for_signal(timeline: list[dict[str, Any]], signal_key: str) -> list[str]:
        return [item["evidence_id"] for item in timeline if item.get("signal_key") == signal_key]

    def _quality_section(self, quality: dict[str, Any], package: dict[str, Any]) -> Section:
        statements: list[Statement] = []
        labels = [item["evidence_id"] for item in package.get("timeline", [])]
        late = int(quality.get("late_events", 0))
        duplicates = int(quality.get("duplicates_removed", 0))
        out_of_order = int(quality.get("out_of_order_events", 0))
        missing = quality.get("missing_fields") or []

        if not any((late, duplicates, out_of_order, missing)):
            statements.append(
                Statement(
                    text="No late, duplicate, out-of-order, or missing telemetry was recorded for this window.",
                    kind="fact",
                    evidence_ids=labels,
                )
            )
            return Section(heading="Data quality", statements=statements)

        if late:
            statements.append(
                Statement(
                    text=f"{late} event(s) arrived after the configured lateness window and are flagged late.",
                    kind="fact",
                    evidence_ids=labels,
                )
            )
        if duplicates:
            statements.append(
                Statement(
                    text=f"{duplicates} duplicate event(s) were suppressed before detection.",
                    kind="fact",
                    evidence_ids=labels,
                )
            )
        if out_of_order:
            statements.append(
                Statement(
                    text=f"{out_of_order} event(s) were delivered out of event-time order and re-sorted.",
                    kind="fact",
                    evidence_ids=labels,
                )
            )
        if missing:
            statements.append(
                Statement(
                    text=f"The following optional fields were absent from some samples: {', '.join(missing)}.",
                    kind="fact",
                    evidence_ids=labels,
                )
            )
        return Section(heading="Data quality", statements=statements)

    # -- prose -----------------------------------------------------------
    def _uncertainty(self, package: dict[str, Any], timeline: list[dict[str, Any]]) -> str:
        parts = [
            "This account covers only the telemetry signals the configured detectors "
            "raised. Conditions outside the vehicle's own measurements are unknown."
        ]
        confidence = package.get("confidence")
        if confidence is not None:
            parts.append(
                f"Overall evidence confidence is {_fmt(confidence)}, reflecting completeness "
                "of the evidence rather than certainty about events."
            )
        if package.get("evidence_truncated"):
            parts.append(
                f"{package.get('total_evidence_available')} evidence items exist; the "
                f"{len(timeline)} highest-weighted were considered."
            )
        return " ".join(parts)

    def _limitations(self, quality: dict[str, Any], package: dict[str, Any]) -> str:
        parts = [
            "Thresholds are configuration defaults, not real-world safety limits. Severity is a triage classification."
        ]
        if int(quality.get("telemetry_points", 0)) == 0:
            parts.append(
                "The raw telemetry series was unavailable when this summary was produced, "
                "so the account rests on persisted evidence rows alone."
            )
        if quality.get("schema_transformed"):
            parts.append(
                f"{quality['schema_transformed']} event(s) were adapted from an older "
                "schema version and were not read exactly as sent."
            )
        return " ".join(parts)


def _fmt(value: Any) -> str:
    """Format a number without trailing noise, so the validator can match it."""
    if value is None:
        return "unknown"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if number == int(number):
        return str(int(number))
    return f"{number:.2f}".rstrip("0").rstrip(".")
