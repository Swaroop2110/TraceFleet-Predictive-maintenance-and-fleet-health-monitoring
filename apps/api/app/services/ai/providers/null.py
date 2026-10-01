"""Null provider: AI explicitly unavailable.

Selected with ``LLM_PROVIDER=null`` to switch narrative generation off while leaving
the endpoint present. The endpoint answers honestly rather than 404ing, so the UI
can show "unavailable" instead of appearing broken.

It is also what the ``ai_summary`` feature flag being disabled looks like from the
outside, except that the flag removes the route entirely.
"""

from __future__ import annotations

from typing import Any

from apps.api.app.services.ai.registry import SummaryResult, register_provider


@register_provider("null", description="Reports AI summaries as unavailable")
class NullProvider:
    name = "null"
    model = "none"
    unavailable_reason = None

    def is_available(self) -> bool:
        # Always "available" in the sense that it can respond; it simply responds
        # that no narrative can be produced.
        return True

    def summarize(self, package: dict[str, Any], prompt: str) -> SummaryResult:
        return SummaryResult(
            sections=[],
            uncertainty="",
            limitations="",
            model="none",
            provider="null",
            available=False,
            unavailable_reason=(
                "Narrative generation is disabled for this deployment. The "
                "deterministic evidence and contributing-factor scores below are "
                "unaffected and remain the authoritative record."
            ),
        )
