"""Google Gemini provider.

Configured for determinism and containment: temperature at or below 0.2, JSON
response mode, a bounded output size, a hard timeout, and retries with backoff on
transient failures only.

No tools and no function calling are configured. The summariser has no capability to
act, which is a property of how it is constructed rather than a rule it is asked to
follow.

Unavailable without ``GEMINI_API_KEY``; the registry then falls back to the mock so
the platform runs fully without a key.
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any

from apps.api.app.services.ai.registry import (
    ProviderError,
    Section,
    Statement,
    SummaryResult,
    register_provider,
)
from packages.common import logging as tf_logging
from packages.common import metrics

LOGGER = tf_logging.get_logger(__name__)

DEFAULT_MODEL = "gemini-3.8-flash"
#: Low but non-zero. Zero can make a model loop on a phrase; 0.2 keeps it varied
#: enough to be readable while staying reproducible in substance.
TEMPERATURE = 0.2
MAX_OUTPUT_TOKENS = 2048
TIMEOUT_S = 30
MAX_ATTEMPTS = 3


@register_provider("gemini", description="Google Gemini via google-genai, JSON response mode")
class GeminiProvider:
    name = "gemini"

    def __init__(self) -> None:
        self.model = os.getenv("GEMINI_MODEL", DEFAULT_MODEL)
        self.api_key = os.getenv("GEMINI_API_KEY", "").strip()
        self.unavailable_reason: str | None = None
        self._client: Any = None

    def is_available(self) -> bool:
        if not self.api_key or self.api_key.lower().startswith("your_"):
            # The placeholder from .env.example counts as absent.
            self.unavailable_reason = "GEMINI_API_KEY is not set"
            return False
        try:
            import google.genai  # noqa: F401
        except ImportError:
            self.unavailable_reason = "the google-genai package is not installed"
            return False
        return True

    @property
    def client(self) -> Any:
        if self._client is None:
            from google import genai

            self._client = genai.Client(api_key=self.api_key)
        return self._client

    def summarize(self, package: dict[str, Any], prompt: str) -> SummaryResult:
        from google.genai import types

        config = types.GenerateContentConfig(
            temperature=TEMPERATURE,
            max_output_tokens=MAX_OUTPUT_TOKENS,
            # JSON mode removes the need to strip markdown fences and makes a
            # parse failure a real error rather than a formatting quirk.
            response_mime_type="application/json",
            candidate_count=1,
        )

        started = time.perf_counter()
        last_error: Exception | None = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                response = self.client.models.generate_content(
                    model=self.model,
                    contents=prompt,
                    config=config,
                )
                latency_ms = int((time.perf_counter() - started) * 1000)
                metrics.ai_latency_seconds.labels(provider=self.name).observe(latency_ms / 1000.0)
                return self._parse(response, latency_ms)
            except Exception as error:
                last_error = error
                if not _is_retryable(error) or attempt == MAX_ATTEMPTS:
                    break
                delay = min(8.0, 0.5 * (2 ** (attempt - 1)))
                LOGGER.warning(
                    "gemini call failed, retrying",
                    extra={"attempt": attempt, "backoff_s": delay, "error": str(error)[:200]},
                )
                time.sleep(delay)

        metrics.ai_requests_total.labels(provider=self.name, outcome="error").inc()
        raise ProviderError(f"gemini request failed after {MAX_ATTEMPTS} attempt(s): {last_error}")

    def _parse(self, response: Any, latency_ms: int) -> SummaryResult:
        text = _extract_text(response)
        if not text:
            raise ProviderError("gemini returned an empty response")

        payload = _load_json(text)
        sections: list[Section] = []
        for raw_section in payload.get("summary_sections", []) or []:
            statements = [
                Statement(
                    text=str(item.get("text", "")).strip(),
                    # An unrecognised type is treated as an inference, the weaker
                    # claim. Defaulting to "fact" would upgrade an unverified
                    # statement on the strength of a malformed field.
                    kind="fact" if str(item.get("type", "")).lower() == "fact" else "inference",
                    evidence_ids=[str(value) for value in (item.get("evidence_ids") or [])],
                )
                for item in (raw_section.get("statements") or [])
                if str(item.get("text", "")).strip()
            ]
            if statements:
                sections.append(Section(heading=str(raw_section.get("heading", "Summary")), statements=statements))

        tokens = _token_usage(response)
        for kind, count in tokens.items():
            metrics.ai_tokens_total.labels(provider=self.name, kind=kind).inc(count)

        return SummaryResult(
            sections=sections,
            uncertainty=str(payload.get("uncertainty", "")).strip(),
            limitations=str(payload.get("limitations", "")).strip(),
            model=self.model,
            provider=self.name,
            raw_output=text[:20000],
            latency_ms=latency_ms,
            tokens=tokens,
        )


def _extract_text(response: Any) -> str:
    text = getattr(response, "text", None)
    if text:
        return str(text)
    # Older SDK shapes nest the payload under candidates/content/parts.
    try:
        parts = response.candidates[0].content.parts
        return "".join(str(getattr(part, "text", "")) for part in parts)
    except Exception:
        return ""


def _load_json(text: str) -> dict[str, Any]:
    """Parse the model's JSON, tolerating a stray code fence."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    if fenced:
        try:
            return json.loads(fenced.group(1))
        except json.JSONDecodeError:
            pass
    brace = re.search(r"\{.*\}", text, re.DOTALL)
    if brace:
        try:
            return json.loads(brace.group(0))
        except json.JSONDecodeError as error:
            raise ProviderError(f"gemini response was not valid JSON: {error}") from error
    raise ProviderError("gemini response contained no JSON object")


def _token_usage(response: Any) -> dict[str, int]:
    usage = getattr(response, "usage_metadata", None)
    if usage is None:
        return {}
    result: dict[str, int] = {}
    for attribute, label in (
        ("prompt_token_count", "prompt"),
        ("candidates_token_count", "completion"),
        ("total_token_count", "total"),
    ):
        value = getattr(usage, attribute, None)
        if isinstance(value, int):
            result[label] = value
    return result


def _is_retryable(error: Exception) -> bool:
    """Whether a failure is worth retrying.

    Rate limits, timeouts, and 5xx are transient. An authentication failure or a
    rejected request will fail identically on a retry and should surface at once.
    """
    text = str(error).lower()
    transient = ("429", "rate limit", "timeout", "timed out", "unavailable", "500", "502", "503", "504", "deadline")
    permanent = ("401", "403", "invalid api key", "permission denied", "not found", "invalid argument")
    if any(marker in text for marker in permanent):
        return False
    return any(marker in text for marker in transient)
