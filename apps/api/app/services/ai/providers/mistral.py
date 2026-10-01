"""Mistral evidence narratives via bounded, tool-free JSON chat completions."""

from __future__ import annotations

import json
import os
import time
from typing import Any

import httpx

from apps.api.app.services.ai.registry import ProviderError, Section, Statement, SummaryResult, register_provider
from packages.common import metrics


@register_provider("mistral", description="Mistral JSON evidence-cited narrative")
class MistralProvider:
    name = "mistral"

    def __init__(self) -> None:
        self.model = os.getenv("MISTRAL_MODEL", "mistral-small-latest")
        self.api_key = os.getenv("MISTRAL_API_KEY", "").strip()
        self.unavailable_reason: str | None = None

    def is_available(self) -> bool:
        if not self.api_key or self.api_key.lower().startswith("your_"):
            self.unavailable_reason = "MISTRAL_API_KEY is not set"
            return False
        return True

    def summarize(self, package: dict[str, Any], prompt: str) -> SummaryResult:
        started = time.perf_counter()
        try:
            with httpx.Client(timeout=30) as client:
                for attempt in range(3):
                    try:
                        response = client.post(
                            "https://api.mistral.ai/v1/chat/completions",
                            headers={"Authorization": f"Bearer {self.api_key}"},
                            json={
                                "model": self.model,
                                "messages": [{"role": "user", "content": prompt}],
                                "temperature": 0.2,
                                "max_tokens": 2048,
                                "response_format": {"type": "json_object"},
                                "tool_choice": "none",
                            },
                        )
                    except (httpx.TimeoutException, httpx.NetworkError):
                        if attempt == 2:
                            raise ProviderError("Mistral connection timed out or failed") from None
                    else:
                        if response.status_code < 400:
                            return self._parse(response.json(), started)
                        if response.status_code != 429 and response.status_code < 500:
                            raise ProviderError(f"Mistral rejected the request (HTTP {response.status_code})")
                        if attempt == 2:
                            if response.status_code == 429:
                                raise ProviderError("Mistral rate limit or quota exceeded (HTTP 429); try again later")
                            raise ProviderError(f"Mistral temporarily unavailable (HTTP {response.status_code})")
                    time.sleep(0.5 * 2**attempt)
        except (ValueError, KeyError, IndexError, TypeError, AttributeError) as error:
            metrics.ai_requests_total.labels(provider=self.name, outcome="error").inc()
            raise ProviderError("Mistral returned an invalid summary response") from error
        except ProviderError:
            metrics.ai_requests_total.labels(provider=self.name, outcome="error").inc()
            raise
        raise ProviderError("Mistral returned no response")

    def _parse(self, response: dict[str, Any], started: float) -> SummaryResult:
        text = response["choices"][0]["message"]["content"]
        payload = json.loads(text)
        sections = [
            Section(
                heading=str(section.get("heading", "Summary")),
                statements=[
                    Statement(
                        text=str(item["text"]).strip(),
                        kind="fact" if item.get("type") == "fact" else "inference",
                        evidence_ids=[str(eid) for eid in item.get("evidence_ids", [])],
                    )
                    for item in section.get("statements", [])
                    if str(item.get("text", "")).strip()
                ],
            )
            for section in payload.get("summary_sections", [])
        ]
        usage = response.get("usage") or {}
        tokens = {"input": int(usage.get("prompt_tokens", 0)), "output": int(usage.get("completion_tokens", 0))}
        latency = time.perf_counter() - started
        metrics.ai_latency_seconds.labels(provider=self.name).observe(latency)
        for kind, count in tokens.items():
            metrics.ai_tokens_total.labels(provider=self.name, kind=kind).inc(count)
        return SummaryResult(
            sections=sections,
            uncertainty=str(payload.get("uncertainty", "")),
            limitations=str(payload.get("limitations", "")),
            model=self.model,
            provider=self.name,
            raw_output=text[:20000],
            latency_ms=int(latency * 1000),
            tokens=tokens,
        )
