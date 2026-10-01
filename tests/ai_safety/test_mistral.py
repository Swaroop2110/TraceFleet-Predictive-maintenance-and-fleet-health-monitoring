import json

import httpx
import pytest

from apps.api.app.services.ai.providers.mistral import MistralProvider
from apps.api.app.services.ai.registry import ProviderError, resolve_provider


def mock_api(monkeypatch, handler):
    original = httpx.Client
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: original(transport=httpx.MockTransport(handler), **kwargs))
    monkeypatch.setattr("apps.api.app.services.ai.providers.mistral.time.sleep", lambda _: None)


def test_mistral_json_contract_and_registration(monkeypatch):
    monkeypatch.setenv("MISTRAL_API_KEY", "test-key")
    monkeypatch.setenv("MISTRAL_MODEL", "test-model")

    def handler(request):
        payload = json.loads(request.content)
        assert request.headers["Authorization"] == "Bearer test-key"
        assert payload["response_format"] == {"type": "json_object"}
        assert payload["tool_choice"] == "none"
        assert payload["model"] == "test-model"
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "summary_sections": [
                                        {
                                            "heading": "Timeline",
                                            "statements": [
                                                {
                                                    "text": "Speed crossed its threshold.",
                                                    "type": "unexpected",
                                                    "evidence_ids": ["E1"],
                                                }
                                            ],
                                        }
                                    ],
                                    "uncertainty": "Limited evidence",
                                    "limitations": "No cause determined",
                                }
                            )
                        }
                    }
                ],
                "usage": {"prompt_tokens": 10, "completion_tokens": 20},
            },
        )

    mock_api(monkeypatch, handler)
    provider = resolve_provider("mistral")
    result = provider.summarize({}, "Evidence prompt")
    assert result.provider == "mistral"
    assert result.sections[0].statements[0].kind == "inference"
    assert result.sections[0].statements[0].evidence_ids == ["E1"]
    assert result.tokens == {"input": 10, "output": 20}


@pytest.mark.parametrize("status, attempts", [(401, 1), (429, 3), (503, 3)])
def test_mistral_errors_are_bounded_and_do_not_expose_response(monkeypatch, status, attempts):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, text="sensitive upstream detail")

    mock_api(monkeypatch, handler)
    with pytest.raises(ProviderError, match=f"HTTP {status}") as exc:
        MistralProvider().summarize({}, "prompt")
    assert len(calls) == attempts
    assert "sensitive" not in str(exc.value)


def test_mistral_malformed_json_becomes_provider_error(monkeypatch):
    mock_api(monkeypatch, lambda _: httpx.Response(200, json={"choices": [{"message": {"content": "bad"}}]}))
    with pytest.raises(ProviderError, match="invalid summary response"):
        MistralProvider().summarize({}, "prompt")


def test_mistral_without_key_falls_back(monkeypatch):
    monkeypatch.delenv("MISTRAL_API_KEY", raising=False)
    assert resolve_provider("mistral").name == "mock"


def test_summary_cache_is_scoped_to_provider_and_model():
    from apps.api.app.services.ai.service import _cached_run

    class FakeDB:
        def execute(self, statement):
            self.query = statement.compile().params
            return self

        def scalar_one_or_none(self):
            return None

    db = FakeDB()
    assert _cached_run(db, "INC-1", "prompt-hash", "evidence-hash", "mistral", "test-model") is None
    assert db.query["provider_1"] == "mistral"
    assert db.query["model_1"] == "test-model"
