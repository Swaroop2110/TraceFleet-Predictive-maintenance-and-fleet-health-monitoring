import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
STREAM_PROCESSOR = ROOT / "apps" / "stream-processor"
for path in (ROOT, STREAM_PROCESSOR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

os.environ.setdefault("OTEL_SDK_DISABLED", "true")
os.environ.setdefault("JWT_KEY_DIR", "/tmp/tracefleet-test-keys")


@pytest.fixture(autouse=True)
def local_ai_provider(monkeypatch):
    """Keep acceptance tests independent of live API keys and provider quotas."""
    monkeypatch.setenv("LLM_PROVIDER", "mock")
