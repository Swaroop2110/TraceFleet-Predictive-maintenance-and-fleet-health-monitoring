# Shared image for the API, the stream-processor roles, and the simulator. One
# image because they share `packages/` and differ only in their entrypoint; three
# images would triple the build time and the scan surface for no benefit.
#
# Multi-stage so compiler toolchains needed to build wheels do not ship in the
# runtime layer.
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# confluent-kafka and psycopg2 need a compiler when no wheel matches.
RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc g++ libpq-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .
RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install --upgrade pip \
 && /opt/venv/bin/pip install -r requirements.txt


FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app:/app/apps/stream-processor:/app/apps/simulator \
    PATH="/opt/venv/bin:$PATH"

# libpq for psycopg2, curl for the healthcheck. No compilers in the runtime layer.
RUN apt-get update \
 && apt-get install -y --no-install-recommends libpq5 curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --create-home --uid 10001 --shell /usr/sbin/nologin tracefleet

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY --chown=tracefleet:tracefleet . .

# JWT keys are generated at first start when none are supplied. The directory is a
# volume in Compose so tokens survive a restart.
RUN mkdir -p /app/artifacts /var/lib/tracefleet/keys \
 && chown -R tracefleet:tracefleet /app/artifacts /var/lib/tracefleet

# Non-root. A container that does not need root should not have it.
USER tracefleet

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=5 \
    CMD curl -fsS http://localhost:8000/api/v1/health/live || exit 1

# --workers 1 by default: horizontal scaling is Compose replicas or a k8s HPA, and
# multiple uvicorn workers would multiply the in-process rate-limiter state.
CMD ["uvicorn", "apps.api.app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--no-access-log"]
