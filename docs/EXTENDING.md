# Extending TraceFleet

## Features and API modules

Add a feature to `config/features.yaml` with explicit dependencies. The dependency validator rejects enabled features whose prerequisites are disabled. `scripts/feature.py enable|disable NAME` updates the manifest; restart the API and processors so discovery and runtime behavior use the same deployment configuration. Disabled module routes disappear from OpenAPI. `/api/v1/config/features` feeds the browser navigation.

Place an API module in `apps/api/app/modules/`, expose `router` and `FEATURE_FLAGS`, and use `require(permission)` plus `get_tenant_db` for tenant data. Register permissions in `config/roles.yaml`. Module discovery skips private helpers. Use `register(context)` for optional setup. Add a contract test and a feature-off OpenAPI test.

## Detection and factors

Add a file under `processor/detectors/` with `@register_detector(name)`. It receives the vehicle window, current canonical event, validated parameters and configured weight, returning an explainable `Signal` or `None`. Add the matching YAML rule, factor plug-in and scenario. A factor must retain its evidence IDs, bounded score, confidence and observed components. Keep AI and supplemental anomaly scores out of incident trigger/severity decisions.

Correlation policies and severity strategies have separate registries under `processor/correlation/` and `processor/severity/`. Check schema/config validation and test false positives, lateness, replay and lifecycle behavior when changing a trigger. Window memory is bounded by `max_active_vehicles`.

## Simulator and evaluation

Use `@register_scenario` in `sim/scenarios/`; return `ScenarioResult(events, GroundTruth(...))`. Labels must declare both positive and negative outcomes. Add the scenario to `config/scenarios.yaml`. Ground-truth persistence bypasses Kafka. Add BDD and real-store tests, then run `make evaluate`; it gates precision, recall, accuracy, severity, signal coverage and timing at 95%.

## AI providers

Implement the provider contract in `apps/api/app/services/ai/providers/` and register it. The evidence package, prompt and validator are provider-independent. A narrative must cite known evidence, separate fact/inference and avoid numeric hallucinations and fault attribution. Failed validation withholds the narrative; every run retains its reconstructable evidence package, model/version, prompt hash, result and validation. The mock provider supports local and CI runs without secrets.

Optional model comparison: `PYTHONPATH=.:apps/stream-processor:apps/simulator .venv/bin/python scripts/compare_anomaly.py`. The robust MAD fallback needs no ML dependency. A trusted local Isolation Forest artifact may be supplied via `ANOMALY_MODEL_PATH`; never load untrusted joblib files.
