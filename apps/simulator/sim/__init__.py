"""TraceFleet telemetry simulator.

Generates the connected-vehicle stream the platform consumes, plus the hidden ground
truth used to measure detection accuracy. Ground truth is written directly to
Postgres and never travels the Kafka path, so the detection side cannot see it.
"""
