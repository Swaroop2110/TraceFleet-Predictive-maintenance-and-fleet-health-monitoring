$ErrorActionPreference = "Stop"

Write-Host "Starting TraceFleet services and waiting for the API health check..."
docker compose up -d --build --wait
if ($LASTEXITCODE -ne 0) {
    throw "TraceFleet services did not become ready. Run 'docker compose logs' for details."
}

Write-Host "Publishing collision, overspeed and medium-severity examples..."
docker compose run --rm simulator python apps/simulator/sim/main.py kafka:9092 --scenario mixed --batches 4 --ground-truth /app/artifacts/ground-truth.jsonl
if ($LASTEXITCODE -ne 0) {
    throw "The simulator failed to publish the demo scenario."
}

Write-Host "Opening the demo dashboard and observability views..."
Start-Process "http://localhost:5173"
Start-Process "http://localhost:3000/d/tracefleet-overview/tracefleet-overview"
Write-Host "Demo is live. API docs: http://localhost:8000/docs"
Write-Host "Login: tenant_default / analyst / tracefleet-demo"