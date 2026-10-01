#!/usr/bin/env bash
set -uo pipefail
mkdir -p artifacts
py="${PYTHON:-.venv/bin/python}"
status=0
"$py" -m bandit -r apps/api apps/stream-processor apps/simulator packages scripts -ll -f json -o artifacts/bandit.json || status=1
"$py" -m pip_audit -r requirements.txt -f json -o artifacts/pip-audit.json || status=1
npm --prefix apps/web audit --json > artifacts/npm-audit.json || status=1
if command -v trivy >/dev/null; then
  trivy fs --scanners vuln,misconfig --severity HIGH,CRITICAL --format json --output artifacts/trivy.json --exit-code 1 . || status=1
elif [ -f artifacts/trivy.json ]; then
  echo "Keeping the previously generated Trivy container report"
else
  echo '{"status":"not_run","reason":"trivy is not installed; CI installs and runs the scanner"}' > artifacts/trivy.json
fi
exit "$status"
