#!/usr/bin/env bash
set -euo pipefail
bandit -r apps packages -x apps/web/node_modules -ll
