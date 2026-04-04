#!/usr/bin/env bash
# Usage:  bash run_all.sh [sf]   default sf=0.05
set -euo pipefail
SF=${1:-0.05}
DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECTS=(p01_ecommerce p02_fraud p03_iot p04_hr p05_logistics
          p06_saas p07_healthcare p08_adtech p09_gaming p10_energy)
echo "=== Installing dbt-duckdb ==="
pip install dbt-duckdb --quiet
PASS=0; FAIL=0
for P in "${PROJECTS[@]}"; do
  echo ""
  echo "━━━━ $P (sf=$SF) ━━━━"
  cd "$DIR/$P"
  mkdir -p data
  if python3 generate_data.py "$SF" && \
     dbt run --profiles-dir . --project-dir . 2>&1 | tail -15; then
    echo "✓ $P PASSED"; PASS=$((PASS+1))
  else
    echo "✗ $P FAILED"; FAIL=$((FAIL+1))
  fi
  cd "$DIR"
done
echo ""
echo "Results: $PASS passed, $FAIL failed"
