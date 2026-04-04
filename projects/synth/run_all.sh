#!/usr/bin/env bash
# run_all.sh  –  install deps, generate data at sf=0.05, run dbt for all 10 projects
# Usage:  bash run_all.sh [scale_factor]   default sf=0.05
set -euo pipefail

SF=${1:-0.05}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECTS=(p01_ecommerce p02_fraud p03_iot p04_hr p05_logistics
          p06_saas p07_healthcare p08_adtech p09_gaming p10_energy)

echo "=== Installing dbt-duckdb ==="
pip install dbt-duckdb --quiet

PASS=0; FAIL=0
for P in "${PROJECTS[@]}"; do
  DIR="$SCRIPT_DIR/$P"
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  PROJECT: $P  (sf=$SF)"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  cd "$DIR"
  mkdir -p data

  echo "  [1/2] Generating data..."
  if python3 generate_data.py "$SF"; then
    echo "  [2/2] Running dbt..."
    if dbt run --profiles-dir . --project-dir . --select "*" 2>&1 | tail -20; then
      echo "  ✓ $P PASSED"
      PASS=$((PASS+1))
    else
      echo "  ✗ $P FAILED (dbt run)"
      FAIL=$((FAIL+1))
    fi
  else
    echo "  ✗ $P FAILED (data gen)"
    FAIL=$((FAIL+1))
  fi
  cd "$SCRIPT_DIR"
done

echo ""
echo "══════════════════════════════════════════"
echo "  Results: $PASS passed, $FAIL failed"
echo "══════════════════════════════════════════"
