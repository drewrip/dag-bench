#!/usr/bin/env bash
# run_all.sh  –  install deps, generate data at sf=0.05, run dbt for all 10 projects
# Usage:  bash run_all.sh [scale_factor]   default sf=0.05
set -euo pipefail

SF=${1:-0.05}
TARGET=${2:-${DBT_TARGET:-dev}}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECTS=(p01_ecommerce p02_fraud p03_iot p04_hr p05_logistics
          p06_saas p07_healthcare p08_adtech p09_gaming p10_energy)

if [[ "$TARGET" == "postgres" ]]; then
  echo "=== Installing dbt-duckdb and dbt-postgres ==="
  pip install dbt-duckdb dbt-postgres --quiet
else
  echo "=== Installing dbt-duckdb ==="
  pip install dbt-duckdb --quiet
fi

PASS=0; FAIL=0
for P in "${PROJECTS[@]}"; do
  DIR="$SCRIPT_DIR/$P"
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  PROJECT: $P  (sf=$SF, target=$TARGET)"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  cd "$DIR"
  mkdir -p data

  echo "  [1/2] Generating data..."
  if python3 generate_data.py "$SF"; then
    if [[ "$TARGET" == "postgres" ]]; then
      echo "  [2/3] Loading Postgres source tables..."
      python3 load_postgres.py
      echo "  [3/3] Running dbt..."
    else
      echo "  [2/2] Running dbt..."
    fi
    if DBT_TARGET="$TARGET" dbt run --target "$TARGET" --profiles-dir . --project-dir . --select "*" 2>&1 | tail -20; then
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
