#!/usr/bin/env bash
# Usage:  bash run_all.sh [sf]   default sf=0.05
set -euo pipefail
SF=${1:-0.05}
TARGET=${2:-${DBT_TARGET:-dev}}
DIR="$(cd "$(dirname "$0")" && pwd)"
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
  echo ""
  echo "━━━━ $P (sf=$SF, target=$TARGET) ━━━━"
  cd "$DIR/$P"
  mkdir -p data
  if python3 generate_data.py "$SF"; then
    if [[ "$TARGET" == "postgres" ]]; then
      python3 load_postgres.py
    fi
    if DBT_TARGET="$TARGET" dbt run --target "$TARGET" --profiles-dir . --project-dir . 2>&1 | tail -15; then
      echo "✓ $P PASSED"; PASS=$((PASS+1))
    else
      echo "✗ $P FAILED"; FAIL=$((FAIL+1))
    fi
  else
    echo "✗ $P FAILED"; FAIL=$((FAIL+1))
  fi
  cd "$DIR"
done
echo ""
echo "Results: $PASS passed, $FAIL failed"
