#!/usr/bin/env bash
# Guard for the Snowflake work: the compiled SQL of the existing `dev` (DuckDB)
# and `postgres` targets must stay byte-identical.
#
#   utils/check_compiled_unchanged.sh baseline [project ...]   # snapshot "before"
#   utils/check_compiled_unchanged.sh check    [project ...]   # compile "after" and diff
#
# Projects default to every entry of projects.yaml. Snapshots go to
# .compiled_baseline/<project>/<target>/ (gitignored). Each compile uses its own
# --target-path, so the project's real target/ is left alone. `postgres` needs
# the local Postgres to be up (DAGBENCH_PG_HOST/PORT), as `dbt compile` connects.
# Parallelism: DAGBENCH_COMPILE_JOBS (default 4).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DBT="${DBT:-$ROOT/.venv/bin/dbt}"
BASE="$ROOT/.compiled_baseline"
TARGETS=(${DAGBENCH_COMPILE_TARGETS:-dev postgres})
JOBS="${DAGBENCH_COMPILE_JOBS:-4}"

mode="${1:-}"
shift || true
case "$mode" in baseline|check) ;; *) echo "usage: $0 baseline|check [project ...]" >&2; exit 2 ;; esac

if [ $# -gt 0 ]; then
  projects=("$@")
else
  mapfile -t projects < <(sed -n 's#^- *projects/\([^/]*\)/*$#\1#p' "$ROOT/projects.yaml")
fi

# Per-invocation values (Tuva's metadata__tuva_invocations embeds them) are masked.
normalize() {
  grep -rlE "as (invocation_id|run_started_at)" "$1" 2>/dev/null | xargs -r sed -i -E \
    -e "s/'[0-9a-f-]{36}'( as [A-Za-z]+\)? as invocation_id)/'<invocation_id>'\1/" \
    -e "s/'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:.]+\+00:00'( as [A-Za-z]+\)? as run_started_at)/'<run_started_at>'\1/"
}

compile_one() {
  local mode="$1" project="$2" target="$3"
  local pdir="$ROOT/projects/$project"
  local out="$BASE/$project/$target"
  [ "$mode" = check ] && out="$BASE/$project/$target.after"
  local tp
  tp="$(mktemp -d "${TMPDIR:-/tmp}/dagbench_compile.XXXXXX")"
  if ! (cd "$pdir" && "$DBT" compile --quiet --no-partial-parse --target "$target" --target-path "$tp" >"$tp.log" 2>&1); then
    echo "FAIL compile $project/$target (log: $tp.log)" >&2
    return 1
  fi
  rm -rf "$out"
  mkdir -p "$(dirname "$out")"
  mv "$tp/compiled" "$out"
  normalize "$out"
  rm -rf "$tp" "$tp.log"
  echo "compiled $project/$target"
}
export -f compile_one normalize
export ROOT DBT BASE

jobs_list=()
for p in "${projects[@]}"; do
  for t in "${TARGETS[@]}"; do jobs_list+=("$mode $p $t"); done
done
printf '%s\n' "${jobs_list[@]}" | xargs -P "$JOBS" -L 1 bash -c 'compile_one "$@"' _

[ "$mode" = baseline ] && exit 0

status=0
for p in "${projects[@]}"; do
  for t in "${TARGETS[@]}"; do
    if [ ! -d "$BASE/$p/$t" ]; then
      echo "MISSING baseline for $p/$t; run '$0 baseline $p' on the unchanged tree first" >&2
      status=1
      continue
    fi
    if diff -r -q "$BASE/$p/$t" "$BASE/$p/$t.after" >/dev/null; then
      echo "OK   $p/$t"
    else
      echo "DIFF $p/$t" >&2
      diff -r -u "$BASE/$p/$t" "$BASE/$p/$t.after" | head -40 >&2 || true
      status=1
    fi
  done
done
exit $status
