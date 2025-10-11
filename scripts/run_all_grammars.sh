#!/usr/bin/env bash
# Run all registered grammars sequentially, 5k queries each by default.
# Usage:
#   scripts/run_all_grammars.sh [extra runner flags]
#
# Env vars:
#   COUNT=5000                # queries per grammar
#   DSN=postgresql://...      # optional DSN to execute against a DB
#   SCHEMA_NAME=public        # optional target schema (when DSN is set)
#   PROGRESS_EVERY=           # optional progress interval; extras can override
#
# Examples:
#   COUNT=5000 DSN="postgresql://yugabyte:yugabyte@localhost:5433/yugabyte" \
#   SCHEMA_NAME=public scripts/run_all_grammars.sh --print-errors --progress-every 500

set -uo pipefail
trap 'echo "\n[run_all_grammars] Interrupted by user" >&2; exit 130' INT QUIT

COUNT="${COUNT:-5000}"
DSN="${DSN:-}"
SCHEMA_NAME="${SCHEMA_NAME:-}"
PROGRESS_EVERY="${PROGRESS_EVERY:-}"

extra=("$@")

echo "Discovering grammars..." >&2
mapfile -t grammars < <(python -m pyrqg.runner --list-grammars | awk -F: '{print $1}' | sed '/^$/d')

if [[ ${#grammars[@]} -eq 0 ]]; then
  echo "No grammars found. Aborting." >&2
  exit 1
fi

echo "Found ${#grammars[@]} grammars. Running ${COUNT} queries per grammar..." >&2

# Detect Yugabyte from DSN
is_yb=0
if [[ -n "${DSN}" ]]; then
  dsn_lc="$(printf "%s" "${DSN}" | tr '[:upper:]' '[:lower:]')"
  if [[ "${dsn_lc}" == *yugabyte* ]] || [[ "${dsn_lc}" == *:5433* ]]; then
    is_yb=1
  fi
fi

ok=0
fail=0
start_ts=$(date +%s)

for g in "${grammars[@]}"; do
  # Skip known Yugabyte-sensitive grammars
  if [[ ${is_yb} -eq 1 ]] && [[ "${g}" == "merge_statement" || "${g}" == "performance_edge_cases" ]]; then
    echo "Skipping ${g} for Yugabyte DSN" >&2
    continue
  fi

  # Per-grammar tuning
  local_count="${COUNT}"
  local_threads=""
  if [[ "${g}" == "ddl_focused" || "${g}" == "functions_ddl" ]]; then
    local_count=300
    local_threads=(--threads 1)
  fi

  echo "\n=== Running grammar: ${g} (count=${local_count}) ===" >&2
  cmd=(python -m pyrqg.runner production --custom --workload-grammars "${g}" --count "${local_count}" "${local_threads[@]}")
  if [[ -n "${DSN}" ]]; then
    cmd+=(--dsn "${DSN}")
  fi
  if [[ -n "${SCHEMA_NAME}" ]]; then
    cmd+=(--schema-name "${SCHEMA_NAME}")
  fi
  # Default helpful flags; can be overridden by extras
  cmd+=(--error-samples 10)
  if [[ -n "${PROGRESS_EVERY}" ]]; then
    cmd+=(--progress-every "${PROGRESS_EVERY}")
  fi
  cmd+=("${extra[@]}")

  # Run and keep going on failure
  if "${cmd[@]}"; then
    ((ok++))
  else
    ((fail++))
    echo "Grammar '${g}' failed (continuing)." >&2
  fi
done

dur=$(( $(date +%s) - start_ts ))
echo "\nAll grammars complete. OK=${ok}, FAIL=${fail}, duration=${dur}s" >&2
exit 0
