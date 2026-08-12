#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
log_dir="${SMC_ICT_LOG_DIR:-var/logs}"
max_attempts="${SMC_ICT_MAX_LOCK_ATTEMPTS:-6}"
retry_delay="${SMC_ICT_RETRY_DELAY:-10}"
if [[ ! "$max_attempts" =~ ^[1-9][0-9]*$ || ! "$retry_delay" =~ ^[0-9]+$ ]]; then
  printf 'invalid retry configuration: attempts must be positive; delay non-negative\n' >&2
  exit 2
fi
mkdir -p "$log_dir"

result=""
for ((attempt = 1; attempt <= max_attempts; attempt++)); do
  if ! result="$(uv run smc-ict ingest-once --config configs/default.toml 2>&1)"; then
    printf '%s\n' "$result" >&2
    exit 1
  fi
  if ! status="$(python3 -c 'import json,sys; value=json.loads(sys.argv[1]); assert isinstance(value,dict) and isinstance(value.get("status"),str); print(value["status"])' "$result" 2>/dev/null)"; then
    printf 'invalid JSON receipt from SMC/ICT ingestion\n' >&2
    exit 1
  fi
  if [[ "$status" != "SKIPPED_LOCKED" ]]; then
    break
  fi
  if ((attempt == max_attempts)); then
    printf 'SMC/ICT ingestion remained locked after %s attempts\n' "$max_attempts" >&2
    exit 1
  fi
  sleep "$retry_delay"
done
if [[ "$status" != "COMMITTED" ]]; then
  printf 'invalid ingestion status: %s\n' "$status" >&2
  exit 1
fi
printf '%s %s\n' "$(date -u +%FT%TZ)" "$result" >> "$log_dir/ingestion.jsonl"
