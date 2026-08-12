#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
log_dir="${SMC_ICT_LOG_DIR:-var/logs}"
evidence_dir="${SMC_ICT_EVIDENCE_DIR:-var/evidence}"
max_attempts="${SMC_ICT_MAX_LOCK_ATTEMPTS:-6}"
retry_delay="${SMC_ICT_RETRY_DELAY:-10}"
if [[ ! "$max_attempts" =~ ^[1-9][0-9]*$ || ! "$retry_delay" =~ ^[0-9]+$ ]]; then
  printf 'invalid retry configuration: attempts must be positive; delay non-negative\n' >&2
  exit 2
fi
mkdir -p "$log_dir" "$evidence_dir"

analysis=""
for ((attempt = 1; attempt <= max_attempts; attempt++)); do
  if ! analysis="$(uv run smc-ict analyze-once --config configs/default.toml 2>&1)"; then
    printf '%s\n' "$analysis" >&2
    exit 1
  fi
  if ! status="$(python3 -c 'import json,sys; v=json.loads(sys.argv[1]); assert isinstance(v,dict) and isinstance(v.get("status"),str); print(v["status"])' "$analysis" 2>/dev/null)"; then
    printf 'invalid JSON receipt from SMC/ICT analysis\n' >&2
    exit 1
  fi
  if [[ "$status" != "SKIPPED_LOCKED" ]]; then
    break
  fi
  if ((attempt == max_attempts)); then
    printf 'SMC/ICT analysis remained locked after %s attempts\n' "$max_attempts" >&2
    exit 1
  fi
  sleep "$retry_delay"
done

case "$status" in
  NO_SETUP|BLOCKED|TRADE|ORDER_PENDING|ARMED|SKIPPED_ALREADY_ANALYZED) ;;
  *)
    printf 'invalid analysis status: %s\n' "$status" >&2
    exit 1
    ;;
esac

if ! receipt_identity="$(python3 -c 'import json,sys; v=json.loads(sys.argv[1]); assert v.get("strategy_version")=="v2-1d-4h-1h"; b=v.get("analysis_boundary"); assert isinstance(b,int) and not isinstance(b,bool) and b>=3599999 and (b+1)%3600000==0; d=v.get("dataset_version"); assert isinstance(d,str) and d; print(b,d)' "$analysis" 2>/dev/null)"; then
  printf 'invalid v2 identity in SMC/ICT analysis receipt\n' >&2
  exit 1
fi
read -r analysis_boundary dataset_version <<< "$receipt_identity"

if ! casebook="$(uv run smc-ict casebook --runs-root var/runs --output "$evidence_dir/casebook.json" --milestone-target 20 2>&1)"; then
  printf '%s\n' "$casebook" >&2
  exit 1
fi
if ! python3 -c 'import json,re,sys; value=json.loads(sys.argv[1]); eligible=value.get("eligible_cases") if isinstance(value,dict) else None; digest=value.get("sha256") if isinstance(value,dict) else None; assert isinstance(eligible,int) and not isinstance(eligible,bool) and eligible>=0; assert isinstance(digest,str) and re.fullmatch(r"[0-9a-fA-F]{64}",digest)' "$casebook" 2>/dev/null; then
  printf 'invalid JSON receipt from SMC/ICT casebook\n' >&2
  exit 1
fi
printf '%s analysis=%s casebook=%s\n' "$(date -u +%FT%TZ)" "$analysis" "$casebook" >> "$log_dir/analysis.jsonl"
