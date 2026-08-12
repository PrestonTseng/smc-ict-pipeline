#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from pathlib import Path

path = Path("var/evidence/casebook.json")
if not path.is_file():
    raise SystemExit("SMC/ICT daily summary failed: casebook is missing")
payload = path.read_bytes()
casebook = json.loads(payload)
summary = casebook["summary"]
rows = casebook["cases"]
latest_cutoff = max((row["cutoff"] for row in rows), default=None)

print("SMC/ICT Binance forward evidence daily summary (research-only; no orders/PnL)")
print(
    f"Eligible rows: {summary['eligible_cases']}/{summary['milestone_target']} "
    f"(remaining {summary['milestone_remaining']}); "
    f"unique dataset/cutoffs: {summary['unique_dataset_cutoffs']}"
)
print(
    f"Eligible runs: {summary['eligible_runs']}; "
    f"verified ineligible runs: {summary['ineligible_runs']}; "
    f"latest cutoff ms: {latest_cutoff}"
)
print(
    "Statuses: "
    + ", ".join(f"{key}={value}" for key, value in sorted(summary["status_counts"].items()))
)
print(
    "Failed gates: "
    + ", ".join(f"{key}={value}" for key, value in sorted(summary["failed_gate_counts"].items()))
)
print(f"Casebook SHA-256: {hashlib.sha256(payload).hexdigest()}")
