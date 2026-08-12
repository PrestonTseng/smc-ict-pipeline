#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path

path = Path("var/evidence/casebook.json")
if not path.is_file():
    raise SystemExit("SMC/ICT daily summary failed: casebook is missing")
payload = path.read_bytes()
casebook = json.loads(payload)
summary = casebook["summary"]
rows = casebook["cases"]
active_version = "v2-1d-4h-1h"
active_rows = [row for row in rows if row.get("strategy_version") == active_version]
legacy_rows = [row for row in rows if row.get("strategy_version") != active_version]
latest_cutoff = max((row["cutoff"] for row in active_rows), default=None)
active_unique = len({(row["dataset_version"], row["analysis_boundary"]) for row in active_rows})
active_remaining = max(0, summary["milestone_target"] - len(active_rows))
status_counts = Counter(row["status"] for row in active_rows)
failed_gate_counts = Counter(
    row["failed_gate"] for row in active_rows if row.get("failed_gate") is not None
)

print("SMC/ICT v2 1D→4H→1H forward evidence daily summary (research-only; no orders/PnL)")
print(
    f"Active v2 eligible rows: {len(active_rows)}/{summary['milestone_target']} "
    f"(remaining {active_remaining}); unique closed-1H boundaries: {active_unique}"
)
print(
    f"All verified eligible runs: {summary['eligible_runs']}; "
    f"verified ineligible runs: {summary['ineligible_runs']}; "
    f"legacy v1 rows preserved: {len(legacy_rows)}; latest v2 cutoff ms: {latest_cutoff}"
)
print(
    "Statuses: "
    + (", ".join(f"{key}={value}" for key, value in sorted(status_counts.items())) or "none")
)
print(
    "Failed gates: "
    + (", ".join(f"{key}={value}" for key, value in sorted(failed_gate_counts.items())) or "none")
)
print(f"Casebook SHA-256: {hashlib.sha256(payload).hexdigest()}")
