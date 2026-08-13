from __future__ import annotations

import csv
import fcntl
import hashlib
import io
import json
import os
import secrets
import sqlite3
import stat
import sys
from collections import Counter
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import config_hash
from .pipeline.state_machine import GATES
from .pipeline.v2_state_machine import V2_GATES, V2_STRATEGY_VERSION


class EvidenceError(ValueError):
    pass


_SHA256 = set("0123456789abcdef")
_MANIFEST_KEYS_V1 = {
    "schema_version",
    "ingestion_run_id",
    "analysis_run_id",
    "dataset_version",
    "dataset_checksum",
    "cutoff",
    "config_hash",
    "code_version",
    "files",
}
_MANIFEST_KEYS_V2 = _MANIFEST_KEYS_V1 | {"strategy_version", "analysis_boundary"}
_V1_STRATEGY_VERSION = "v1-4h-1h-5m"
_FILE_NAMES = {"config-snapshot.json", "decision.json"}
_REQUIRED_RUN_ENTRIES = _FILE_NAMES | {"manifest.json"}
_STATUSES = {"FAILED", "BLOCKED", "TRADE", "ORDER_PENDING", "ARMED", "NO_SETUP"}
_STATUS_PRIORITY = ("FAILED", "BLOCKED", "TRADE", "ORDER_PENDING", "ARMED", "NO_SETUP")
_INDICATOR_KEYS = {
    "status",
    "value",
    "event_time",
    "known_at",
    "reason_codes",
    "reference_levels",
    "input_hash",
    "config_hash",
}


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _is_hash(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and set(value) <= _SHA256


def _read_regular(path: Path) -> bytes:
    if path.is_symlink():
        raise EvidenceError(f"symlink is forbidden: {path}")
    if not path.is_file():
        raise EvidenceError(f"required regular file missing: {path}")
    return path.read_bytes()


def _open_directory(path: Path | str, *, dir_fd: int | None = None) -> int:
    try:
        return os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=dir_fd)
    except OSError as error:
        raise EvidenceError(f"directory is missing, invalid, or a symlink: {path}") from error


def _read_regular_at(directory_fd: int, name: str, label: Path) -> bytes:
    try:
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
    except OSError as error:
        raise EvidenceError(f"required regular file missing or symlinked: {label}") from error
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise EvidenceError(f"required regular file missing or symlinked: {label}")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            return handle.read()
    finally:
        os.close(descriptor)


def _optional_regular_at(directory_fd: int, name: str, label: Path) -> bytes | None:
    try:
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise EvidenceError(f"required regular file missing or symlinked: {label}")
    return _read_regular_at(directory_fd, name, label)


def _decode(payload: bytes, label: str) -> dict:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise EvidenceError(f"invalid {label}") from error
    if not isinstance(value, dict):
        raise EvidenceError(f"{label} must be an object")
    return value


def _directory_entries(directory_fd: int) -> list[str]:
    return sorted(os.listdir(directory_fd))


@contextmanager
def _open_runs(runs_root: Path):
    root_fd = _open_directory(runs_root)
    runs: list[tuple[Path, int]] = []
    try:
        for day_name in _directory_entries(root_fd):
            day_path = runs_root / day_name
            day_stat = os.stat(day_name, dir_fd=root_fd, follow_symlinks=False)
            if stat.S_ISLNK(day_stat.st_mode):
                raise EvidenceError(f"day symlink is forbidden: {day_path}")
            if day_name == ".tmp" or not stat.S_ISDIR(day_stat.st_mode):
                continue
            day_fd = _open_directory(day_name, dir_fd=root_fd)
            try:
                for run_name in _directory_entries(day_fd):
                    run_path = day_path / run_name
                    run_stat = os.stat(run_name, dir_fd=day_fd, follow_symlinks=False)
                    if stat.S_ISLNK(run_stat.st_mode):
                        raise EvidenceError(f"run symlink is forbidden: {run_path}")
                    if not stat.S_ISDIR(run_stat.st_mode):
                        continue
                    runs.append((run_path, _open_directory(run_name, dir_fd=day_fd)))
            finally:
                os.close(day_fd)
        yield runs
    finally:
        for _, run_fd in runs:
            os.close(run_fd)
        os.close(root_fd)


def _verify_hashes(
    run: Path, run_fd: int, manifest: dict
) -> tuple[dict[str, bytes], dict[str, str]]:
    entries = set(_directory_entries(run_fd))
    if not entries >= _REQUIRED_RUN_ENTRIES or entries - _REQUIRED_RUN_ENTRIES not in (
        set(),
        {"indicators"},
    ):
        raise EvidenceError("run artifact set mismatch")
    if "indicators" in entries:
        indicators_fd = _open_directory("indicators", dir_fd=run_fd)
        try:
            if _directory_entries(indicators_fd):
                raise EvidenceError("indicators directory must be empty")
        finally:
            os.close(indicators_fd)
    declared = manifest.get("files")
    if not isinstance(declared, dict) or set(declared) != _FILE_NAMES:
        raise EvidenceError("manifest files must name exact artifact set")
    payloads = {}
    hashes = {}
    for name in sorted(_FILE_NAMES):
        expected = declared[name]
        if not _is_hash(expected):
            raise EvidenceError(f"invalid {name} hash")
        payload = _read_regular_at(run_fd, name, run / name)
        actual = hashlib.sha256(payload).hexdigest()
        if actual != expected:
            raise EvidenceError(f"{name} hash mismatch")
        payloads[name] = payload
        hashes[name] = actual
    return payloads, hashes


def _string_list(value: Any, label: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise EvidenceError(f"{label} must be a string list")
    return value


def _expected_gate_decision(indicators: dict, gates: tuple[str, ...]) -> dict:
    if set(indicators) != set(gates):
        raise EvidenceError("gate decision indicator set mismatch")
    passed = []
    for gate in gates:
        indicator = indicators[gate]
        if not isinstance(indicator, dict):
            raise EvidenceError("invalid indicator")
        status = indicator.get("status")
        reasons = _string_list(indicator.get("reason_codes"), "indicator reason_codes")
        if status == "PASS":
            passed.append(gate)
            continue
        if status not in {"FAIL", "UNAVAILABLE"}:
            raise EvidenceError("gate decision indicator status mismatch")
        return {
            "status": "BLOCKED" if status == "UNAVAILABLE" else "NO_SETUP",
            "failed_gate": gate,
            "passed_gates": passed,
            "reason_codes": reasons,
        }
    return {"status": "TRADE", "passed_gates": passed, "reason_codes": ["all_gates_passed"]}


def _validate_indicators(
    indicators: dict,
    config_hash: str,
    gates: tuple[str, ...],
    timestamp_limit: int,
) -> None:
    if set(indicators) != set(gates):
        raise EvidenceError("gate decision indicator set mismatch")
    for gate in gates:
        indicator = indicators[gate]
        if not isinstance(indicator, dict):
            raise EvidenceError("invalid indicator")
        if set(indicator) != _INDICATOR_KEYS:
            raise EvidenceError("indicator wire key mismatch")
        if indicator.get("config_hash") != config_hash:
            raise EvidenceError("indicator config hash mismatch")
        if not _is_hash(indicator.get("input_hash")):
            raise EvidenceError("invalid indicator input hash")
        if indicator.get("status") not in {"PASS", "FAIL", "UNAVAILABLE"}:
            raise EvidenceError("invalid indicator status")
        _string_list(indicator.get("reason_codes"), "indicator reason_codes")
        if not isinstance(indicator.get("value"), dict):
            raise EvidenceError("indicator value must be an object")
        event_time = indicator.get("event_time")
        known_at = indicator.get("known_at")
        for label, value in (("event_time", event_time), ("known_at", known_at)):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise EvidenceError(f"invalid indicator {label}")
        if (event_time is None) != (known_at is None):
            raise EvidenceError("indicator event_time and known_at must be paired")
        if event_time is not None and known_at < event_time:
            raise EvidenceError("indicator known_at precedes event_time")
        if event_time is not None:
            assert known_at is not None
            if event_time > timestamp_limit or known_at > timestamp_limit:
                raise EvidenceError("indicator timestamp exceeds analysis boundary")
            timeframe_ms = {
                "smc_1d_regime": 86_400_000,
                "smc_4h_structure": 14_400_000,
                "smc_4h_dealing_range": 14_400_000,
                "smc_4h_order_block": 14_400_000,
                "smc_1h_dealing_range": 3_600_000,
                "smc_1h_order_block": 3_600_000,
                "ict_1h_liquidity": 3_600_000,
                "ict_1h_displacement": 3_600_000,
                "ict_1h_mss": 3_600_000,
                "ict_1h_fvg": 3_600_000,
                "ict_5m_liquidity": 300_000,
                "ict_5m_displacement": 300_000,
                "ict_5m_mss": 300_000,
                "ict_5m_fvg": 300_000,
            }.get(gate)
            if timeframe_ms is not None and (
                (event_time + 1) % timeframe_ms != 0 or (known_at + 1) % timeframe_ms != 0
            ):
                raise EvidenceError("indicator timestamp is not timeframe-close aligned")
        levels = indicator.get("reference_levels")
        if not isinstance(levels, dict) or any(
            not isinstance(key, str) or not isinstance(value, str) for key, value in levels.items()
        ):
            raise EvidenceError("indicator reference_levels must map strings to strings")


def _strategy_contract(manifest: dict) -> tuple[str, tuple[str, ...]]:
    if manifest.get("schema_version") == "1":
        if set(manifest) != _MANIFEST_KEYS_V1:
            raise EvidenceError("schema-v1 manifest key mismatch")
        return _V1_STRATEGY_VERSION, GATES
    if manifest.get("schema_version") == "2":
        if set(manifest) != _MANIFEST_KEYS_V2:
            raise EvidenceError("schema-v2 manifest key mismatch")
        if manifest.get("strategy_version") != V2_STRATEGY_VERSION:
            raise EvidenceError("unsupported strategy_version")
        return V2_STRATEGY_VERSION, V2_GATES
    raise EvidenceError("unsupported schema_version")


def _eligible_cases(run: Path, manifest: dict, manifest_hash: str, payloads, hashes):
    strategy_version, gates = _strategy_contract(manifest)
    for name in ("dataset_checksum", "config_hash", "code_version"):
        if not _is_hash(manifest[name]):
            raise EvidenceError(f"invalid manifest {name}")
    for name in ("ingestion_run_id", "analysis_run_id", "dataset_version"):
        if not isinstance(manifest[name], str) or not manifest[name]:
            raise EvidenceError(f"invalid manifest {name}")
    cutoff = manifest["cutoff"]
    if isinstance(cutoff, bool) or not isinstance(cutoff, int) or cutoff < 0:
        raise EvidenceError("invalid cutoff")
    analysis_boundary = manifest.get("analysis_boundary")
    if manifest["schema_version"] == "2":
        expected_boundary = ((cutoff + 1) // 3_600_000) * 3_600_000 - 1
        if (
            isinstance(analysis_boundary, bool)
            or not isinstance(analysis_boundary, int)
            or analysis_boundary != expected_boundary
        ):
            raise EvidenceError("invalid analysis_boundary")
    if manifest["analysis_run_id"] != run.name:
        raise EvidenceError("analysis_run_id does not match run directory")
    expected_day = datetime.fromtimestamp(cutoff / 1000, UTC).date().isoformat()
    if run.parent.name != expected_day:
        raise EvidenceError("cutoff does not match day directory")

    config = _decode(payloads["config-snapshot.json"], "config snapshot")
    if config.get("_config_hash") != manifest["config_hash"]:
        raise EvidenceError("config hash mismatch")
    governed_config = dict(config)
    governed_config.pop("_config_hash", None)
    if (
        manifest["schema_version"] == "2"
        and config_hash(governed_config) != manifest["config_hash"]
    ):
        raise EvidenceError("config snapshot content hash mismatch")
    decision = _decode(payloads["decision.json"], "decision")
    if set(decision) != {"status", "symbols"} or decision["status"] not in _STATUSES:
        raise EvidenceError("invalid run decision")
    symbols = decision["symbols"]
    if not isinstance(symbols, dict) or not symbols:
        raise EvidenceError("invalid decision symbols")
    config_symbols = config.get("symbols")
    if (
        not isinstance(config_symbols, list)
        or any(not isinstance(symbol, str) or not symbol for symbol in config_symbols)
        or len(config_symbols) != len(set(config_symbols))
        or set(config_symbols) != set(symbols)
    ):
        raise EvidenceError("config symbols do not match decision symbols")

    cases = []
    symbol_statuses = set()
    for symbol in sorted(symbols):
        if not isinstance(symbol, str) or not symbol:
            raise EvidenceError("invalid symbol")
        body = symbols[symbol]
        if not isinstance(body, dict) or not isinstance(body.get("decision"), dict):
            raise EvidenceError("invalid symbol decision")
        symbol_decision = body["decision"]
        status = symbol_decision.get("status")
        if status not in _STATUSES:
            raise EvidenceError("invalid symbol status")
        symbol_statuses.add(status)
        reasons = _string_list(symbol_decision.get("reason_codes", []), "reason_codes")
        passed = _string_list(symbol_decision.get("passed_gates", []), "passed_gates")
        failed_gate = symbol_decision.get("failed_gate")
        if failed_gate is not None and not isinstance(failed_gate, str):
            raise EvidenceError("invalid failed_gate")
        indicators = body.get("indicators")
        if not isinstance(indicators, dict) or not indicators:
            raise EvidenceError("invalid indicators")
        timestamp_limit = analysis_boundary if analysis_boundary is not None else cutoff
        _validate_indicators(indicators, manifest["config_hash"], gates, timestamp_limit)
        if symbol_decision != _expected_gate_decision(indicators, gates):
            raise EvidenceError("gate decision does not match authoritative state machine")
        identity = _canonical([manifest["analysis_run_id"], symbol])
        cases.append(
            {
                "case_id": hashlib.sha256(identity).hexdigest(),
                "analysis_run_id": manifest["analysis_run_id"],
                "strategy_version": strategy_version,
                "analysis_boundary": analysis_boundary,
                "symbol": symbol,
                "cutoff": cutoff,
                "run_status": decision["status"],
                "status": status,
                "failed_gate": failed_gate,
                "passed_gates": passed,
                "reason_codes": reasons,
                "pipeline_steps": [{"gate": gate, **indicators[gate]} for gate in gates],
                "dataset_version": manifest["dataset_version"],
                "dataset_checksum": manifest["dataset_checksum"],
                "ingestion_run_id": manifest["ingestion_run_id"],
                "config_hash": manifest["config_hash"],
                "code_version": manifest["code_version"],
                "manifest_sha256": manifest_hash,
                "decision_sha256": hashes["decision.json"],
            }
        )
    expected_run_status = next(status for status in _STATUS_PRIORITY if status in symbol_statuses)
    if decision["status"] != expected_run_status:
        raise EvidenceError("run status disagrees with symbol decisions")
    return cases


def _validate_v2_claim(runs_root: Path, run: Path, manifest: dict, manifest_hash: str) -> None:
    database = runs_root.parent / "data" / "market.sqlite3"
    if not database.is_file():
        raise EvidenceError("schema-v2 published claim database missing")
    try:
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
            row = connection.execute(
                """SELECT status,artifact_day,manifest_sha256
                FROM analysis_claims
                WHERE strategy_version=? AND analysis_boundary=? AND analysis_run_id=?""",
                (
                    manifest["strategy_version"],
                    manifest["analysis_boundary"],
                    manifest["analysis_run_id"],
                ),
            ).fetchone()
    except sqlite3.Error as error:
        raise EvidenceError("schema-v2 published claim unavailable") from error
    if row != ("PUBLISHED", run.parent.name, manifest_hash):
        raise EvidenceError("schema-v2 published claim mismatch")


def build_casebook(runs_root: Path, milestone_target: int = 20) -> dict:
    if (
        isinstance(milestone_target, bool)
        or not isinstance(milestone_target, int)
        or milestone_target <= 0
    ):
        raise ValueError("milestone_target must be a positive integer")
    cases = []
    exclusions = []
    eligible_runs = 0
    with _open_runs(runs_root) as runs:
        discovered_runs = len(runs)
        for run, run_fd in runs:
            manifest_payload = _read_regular_at(run_fd, "manifest.json", run / "manifest.json")
            manifest_hash = hashlib.sha256(manifest_payload).hexdigest()
            manifest = _decode(manifest_payload, "manifest")
            payloads, hashes = _verify_hashes(run, run_fd, manifest)
            if "schema_version" not in manifest:
                exclusions.append(
                    {"path": str(run.relative_to(runs_root)), "reason": "LEGACY_SCHEMA"}
                )
                continue
            _strategy_contract(manifest)
            if manifest.get("schema_version") == "2":
                _validate_v2_claim(runs_root, run, manifest, manifest_hash)
            cases.extend(_eligible_cases(run, manifest, manifest_hash, payloads, hashes))
            eligible_runs += 1
    cases.sort(key=lambda row: (row["cutoff"], row["analysis_run_id"], row["symbol"]))
    strategy_versions = {}
    for strategy_version in sorted({row["strategy_version"] for row in cases}):
        version_cases = [row for row in cases if row["strategy_version"] == strategy_version]
        version_statuses = Counter(row["status"] for row in version_cases)
        version_gates = Counter(
            row["failed_gate"] for row in version_cases if row["failed_gate"] is not None
        )
        version_reasons = Counter(reason for row in version_cases for reason in row["reason_codes"])
        boundaries = {
            row["analysis_boundary"]
            for row in version_cases
            if row["analysis_boundary"] is not None
        }
        strategy_versions[strategy_version] = {
            "eligible_cases": len(version_cases),
            "unique_dataset_cutoffs": len(
                {(row["dataset_version"], row["cutoff"]) for row in version_cases}
            ),
            "unique_analysis_boundaries": len(boundaries),
            "status_counts": dict(sorted(version_statuses.items())),
            "failed_gate_counts": dict(sorted(version_gates.items())),
            "reason_counts": dict(sorted(version_reasons.items())),
            "milestone_remaining": max(0, milestone_target - len(version_cases)),
            "milestone_reached": len(version_cases) >= milestone_target,
        }
    active_strategy = V2_STRATEGY_VERSION
    active = strategy_versions.get(
        active_strategy,
        {
            "eligible_cases": 0,
            "unique_dataset_cutoffs": 0,
            "unique_analysis_boundaries": 0,
            "status_counts": {},
            "failed_gate_counts": {},
            "reason_counts": {},
            "milestone_remaining": milestone_target,
            "milestone_reached": False,
        },
    )
    return {
        "schema_version": "2",
        "cases": cases,
        "exclusions": sorted(exclusions, key=lambda row: row["path"]),
        "summary": {
            "discovered_runs": discovered_runs,
            "eligible_runs": eligible_runs,
            "ineligible_runs": discovered_runs - eligible_runs,
            "total_eligible_cases": len(cases),
            "active_strategy_version": active_strategy,
            "eligible_cases": active["eligible_cases"],
            "unique_dataset_cutoffs": active["unique_dataset_cutoffs"],
            "unique_analysis_boundaries": active["unique_analysis_boundaries"],
            "strategy_versions": strategy_versions,
            "status_counts": active["status_counts"],
            "failed_gate_counts": active["failed_gate_counts"],
            "reason_counts": active["reason_counts"],
            "milestone_target": milestone_target,
            "milestone_remaining": active["milestone_remaining"],
            "milestone_reached": active["milestone_reached"],
        },
    }


def render_casebook(result: dict) -> bytes:
    return _canonical(result) + b"\n"


_GATE_LABELS = {
    "smc_1d_regime": "1D Regime",
    "smc_4h_structure": "4H Structure",
    "smc_4h_dealing_range": "4H Range",
    "smc_4h_order_block": "4H Order Block",
    "ict_1h_liquidity": "1H Liquidity",
    "ict_1h_displacement": "1H Displacement",
    "ict_1h_mss": "1H MSS",
    "ict_1h_fvg": "1H FVG",
    "risk": "Risk",
}


def _utc(timestamp: int | None) -> str:
    if timestamp is None:
        return ""
    return datetime.fromtimestamp(timestamp / 1000, UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _compact_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _active_cases(result: dict) -> list[dict]:
    active_strategy = result["summary"]["active_strategy_version"]
    return [case for case in result["cases"] if case["strategy_version"] == active_strategy]


def render_casebook_markdown(result: dict) -> bytes:
    cases = _active_cases(result)
    gates = [step["gate"] for step in cases[0]["pipeline_steps"]] if cases else []
    lines = [
        "# SMC/ICT Pipeline Casebook",
        "",
        f"- Active strategy: `{result['summary']['active_strategy_version']}`",
        f"- Eligible cases: {result['summary']['eligible_cases']}",
        f"- Unique hourly boundaries: {result['summary']['unique_analysis_boundaries']}",
        "",
        "## Hourly pipeline matrix",
        "",
        "| Boundary (UTC) | Symbol | Decision | "
        + " | ".join(_GATE_LABELS.get(gate, gate) for gate in gates)
        + " |",
        "|---|---|---|" + "---|" * len(gates),
    ]
    for case in cases:
        cells = [
            f"{step['status']} ({'; '.join(step['reason_codes']) or '-'})"
            for step in case["pipeline_steps"]
        ]
        lines.append(
            f"| {_utc(case['analysis_boundary'])} | {case['symbol']} | {case['status']} | "
            + " | ".join(cells)
            + " |"
        )
    lines.extend(["", "## Pipeline step details", ""])
    for case in cases:
        lines.extend(
            [
                f"### {_utc(case['analysis_boundary'])} · {case['symbol']}",
                "",
                f"Decision: **{case['status']}**; failed gate: `{case['failed_gate'] or '-'}`.",
                "",
            ]
        )
        for step in case["pipeline_steps"]:
            reasons = "; ".join(step["reason_codes"]) or "-"
            value_json = _compact_json(step["value"])
            levels_json = _compact_json(step["reference_levels"])
            lines.extend(
                [
                    f"- `{step['gate']}` — **{step['status']}** — {reasons}",
                    (
                        f"  - event: `{_utc(step['event_time']) or '-'}`; "
                        f"known: `{_utc(step['known_at']) or '-'}`"
                    ),
                    f"  - value: `{value_json}`",
                    f"  - reference levels: `{levels_json}`",
                ]
            )
        lines.append("")
    return ("\n".join(lines) + "\n").encode()


def render_casebook_csv(result: dict) -> bytes:
    output = io.StringIO(newline="")
    fields = (
        "analysis_boundary_utc",
        "symbol",
        "decision_status",
        "failed_gate",
        "gate",
        "status",
        "reason_codes",
        "event_time_utc",
        "known_at_utc",
        "value_json",
        "reference_levels_json",
        "analysis_run_id",
        "dataset_version",
        "case_id",
    )
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for case in _active_cases(result):
        for step in case["pipeline_steps"]:
            writer.writerow(
                {
                    "analysis_boundary_utc": _utc(case["analysis_boundary"]),
                    "symbol": case["symbol"],
                    "decision_status": case["status"],
                    "failed_gate": case["failed_gate"] or "",
                    "gate": step["gate"],
                    "status": step["status"],
                    "reason_codes": ";".join(step["reason_codes"]),
                    "event_time_utc": _utc(step["event_time"]),
                    "known_at_utc": _utc(step["known_at"]),
                    "value_json": json.dumps(step["value"], sort_keys=True, separators=(",", ":")),
                    "reference_levels_json": json.dumps(
                        step["reference_levels"], sort_keys=True, separators=(",", ":")
                    ),
                    "analysis_run_id": case["analysis_run_id"],
                    "dataset_version": case["dataset_version"],
                    "case_id": case["case_id"],
                }
            )
    return output.getvalue().encode()


def _write_temporary_at(directory_fd: int, output_name: str, payload: bytes) -> str:
    temporary_name = f".{output_name}.{secrets.token_hex(16)}.tmp"
    descriptor = os.open(
        temporary_name,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o600,
        dir_fd=directory_fd,
    )
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(descriptor)
    return temporary_name


def publish_casebook(
    runs_root: Path,
    output: Path,
    milestone_target: int = 20,
    markdown_output: Path | None = None,
    csv_output: Path | None = None,
) -> dict:
    output.parent.mkdir(parents=True, exist_ok=True)
    requested_outputs = [path for path in (markdown_output, csv_output) if path is not None]
    if any(path.parent != output.parent for path in requested_outputs):
        raise ValueError("all casebook outputs must share one directory")
    parent_fd = _open_directory(output.parent)
    lock_name = f".{output.name}.lock"
    lock_fd = None
    locked = False
    temporary_names: dict[str, str] = {}
    try:
        lock_fd = os.open(
            lock_name,
            os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
            0o600,
            dir_fd=parent_fd,
        )
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = True
        except BlockingIOError as error:
            raise EvidenceError(f"casebook publication already running for {output}") from error

        result = build_casebook(runs_root, milestone_target=milestone_target)
        payloads = {}
        if markdown_output is not None:
            payloads[markdown_output.name] = render_casebook_markdown(result)
        if csv_output is not None:
            payloads[csv_output.name] = render_casebook_csv(result)
        payloads[output.name] = render_casebook(result)
        if len(payloads) != 1 + len(requested_outputs):
            raise ValueError("casebook output names must be unique")
        old_payloads = {
            name: _optional_regular_at(parent_fd, name, output.parent / name) for name in payloads
        }
        for name, payload in payloads.items():
            temporary_names[name] = _write_temporary_at(parent_fd, name, payload)
        try:
            for name in payloads:
                os.replace(
                    temporary_names[name],
                    name,
                    src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd,
                )
                temporary_names.pop(name)
            os.fsync(parent_fd)
        except BaseException as primary_error:
            try:
                for name, old_payload in old_payloads.items():
                    if old_payload is None:
                        with suppress(FileNotFoundError):
                            os.unlink(name, dir_fd=parent_fd)
                    else:
                        recovery_name = _write_temporary_at(parent_fd, name, old_payload)
                        try:
                            os.replace(
                                recovery_name,
                                name,
                                src_dir_fd=parent_fd,
                                dst_dir_fd=parent_fd,
                            )
                        finally:
                            with suppress(FileNotFoundError):
                                os.unlink(recovery_name, dir_fd=parent_fd)
                os.fsync(parent_fd)
            except BaseException as recovery_error:
                primary_error.add_note(f"casebook publication recovery failed: {recovery_error!r}")
            raise primary_error

        reopened_payloads = {
            name: _read_regular_at(parent_fd, name, output.parent / name) for name in payloads
        }
        if reopened_payloads != payloads:
            raise EvidenceError("published casebook bytes changed")
        reopened = reopened_payloads[output.name]
        summary = result["summary"]
        return {
            "output": str(output),
            "sha256": hashlib.sha256(reopened).hexdigest(),
            "eligible_cases": summary["eligible_cases"],
            "milestone_target": summary["milestone_target"],
            "milestone_remaining": summary["milestone_remaining"],
            "milestone_reached": summary["milestone_reached"],
        }
    finally:
        primary_error = sys.exception()
        cleanup_errors: list[BaseException] = []
        for temporary_name in temporary_names.values():
            try:
                os.unlink(temporary_name, dir_fd=parent_fd)
            except FileNotFoundError:
                pass
            except BaseException as error:
                cleanup_errors.append(error)
        if locked and lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            except BaseException as error:
                cleanup_errors.append(error)
        if lock_fd is not None:
            try:
                os.close(lock_fd)
            except BaseException as error:
                cleanup_errors.append(error)
        try:
            os.close(parent_fd)
        except BaseException as error:
            cleanup_errors.append(error)
        if primary_error is not None:
            for error in cleanup_errors:
                primary_error.add_note(f"casebook cleanup failed: {error!r}")
        elif cleanup_errors:
            cleanup_error = cleanup_errors[0]
            for error in cleanup_errors[1:]:
                cleanup_error.add_note(f"additional casebook cleanup failure: {error!r}")
            raise cleanup_error
