import csv
import hashlib
import io
import json
import os
import sqlite3
import threading
from pathlib import Path

import pytest

from smc_ict.casebook import (
    EvidenceError,
    build_casebook,
    publish_casebook,
    render_casebook,
    render_casebook_csv,
    render_casebook_markdown,
)
from smc_ict.cli import main
from smc_ict.config import config_hash as compute_config_hash
from smc_ict.pipeline.state_machine import GATES
from smc_ict.pipeline.v2_state_machine import V2_GATES


def _json_bytes(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def _write_run(
    root: Path,
    run_id: str,
    cutoff: int,
    *,
    schema=True,
    schema_version="1",
    strategy_version=None,
    symbol_order=("BTCUSDT", "ETHUSDT"),
):
    day = "2026-08-12"
    run = root / day / run_id
    run.mkdir(parents=True)
    (run / "indicators").mkdir()
    config: dict = {"symbols": list(symbol_order)}
    config_hash = compute_config_hash(config) if schema_version == "2" else "a" * 64
    config["_config_hash"] = config_hash
    gates = V2_GATES if strategy_version == "v2-1d-4h-1h" else GATES
    first_gate = gates[0]
    decision = {
        "status": "NO_SETUP",
        "symbols": {
            symbol: {
                "decision": {
                    "failed_gate": first_gate,
                    "passed_gates": [],
                    "reason_codes": ["no_confirmed_bos"],
                    "status": "NO_SETUP",
                },
                "indicators": {
                    gate: {
                        "config_hash": config_hash,
                        "event_time": None,
                        "input_hash": ("b" if symbol == "BTCUSDT" else "c") * 64,
                        "known_at": None,
                        "reason_codes": [
                            "no_confirmed_bos" if gate == first_gate else "upstream_gate_not_passed"
                        ],
                        "reference_levels": {},
                        "status": "FAIL" if gate == first_gate else "UNAVAILABLE",
                        "value": {},
                    }
                    for gate in gates
                },
            }
            for symbol in symbol_order
        },
    }
    files = {
        "config-snapshot.json": _json_bytes(config),
        "decision.json": _json_bytes(decision),
    }
    for name, payload in files.items():
        (run / name).write_bytes(payload)
    manifest = {
        "analysis_run_id": run_id,
        "code_version": "d" * 64,
        "config_hash": config_hash,
        "cutoff": cutoff,
        "dataset_checksum": "e" * 64,
        "dataset_version": "ds-test",
        "files": {name: hashlib.sha256(payload).hexdigest() for name, payload in files.items()},
        "ingestion_run_id": "ingest-test",
    }
    if schema:
        manifest["schema_version"] = schema_version
    if strategy_version is not None:
        manifest["strategy_version"] = strategy_version
        manifest["analysis_boundary"] = ((cutoff + 1) // 3_600_000) * 3_600_000 - 1
    (run / "manifest.json").write_bytes(_json_bytes(manifest))
    if schema_version == "2":
        _sync_claim(run)
    return run


def _sync_claim(run: Path):
    manifest_path = run / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    database = run.parents[2] / "data" / "market.sqlite3"
    database.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database) as connection:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS analysis_claims(
            strategy_version TEXT, analysis_boundary INTEGER, analysis_run_id TEXT,
            status TEXT, artifact_day TEXT, manifest_sha256 TEXT,
            PRIMARY KEY(strategy_version,analysis_boundary))"""
        )
        connection.execute(
            "INSERT OR REPLACE INTO analysis_claims VALUES (?,?,?,?,?,?)",
            (
                manifest["strategy_version"],
                manifest["analysis_boundary"],
                manifest["analysis_run_id"],
                "PUBLISHED",
                run.parent.name,
                hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            ),
        )


def _rewrite_manifest_file_hash(run: Path, name: str):
    manifest_path = run / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["files"][name] = hashlib.sha256((run / name).read_bytes()).hexdigest()
    manifest_path.write_bytes(_json_bytes(manifest))
    if manifest.get("schema_version") == "2":
        _sync_claim(run)


def _set_indicator_timestamp(run: Path, gate: str, timestamp: int):
    decision_path = run / "decision.json"
    decision = json.loads(decision_path.read_text())
    for body in decision["symbols"].values():
        indicator = body["indicators"][gate]
        indicator["event_time"] = timestamp
        indicator["known_at"] = timestamp
    decision_path.write_bytes(_json_bytes(decision))
    _rewrite_manifest_file_hash(run, "decision.json")


def test_valid_runs_produce_deterministic_cases_and_summary(tmp_path: Path):
    runs = tmp_path / "runs"
    _write_run(runs, "run-b", 1_786_517_339_999, symbol_order=("ETHUSDT", "BTCUSDT"))
    _write_run(runs, "run-a", 1_786_517_339_999)

    result = build_casebook(runs, milestone_target=20)

    assert [(row["analysis_run_id"], row["symbol"]) for row in result["cases"]] == [
        ("run-a", "BTCUSDT"),
        ("run-a", "ETHUSDT"),
        ("run-b", "BTCUSDT"),
        ("run-b", "ETHUSDT"),
    ]
    assert result["summary"] == {
        "discovered_runs": 2,
        "eligible_runs": 2,
        "ineligible_runs": 0,
        "total_eligible_cases": 4,
        "active_strategy_version": "v2-1d-4h-1h",
        "eligible_cases": 0,
        "unique_dataset_cutoffs": 0,
        "unique_analysis_boundaries": 0,
        "strategy_versions": {
            "v1-4h-1h-5m": {
                "eligible_cases": 4,
                "unique_dataset_cutoffs": 1,
                "unique_analysis_boundaries": 0,
                "status_counts": {"NO_SETUP": 4},
                "failed_gate_counts": {"smc_4h_structure": 4},
                "reason_counts": {"no_confirmed_bos": 4},
                "milestone_remaining": 16,
                "milestone_reached": False,
            }
        },
        "status_counts": {},
        "failed_gate_counts": {},
        "reason_counts": {},
        "milestone_target": 20,
        "milestone_remaining": 20,
        "milestone_reached": False,
    }
    first = result["cases"][0]
    expected_identity = json.dumps(
        ["run-a", "BTCUSDT"], sort_keys=True, separators=(",", ":")
    ).encode()
    expected_id = hashlib.sha256(expected_identity).hexdigest()
    assert first["case_id"] == expected_id
    assert [step["gate"] for step in first["pipeline_steps"]] == list(GATES)
    assert first["pipeline_steps"][0] == {
        "gate": GATES[0],
        "status": "FAIL",
        "reason_codes": ["no_confirmed_bos"],
        "value": {},
        "reference_levels": {},
        "event_time": None,
        "known_at": None,
        "input_hash": "b" * 64,
        "config_hash": "a" * 64,
    }
    assert first["pipeline_steps"][1]["status"] == "UNAVAILABLE"
    assert first["pipeline_steps"][1]["reason_codes"] == ["upstream_gate_not_passed"]
    assert render_casebook(result).endswith(b"\n")
    assert render_casebook(result) == render_casebook(build_casebook(runs, milestone_target=20))


def test_casebook_human_exports_show_every_pipeline_step(tmp_path: Path):
    runs = tmp_path / "runs"
    _write_run(
        runs,
        "run-v2",
        1_786_520_939_999,
        schema_version="2",
        strategy_version="v2-1d-4h-1h",
    )

    result = build_casebook(runs)
    markdown = render_casebook_markdown(result).decode()
    csv_rows = list(csv.DictReader(io.StringIO(render_casebook_csv(result).decode())))

    assert "| Boundary (UTC) | Symbol | Decision | 1D Regime | 4H Structure |" in markdown
    assert "## Pipeline step details" in markdown
    assert "`smc_1d_regime` — **FAIL** — no_confirmed_bos" in markdown
    assert "`smc_4h_structure` — **UNAVAILABLE** — upstream_gate_not_passed" in markdown
    assert len(csv_rows) == len(V2_GATES) * 2
    assert [row["gate"] for row in csv_rows[: len(V2_GATES)]] == list(V2_GATES)
    assert csv_rows[0]["symbol"] == "BTCUSDT"
    assert csv_rows[0]["status"] == "FAIL"
    assert csv_rows[0]["reason_codes"] == "no_confirmed_bos"
    assert csv_rows[0]["value_json"] == "{}"


def test_casebook_human_exports_only_render_active_v2_cohort(tmp_path: Path):
    runs = tmp_path / "runs"
    _write_run(runs, "run-v1", 1_786_517_339_999)
    _write_run(
        runs,
        "run-v2",
        1_786_520_939_999,
        schema_version="2",
        strategy_version="v2-1d-4h-1h",
    )

    result = build_casebook(runs)
    markdown = render_casebook_markdown(result).decode()
    csv_rows = list(csv.DictReader(io.StringIO(render_casebook_csv(result).decode())))

    assert "run-v1" not in markdown
    assert "1D Regime" in markdown
    assert all(row["analysis_run_id"] == "run-v2" for row in csv_rows)
    assert len(csv_rows) == len(V2_GATES) * 2
    matrix_lines = [line for line in markdown.splitlines() if line.startswith("|")]
    assert len({line.count("|") for line in matrix_lines}) == 1


def test_casebook_keeps_v1_and_v2_denominators_separate(tmp_path: Path):
    runs = tmp_path / "runs"
    _write_run(runs, "run-v1", 1_786_517_339_999)
    _write_run(
        runs,
        "run-v2",
        1_786_520_939_999,
        schema_version="2",
        strategy_version="v2-1d-4h-1h",
    )

    result = build_casebook(runs)

    assert {row["strategy_version"] for row in result["cases"]} == {
        "v1-4h-1h-5m",
        "v2-1d-4h-1h",
    }
    v2_cases = [row for row in result["cases"] if row["strategy_version"] == "v2-1d-4h-1h"]
    assert {row["analysis_boundary"] for row in v2_cases} == {1_786_517_999_999}
    assert result["summary"]["strategy_versions"] == {
        "v1-4h-1h-5m": {
            "eligible_cases": 2,
            "unique_dataset_cutoffs": 1,
            "unique_analysis_boundaries": 0,
            "status_counts": {"NO_SETUP": 2},
            "failed_gate_counts": {"smc_4h_structure": 2},
            "reason_counts": {"no_confirmed_bos": 2},
            "milestone_remaining": 18,
            "milestone_reached": False,
        },
        "v2-1d-4h-1h": {
            "eligible_cases": 2,
            "unique_dataset_cutoffs": 1,
            "unique_analysis_boundaries": 1,
            "status_counts": {"NO_SETUP": 2},
            "failed_gate_counts": {"smc_1d_regime": 2},
            "reason_counts": {"no_confirmed_bos": 2},
            "milestone_remaining": 18,
            "milestone_reached": False,
        },
    }
    assert result["summary"]["total_eligible_cases"] == 4
    assert result["summary"]["active_strategy_version"] == "v2-1d-4h-1h"
    assert result["summary"]["eligible_cases"] == 2
    assert result["summary"]["unique_analysis_boundaries"] == 1
    assert result["summary"]["milestone_reached"] is False


def test_v2_casebook_requires_published_claim_and_exact_manifest_commitment(tmp_path: Path):
    runs = tmp_path / "runs"
    run = _write_run(
        runs,
        "run-claim",
        1_786_520_939_999,
        schema_version="2",
        strategy_version="v2-1d-4h-1h",
    )
    database = tmp_path / "data" / "market.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE analysis_claims SET status='CLAIMED'")
    with pytest.raises(EvidenceError, match="published claim mismatch"):
        build_casebook(runs)

    _sync_claim(run)
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE analysis_claims SET manifest_sha256=?", ("0" * 64,))
    with pytest.raises(EvidenceError, match="published claim mismatch"):
        build_casebook(runs)


def test_v2_casebook_recomputes_governed_config_hash(tmp_path: Path):
    runs = tmp_path / "runs"
    run = _write_run(
        runs,
        "run-config-tamper",
        1_786_520_939_999,
        schema_version="2",
        strategy_version="v2-1d-4h-1h",
    )
    config_path = run / "config-snapshot.json"
    config = json.loads(config_path.read_text())
    config["bootstrap_bars"] = 999999
    config_path.write_bytes(_json_bytes(config))
    _rewrite_manifest_file_hash(run, "config-snapshot.json")

    with pytest.raises(EvidenceError, match="config snapshot content hash mismatch"):
        build_casebook(runs)


def test_schema_v2_rejects_unknown_strategy_version(tmp_path: Path):
    runs = tmp_path / "runs"
    _write_run(
        runs,
        "run-v2",
        1_786_520_939_999,
        schema_version="2",
        strategy_version="unknown",
    )
    with pytest.raises(EvidenceError, match="strategy_version"):
        build_casebook(runs)


def test_casebook_rejects_future_or_misaligned_indicator_timestamps(tmp_path: Path):
    runs = tmp_path / "runs"
    future = _write_run(
        runs,
        "run-future",
        1_786_520_939_999,
        schema_version="2",
        strategy_version="v2-1d-4h-1h",
    )
    _set_indicator_timestamp(future, "smc_1d_regime", 1_786_521_599_999)
    with pytest.raises(EvidenceError, match="exceeds analysis boundary"):
        build_casebook(runs)

    for path in future.iterdir():
        if path.is_file():
            path.unlink()
        else:
            path.rmdir()
    future.rmdir()
    misaligned = _write_run(
        runs,
        "run-misaligned",
        1_786_520_939_999,
        schema_version="2",
        strategy_version="v2-1d-4h-1h",
    )
    _set_indicator_timestamp(misaligned, "smc_1d_regime", 1_786_517_999_999)
    with pytest.raises(EvidenceError, match="timeframe-close aligned"):
        build_casebook(runs)


def test_casebook_rejects_future_v1_indicator_timestamp(tmp_path: Path):
    runs = tmp_path / "runs"
    run = _write_run(runs, "run-v1-future", 1_786_517_339_999)
    _set_indicator_timestamp(run, "smc_4h_structure", 1_786_521_599_999)
    with pytest.raises(EvidenceError, match="exceeds analysis boundary"):
        build_casebook(runs)


def test_legacy_run_is_verified_then_excluded(tmp_path: Path):
    runs = tmp_path / "runs"
    legacy = _write_run(runs, "run-legacy", 1_786_517_339_999, schema=False)

    result = build_casebook(runs)

    assert result["cases"] == []
    assert result["exclusions"] == [
        {"path": str(legacy.relative_to(runs)), "reason": "LEGACY_SCHEMA"}
    ]
    assert result["summary"]["ineligible_runs"] == 1


def test_declared_artifact_tampering_fails_closed(tmp_path: Path):
    runs = tmp_path / "runs"
    run = _write_run(runs, "run-tampered", 1_786_517_339_999)
    (run / "decision.json").write_bytes(b"{}\n")

    with pytest.raises(EvidenceError, match="decision.json hash mismatch"):
        build_casebook(runs)


def test_nonempty_indicators_directory_fails_closed(tmp_path: Path):
    runs = tmp_path / "runs"
    run = _write_run(runs, "run-extra-indicator", 1_786_517_339_999)
    (run / "indicators" / "undeclared.json").write_text("{}\n")
    with pytest.raises(EvidenceError, match="indicators directory must be empty"):
        build_casebook(runs)


def test_run_identity_and_symlinked_artifact_fail_closed(tmp_path: Path):
    runs = tmp_path / "runs"
    run = _write_run(runs, "run-path", 1_786_517_339_999)
    manifest_path = run / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["analysis_run_id"] = "run-other"
    manifest_path.write_bytes(_json_bytes(manifest))
    with pytest.raises(EvidenceError, match="analysis_run_id"):
        build_casebook(runs)

    manifest["analysis_run_id"] = "run-path"
    manifest_path.write_bytes(_json_bytes(manifest))
    decision = run / "decision.json"
    external = tmp_path / "external.json"
    external.write_bytes(decision.read_bytes())
    decision.unlink()
    decision.symlink_to(external)
    with pytest.raises(EvidenceError, match="symlink"):
        build_casebook(runs)


def test_publication_is_atomic_deterministic_and_read_only(tmp_path: Path):
    runs = tmp_path / "runs"
    _write_run(runs, "run-case", 1_786_517_339_999)
    output = tmp_path / "evidence" / "casebook.json"
    before = {
        str(path.relative_to(runs)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in runs.rglob("*.json")
    }

    first = publish_casebook(runs, output, milestone_target=30)
    first_bytes = output.read_bytes()
    second = publish_casebook(runs, output, milestone_target=30)

    assert (
        first
        == second
        == {
            "output": str(output),
            "sha256": hashlib.sha256(first_bytes).hexdigest(),
            "eligible_cases": 0,
            "milestone_target": 30,
            "milestone_remaining": 30,
            "milestone_reached": False,
        }
    )
    assert output.read_bytes() == first_bytes
    assert not list(output.parent.glob(".casebook.json.*.tmp"))
    after = {
        str(path.relative_to(runs)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in runs.rglob("*.json")
    }
    assert after == before


def test_casebook_cli_publishes_machine_result(tmp_path: Path, capsys):
    runs = tmp_path / "runs"
    _write_run(runs, "run-cli", 1_786_517_339_999)
    output = tmp_path / "casebook.json"

    assert (
        main(
            [
                "casebook",
                "--runs-root",
                str(runs),
                "--output",
                str(output),
                "--milestone-target",
                "20",
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert result["output"] == str(output)
    assert result["eligible_cases"] == 0
    assert result["sha256"] == hashlib.sha256(output.read_bytes()).hexdigest()


def test_casebook_cli_publishes_json_and_immutable_human_snapshot(tmp_path: Path):
    runs = tmp_path / "runs"
    _write_run(
        runs,
        "run-v2",
        1_786_520_939_999,
        schema_version="2",
        strategy_version="v2-1d-4h-1h",
    )
    output = tmp_path / "casebook.json"
    snapshot = tmp_path / "snapshots" / "1786521599999"

    assert (
        main(
            [
                "casebook",
                "--runs-root",
                str(runs),
                "--output",
                str(output),
                "--snapshot-output",
                str(snapshot),
            ]
        )
        == 0
    )

    assert json.loads(output.read_text())["cases"][0]["pipeline_steps"][0]["gate"] == V2_GATES[0]
    assert "## Hourly pipeline matrix" in (snapshot / "casebook.md").read_text()
    csv_rows = list(csv.DictReader((snapshot / "casebook.csv").read_text().splitlines()))
    assert len(csv_rows) == len(V2_GATES) * 2
    assert json.loads((snapshot / "casebook.json").read_text()) == json.loads(output.read_text())
    assert not list(snapshot.parent.glob(".*.tmp"))


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda manifest: manifest.update(cutoff=True), "invalid cutoff"),
        (lambda manifest: manifest.update(schema_version="2"), "schema-v2 manifest key mismatch"),
        (lambda manifest: manifest.update(extra="value"), "manifest key mismatch"),
    ],
)
def test_manifest_wire_contract_fails_closed(tmp_path: Path, mutation, message):
    runs = tmp_path / "runs"
    run = _write_run(runs, "run-wire", 1_786_517_339_999)
    manifest_path = run / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    mutation(manifest)
    manifest_path.write_bytes(_json_bytes(manifest))

    with pytest.raises(EvidenceError, match=message):
        build_casebook(runs)


def test_run_and_symbol_status_must_be_semantically_consistent(tmp_path: Path):
    runs = tmp_path / "runs"
    run = _write_run(runs, "run-status", 1_786_517_339_999)
    decision_path = run / "decision.json"
    decision = json.loads(decision_path.read_text())
    decision["status"] = "TRADE"
    decision_path.write_bytes(_json_bytes(decision))
    _rewrite_manifest_file_hash(run, "decision.json")

    with pytest.raises(EvidenceError, match="run status disagrees"):
        build_casebook(runs)


def test_indicator_config_and_input_hashes_fail_closed(tmp_path: Path):
    runs = tmp_path / "runs"
    run = _write_run(runs, "run-indicator", 1_786_517_339_999)
    decision_path = run / "decision.json"
    decision = json.loads(decision_path.read_text())
    decision["symbols"]["BTCUSDT"]["indicators"]["smc_4h_structure"]["config_hash"] = "f" * 64
    decision_path.write_bytes(_json_bytes(decision))
    _rewrite_manifest_file_hash(run, "decision.json")
    with pytest.raises(EvidenceError, match="indicator config hash"):
        build_casebook(runs)

    decision["symbols"]["BTCUSDT"]["indicators"]["smc_4h_structure"]["config_hash"] = "a" * 64
    decision["symbols"]["BTCUSDT"]["indicators"]["smc_4h_structure"]["input_hash"] = "bad"
    decision_path.write_bytes(_json_bytes(decision))
    _rewrite_manifest_file_hash(run, "decision.json")
    with pytest.raises(EvidenceError, match="indicator input hash"):
        build_casebook(runs)


def test_output_symlink_and_publish_failure_leave_no_temporary_file(tmp_path: Path, monkeypatch):
    runs = tmp_path / "runs"
    _write_run(runs, "run-output", 1_786_517_339_999)
    external = tmp_path / "external.json"
    external.write_bytes(b"preserve")
    output = tmp_path / "casebook.json"
    output.symlink_to(external)
    with pytest.raises(EvidenceError, match="symlink"):
        publish_casebook(runs, output)
    assert external.read_bytes() == b"preserve"

    output.unlink()

    def fail_replace(*_, **__):
        raise OSError("injected replace failure")

    monkeypatch.setattr("smc_ict.casebook.os.replace", fail_replace)
    with pytest.raises(OSError, match="injected"):
        publish_casebook(runs, output)
    assert not output.exists()
    assert not list(tmp_path.glob(".casebook.json.*.tmp"))


def test_extra_run_artifact_fails_closed(tmp_path: Path):
    runs = tmp_path / "runs"
    run = _write_run(runs, "run-extra", 1_786_517_339_999)
    (run / "unexpected.json").write_text("{}\n")

    with pytest.raises(EvidenceError, match="artifact set"):
        build_casebook(runs)


def test_symlinked_output_parent_fails_closed(tmp_path: Path):
    runs = tmp_path / "runs"
    _write_run(runs, "run-parent", 1_786_517_339_999)
    external = tmp_path / "external"
    external.mkdir()
    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(external, target_is_directory=True)

    with pytest.raises(EvidenceError, match="symlink"):
        publish_casebook(runs, linked_parent / "casebook.json")
    assert not (external / "casebook.json").exists()


@pytest.mark.parametrize("level", ["day", "run"])
def test_dangling_source_directory_symlink_fails_closed(tmp_path: Path, level):
    runs = tmp_path / "runs"
    runs.mkdir()
    if level == "day":
        (runs / "2026-08-12").symlink_to(tmp_path / "missing-day", target_is_directory=True)
    else:
        day = runs / "2026-08-12"
        day.mkdir()
        (day / "run-missing").symlink_to(tmp_path / "missing-run", target_is_directory=True)

    with pytest.raises(EvidenceError, match="symlink"):
        build_casebook(runs)


def test_config_symbols_must_match_decision_symbols(tmp_path: Path):
    runs = tmp_path / "runs"
    run = _write_run(runs, "run-symbols", 1_786_517_339_999)
    config_path = run / "config-snapshot.json"
    config = json.loads(config_path.read_text())
    config["symbols"] = ["XRPUSDT"]
    config_path.write_bytes(_json_bytes(config))
    _rewrite_manifest_file_hash(run, "config-snapshot.json")

    with pytest.raises(EvidenceError, match="config symbols"):
        build_casebook(runs)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda body: body["decision"].update(failed_gate="missing_gate"),
        lambda body: body["decision"].update(passed_gates=["missing_gate"]),
        lambda body: body["decision"].update(passed_gates=[GATES[1]]),
        lambda body: body["indicators"][GATES[0]].update(status="PASS"),
    ],
)
def test_gate_decision_must_match_authoritative_state_machine(tmp_path: Path, mutate):
    runs = tmp_path / "runs"
    run = _write_run(runs, "run-gates", 1_786_517_339_999)
    decision_path = run / "decision.json"
    decision = json.loads(decision_path.read_text())
    mutate(decision["symbols"]["BTCUSDT"])
    decision_path.write_bytes(_json_bytes(decision))
    _rewrite_manifest_file_hash(run, "decision.json")

    with pytest.raises(EvidenceError, match="gate decision"):
        build_casebook(runs)


def test_directory_fsync_failure_restores_existing_output(tmp_path: Path, monkeypatch):
    runs = tmp_path / "runs"
    _write_run(runs, "run-fsync", 1_786_517_339_999)
    output = tmp_path / "casebook.json"
    output.write_bytes(b"known-good-output")
    real_fsync = __import__("smc_ict.casebook", fromlist=["os"]).os.fsync
    calls = 0

    def fail_directory_fsync(descriptor):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected directory fsync failure")
        return real_fsync(descriptor)

    monkeypatch.setattr("smc_ict.casebook.os.fsync", fail_directory_fsync)
    with pytest.raises(OSError, match="injected directory fsync"):
        publish_casebook(runs, output)
    assert output.read_bytes() == b"known-good-output"
    assert not list(tmp_path.glob(".casebook.json.*.tmp"))
    assert not (tmp_path / ".casebook.json.rollback").exists()


def test_immutable_snapshot_conflict_fails_closed(tmp_path: Path):
    runs = tmp_path / "runs"
    _write_run(
        runs,
        "run-v2",
        1_786_520_939_999,
        schema_version="2",
        strategy_version="v2-1d-4h-1h",
    )
    output = tmp_path / "casebook.json"
    snapshot = tmp_path / "snapshots" / "1786521599999"
    snapshot.mkdir(parents=True)
    (snapshot / "casebook.json").write_bytes(b"conflicting-generation")

    with pytest.raises(EvidenceError, match="immutable casebook snapshot conflict"):
        publish_casebook(runs, output, 20, snapshot)

    assert (snapshot / "casebook.json").read_bytes() == b"conflicting-generation"
    assert not list(snapshot.parent.glob(".*.tmp"))


def test_snapshot_directory_appears_only_after_all_formats_are_complete(
    tmp_path: Path, monkeypatch
):
    runs = tmp_path / "runs"
    _write_run(
        runs,
        "run-v2",
        1_786_520_939_999,
        schema_version="2",
        strategy_version="v2-1d-4h-1h",
    )
    output = tmp_path / "casebook.json"
    snapshot = tmp_path / "snapshots" / "1786521599999"
    entered = threading.Event()
    release = threading.Event()
    real_rename = __import__("smc_ict.casebook", fromlist=["os"]).os.rename

    def blocked_snapshot_rename(source, destination):
        entered.set()
        release.wait(timeout=5)
        return real_rename(source, destination)

    monkeypatch.setattr("smc_ict.casebook.os.rename", blocked_snapshot_rename)
    errors = []

    def publish():
        try:
            publish_casebook(runs, output, 20, snapshot)
        except Exception as error:
            errors.append(error)

    publisher = threading.Thread(target=publish)
    publisher.start()
    assert entered.wait(timeout=5)
    assert not snapshot.exists()
    release.set()
    publisher.join(timeout=5)

    assert not errors
    assert {path.name for path in snapshot.iterdir()} == {
        "casebook.json",
        "casebook.md",
        "casebook.csv",
    }


def test_directory_fsync_failure_removes_new_output(tmp_path: Path, monkeypatch):
    runs = tmp_path / "runs"
    _write_run(runs, "run-new-fsync", 1_786_517_339_999)
    output = tmp_path / "casebook.json"
    real_fsync = __import__("smc_ict.casebook", fromlist=["os"]).os.fsync
    calls = 0

    def fail_publish_fsync(descriptor):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected new-output fsync failure")
        return real_fsync(descriptor)

    monkeypatch.setattr("smc_ict.casebook.os.fsync", fail_publish_fsync)
    with pytest.raises(OSError, match="new-output fsync"):
        publish_casebook(runs, output)
    assert not output.exists()
    assert not list(tmp_path.glob(".casebook.json.*.tmp"))


def test_recovery_failure_preserves_primary_publication_error(tmp_path: Path, monkeypatch):
    runs = tmp_path / "runs"
    _write_run(runs, "run-recovery", 1_786_517_339_999)
    output = tmp_path / "casebook.json"
    output.write_bytes(b"known-good-output")
    calls = 0

    def fail_both_directory_fsyncs(_descriptor):
        nonlocal calls
        calls += 1
        if calls == 1:
            return None
        if calls == 2:
            raise OSError("primary publication fsync failure")
        raise OSError("secondary recovery fsync failure")

    monkeypatch.setattr("smc_ict.casebook.os.fsync", fail_both_directory_fsyncs)
    with pytest.raises(OSError, match="primary publication") as captured:
        publish_casebook(runs, output)
    assert output.is_file() and not output.is_symlink()
    assert output.read_bytes() in {
        b"known-good-output",
        render_casebook(build_casebook(runs)),
    }
    assert any("recovery failed" in note for note in captured.value.__notes__)


def test_indicators_directory_is_optional(tmp_path: Path):
    runs = tmp_path / "runs"
    run = _write_run(runs, "run-no-indicator-dir", 1_786_517_339_999)
    (run / "indicators").rmdir()

    assert build_casebook(runs)["summary"]["eligible_cases"] == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [("status", False), ("reason_codes", "not-a-list"), ("input_hash", None)],
)
def test_all_downstream_indicator_fields_are_validated(tmp_path: Path, field, value):
    runs = tmp_path / "runs"
    run = _write_run(runs, "run-downstream", 1_786_517_339_999)
    decision_path = run / "decision.json"
    decision = json.loads(decision_path.read_text())
    decision["symbols"]["BTCUSDT"]["indicators"][GATES[-1]][field] = value
    decision_path.write_bytes(_json_bytes(decision))
    _rewrite_manifest_file_hash(run, "decision.json")

    with pytest.raises(EvidenceError, match="indicator"):
        build_casebook(runs)


def test_publication_does_not_use_persistent_rollback_path(tmp_path: Path):
    runs = tmp_path / "runs"
    _write_run(runs, "run-no-rollback", 1_786_517_339_999)
    output = tmp_path / "casebook.json"
    output.write_bytes(b"old")

    publish_casebook(runs, output)

    assert not (tmp_path / ".casebook.json.rollback").exists()


def test_concurrent_publications_to_same_output_fail_closed(tmp_path: Path, monkeypatch):
    runs = tmp_path / "runs"
    _write_run(runs, "run-concurrent", 1_786_517_339_999)
    output = tmp_path / "casebook.json"
    entered = threading.Event()
    release = threading.Event()
    real_build = __import__("smc_ict.casebook", fromlist=["build_casebook"]).build_casebook

    def blocked_build(*args, **kwargs):
        entered.set()
        release.wait(timeout=5)
        return real_build(*args, **kwargs)

    monkeypatch.setattr("smc_ict.casebook.build_casebook", blocked_build)
    errors = []

    def publish():
        try:
            publish_casebook(runs, output)
        except Exception as error:
            errors.append(type(error).__name__)

    first = threading.Thread(target=publish)
    first.start()
    assert entered.wait(timeout=5)
    second = threading.Thread(target=publish)
    second.start()
    second.join(timeout=5)
    release.set()
    first.join(timeout=5)

    assert errors == ["EvidenceError"]
    assert output.is_file()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda indicator: indicator.update(extra="forbidden"),
        lambda indicator: indicator.pop("value"),
        lambda indicator: indicator.update(value=[]),
        lambda indicator: indicator.update(event_time=True),
        lambda indicator: indicator.update(event_time=-1),
        lambda indicator: indicator.update(known_at="1"),
        lambda indicator: indicator.update(event_time=2, known_at=1),
        lambda indicator: indicator.update(reference_levels=[]),
        lambda indicator: indicator.update(reference_levels={"level": 1}),
    ],
)
def test_indicator_exact_wire_schema_fails_closed(tmp_path: Path, mutate):
    runs = tmp_path / "runs"
    run = _write_run(runs, "run-wire-fields", 1_786_517_339_999)
    decision_path = run / "decision.json"
    decision = json.loads(decision_path.read_text())
    mutate(decision["symbols"]["BTCUSDT"]["indicators"][GATES[-1]])
    decision_path.write_bytes(_json_bytes(decision))
    _rewrite_manifest_file_hash(run, "decision.json")
    with pytest.raises(EvidenceError, match="indicator"):
        build_casebook(runs)


def test_lock_symlink_failure_does_not_leak_parent_fd(tmp_path: Path):
    runs = tmp_path / "runs"
    _write_run(runs, "run-lock-link", 1_786_517_339_999)
    output = tmp_path / "casebook.json"
    external = tmp_path / "external.lock"
    external.write_bytes(b"")
    (tmp_path / ".casebook.json.lock").symlink_to(external)
    before = len(os.listdir("/proc/self/fd"))

    for _ in range(25):
        with pytest.raises(OSError):
            publish_casebook(runs, output)

    assert len(os.listdir("/proc/self/fd")) <= before


def test_lock_open_failure_does_not_leak_parent_fd(tmp_path: Path, monkeypatch):
    runs = tmp_path / "runs"
    _write_run(runs, "run-lock-open", 1_786_517_339_999)
    output = tmp_path / "casebook.json"
    real_open = __import__("smc_ict.casebook", fromlist=["os"]).os.open

    def fail_lock_open(path, *args, **kwargs):
        if path == ".casebook.json.lock":
            raise OSError("injected lock open failure")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr("smc_ict.casebook.os.open", fail_lock_open)
    before = len(os.listdir("/proc/self/fd"))
    with pytest.raises(OSError, match="injected lock"):
        publish_casebook(runs, output)
    assert len(os.listdir("/proc/self/fd")) <= before


def test_unlock_failure_preserves_primary_exception_and_closes_fds(tmp_path: Path, monkeypatch):
    runs = tmp_path / "runs"
    _write_run(runs, "run-primary-cleanup", 1_786_517_339_999)
    output = tmp_path / "casebook.json"
    primary = KeyboardInterrupt("primary body failure")
    real_flock = __import__("smc_ict.casebook", fromlist=["fcntl"]).fcntl.flock

    def fail_build(*_args, **_kwargs):
        raise primary

    def fail_unlock(fd, operation):
        if operation == __import__("fcntl").LOCK_UN:
            raise RuntimeError("injected unlock failure")
        return real_flock(fd, operation)

    monkeypatch.setattr("smc_ict.casebook.build_casebook", fail_build)
    monkeypatch.setattr("smc_ict.casebook.fcntl.flock", fail_unlock)
    before = len(os.listdir("/proc/self/fd"))
    with pytest.raises(KeyboardInterrupt) as captured:
        publish_casebook(runs, output)
    assert captured.value is primary
    assert any("cleanup failed" in note for note in primary.__notes__)
    assert len(os.listdir("/proc/self/fd")) <= before


def test_unlock_failure_after_success_closes_fds_then_raises(tmp_path: Path, monkeypatch):
    runs = tmp_path / "runs"
    _write_run(runs, "run-success-cleanup", 1_786_517_339_999)
    output = tmp_path / "casebook.json"
    real_flock = __import__("smc_ict.casebook", fromlist=["fcntl"]).fcntl.flock

    def fail_unlock(fd, operation):
        if operation == __import__("fcntl").LOCK_UN:
            raise RuntimeError("injected unlock failure")
        return real_flock(fd, operation)

    monkeypatch.setattr("smc_ict.casebook.fcntl.flock", fail_unlock)
    before = len(os.listdir("/proc/self/fd"))
    with pytest.raises(RuntimeError, match="injected unlock"):
        publish_casebook(runs, output)
    assert len(os.listdir("/proc/self/fd")) <= before
