import json
import shutil
from pathlib import Path

from smc_ict.config import AppConfig
from smc_ict.data.binance import FixtureBinanceClient
from smc_ict.pipeline.orchestrator import Orchestrator


def test_manifest_has_reproducibility_identity_and_dataset_checksum(tmp_path: Path):
    result = Orchestrator(
        AppConfig(data_root=tmp_path, bootstrap_bars=300), FixtureBinanceClient()
    ).run_once()
    manifest = json.loads((result.run_dir / "manifest.json").read_text())
    assert manifest["schema_version"] == "2"
    assert manifest["strategy_version"] == "v2-1d-4h-1h"
    assert manifest["analysis_boundary"] == 17_999_999
    assert manifest["ingestion_run_id"].startswith("ingest-")
    assert manifest["code_version"]
    assert len(manifest["dataset_checksum"]) == 64
    assert (
        manifest["config_hash"]
        == json.loads((result.run_dir / "config-snapshot.json").read_text())["_config_hash"]
    )
    decisions = json.loads((result.run_dir / "decision.json").read_text())
    assert set(decisions["symbols"]["BTCUSDT"]["indicators"]) == {
        "smc_1d_regime",
        "smc_4h_structure",
        "smc_4h_dealing_range",
        "smc_4h_order_block",
        "ict_1h_liquidity",
        "ict_1h_displacement",
        "ict_1h_mss",
        "ict_1h_fvg",
        "risk",
    }
    for symbol in decisions["symbols"].values():
        for indicator in symbol["indicators"].values():
            assert indicator["config_hash"] == manifest["config_hash"]
            assert len(indicator["input_hash"]) == 64
    import sqlite3

    with sqlite3.connect(tmp_path / "data" / "market.sqlite3") as connection:
        assert (
            connection.execute(
                "SELECT status FROM ingestion_runs WHERE ingestion_run_id=?",
                (manifest["ingestion_run_id"],),
            ).fetchone()[0]
            == "COMMITTED"
        )
        assert (
            connection.execute(
                "SELECT status FROM datasets WHERE version=?",
                (manifest["dataset_version"],),
            ).fetchone()[0]
            == "COMMITTED"
        )


def test_public_run_status_aggregates_symbol_decisions():
    from smc_ict.pipeline.orchestrator import aggregate_status

    assert (
        aggregate_status(
            {"BTC": {"decision": {"status": "TRADE"}}, "ETH": {"decision": {"status": "NO_SETUP"}}}
        )
        == "TRADE"
    )
    assert (
        aggregate_status(
            {
                "BTC": {"decision": {"status": "BLOCKED"}},
                "ETH": {"decision": {"status": "NO_SETUP"}},
            }
        )
        == "BLOCKED"
    )


def test_v2_analysis_is_idempotent_per_closed_one_hour_boundary(tmp_path: Path):
    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=300)
    orchestrator = Orchestrator(cfg, FixtureBinanceClient())
    orchestrator.ingest_once()

    first = orchestrator.analyze_latest()
    second = orchestrator.analyze_latest()

    assert first.analysis_run_id
    assert second.status == "SKIPPED_ALREADY_ANALYZED"
    assert second.analysis_run_id == first.analysis_run_id
    assert second.run_dir == first.run_dir


def test_v2_dedup_rejects_tampered_existing_artifact(tmp_path: Path):
    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=300)
    orchestrator = Orchestrator(cfg, FixtureBinanceClient())
    orchestrator.ingest_once()
    first = orchestrator.analyze_latest()
    assert first.run_dir is not None
    decision = first.run_dir / "decision.json"
    decision.write_text(decision.read_text() + " ")

    second = orchestrator.analyze_latest()

    assert second.status == "FAILED"
    assert second.error is not None and "hash mismatch" in second.error


def test_v2_dedup_rejects_duplicate_valid_boundary_runs(tmp_path: Path):
    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=300)
    orchestrator = Orchestrator(cfg, FixtureBinanceClient())
    orchestrator.ingest_once()
    first = orchestrator.analyze_latest()
    assert first.run_dir is not None
    duplicate = first.run_dir.with_name("run-duplicate")
    shutil.copytree(first.run_dir, duplicate)
    manifest_path = duplicate / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["analysis_run_id"] = duplicate.name
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")

    second = orchestrator.analyze_latest()

    assert second.status == "FAILED"
    assert second.error is not None and "duplicate v2 analysis boundary" in second.error
