import json
from pathlib import Path

from smc_ict.config import AppConfig
from smc_ict.data.binance import FixtureBinanceClient
from smc_ict.pipeline.orchestrator import Orchestrator


def test_manifest_has_reproducibility_identity_and_dataset_checksum(tmp_path: Path):
    result = Orchestrator(
        AppConfig(data_root=tmp_path, bootstrap_bars=300), FixtureBinanceClient()
    ).run_once()
    manifest = json.loads((result.run_dir / "manifest.json").read_text())
    assert manifest["schema_version"] == "1"
    assert manifest["ingestion_run_id"].startswith("ingest-")
    assert manifest["code_version"]
    assert len(manifest["dataset_checksum"]) == 64
    assert (
        manifest["config_hash"]
        == json.loads((result.run_dir / "config-snapshot.json").read_text())["_config_hash"]
    )
    decisions = json.loads((result.run_dir / "decision.json").read_text())
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
