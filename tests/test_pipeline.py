import json
from pathlib import Path

from smc_ict.config import AppConfig
from smc_ict.data.binance import FixtureBinanceClient
from smc_ict.pipeline.orchestrator import Orchestrator


def test_end_to_end_publishes_after_commit_and_never_overwrites(tmp_path: Path):
    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=300)
    o = Orchestrator(cfg, FixtureBinanceClient())
    first = o.run_once()
    second = o.run_once()
    assert first.dataset_version and first.analysis_run_id != second.analysis_run_id
    assert first.run_dir.exists() and second.run_dir.exists()
    m = json.loads((first.run_dir / "manifest.json").read_text())
    assert m["dataset_version"] == first.dataset_version
    assert (first.run_dir / "decision.json").exists()
    decision = json.loads((first.run_dir / "decision.json").read_text())
    assert set(decision["symbols"]["BTCUSDT"]["indicators"]) == {
        "smc_4h_structure",
        "smc_1h_dealing_range",
        "smc_1h_order_block",
        "ict_5m_liquidity",
        "ict_5m_displacement",
        "ict_5m_mss",
        "ict_5m_fvg",
        "risk",
    }
    assert m["cutoff"] == 17_999_999


def test_fixture_failure_does_not_run_indicators(tmp_path: Path):
    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=300)
    result = Orchestrator(cfg, FixtureBinanceClient(fail_symbol="ETHUSDT")).run_once()
    assert result.status == "FAILED" and result.dataset_version is None
    assert not (tmp_path / "runs").exists()
    import sqlite3

    with sqlite3.connect(tmp_path / "data" / "market.sqlite3") as connection:
        assert (
            connection.execute(
                "SELECT status FROM ingestion_runs ORDER BY started_at DESC LIMIT 1"
            ).fetchone()[0]
            == "FAILED"
        )


def test_analysis_failure_does_not_relabel_committed_ingestion(tmp_path: Path):
    import sqlite3

    def broken_analysis(*args):
        raise RuntimeError("analysis failure")

    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=300)
    result = Orchestrator(cfg, FixtureBinanceClient(), analyzer=broken_analysis).run_once()
    assert result.status == "FAILED" and "analysis failure" in result.error
    with sqlite3.connect(tmp_path / "data" / "market.sqlite3") as connection:
        assert (
            connection.execute(
                "SELECT status FROM ingestion_runs ORDER BY started_at DESC LIMIT 1"
            ).fetchone()[0]
            == "COMMITTED"
        )
        assert connection.execute("SELECT status FROM datasets").fetchone()[0] == "COMMITTED"


def test_server_time_failure_is_audited(tmp_path: Path):
    import sqlite3

    class BrokenClock:
        def latest_closed_cutoff(self):
            raise RuntimeError("clock unavailable")

    result = Orchestrator(AppConfig(data_root=tmp_path), BrokenClock()).run_once()
    assert result.status == "FAILED"
    with sqlite3.connect(tmp_path / "data" / "market.sqlite3") as connection:
        status, cutoff = connection.execute(
            "SELECT status,cutoff FROM ingestion_runs LIMIT 1"
        ).fetchone()
    assert status == "FAILED" and cutoff is None
