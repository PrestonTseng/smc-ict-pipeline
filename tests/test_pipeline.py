import json
from pathlib import Path

import pytest

from smc_ict.config import AppConfig
from smc_ict.data.binance import FixtureBinanceClient
from smc_ict.pipeline.orchestrator import Orchestrator
from smc_ict.storage import MarketRepository, ValidationError


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


def test_ingest_once_commits_without_analysis_or_run_artifact(tmp_path: Path):
    def forbidden_analysis(*_args):
        raise AssertionError("ingestion-only path must not analyze")

    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=300)
    result = Orchestrator(cfg, FixtureBinanceClient(), analyzer=forbidden_analysis).ingest_once()
    assert result.status == "COMMITTED"
    assert result.dataset_version
    assert result.analysis_run_id is None
    assert not (tmp_path / "runs").exists()


def test_analyze_latest_uses_committed_snapshot_without_new_ingestion(tmp_path: Path):
    import sqlite3

    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=300)
    orchestrator = Orchestrator(cfg, FixtureBinanceClient())
    ingested = orchestrator.ingest_once()
    with sqlite3.connect(tmp_path / "data" / "market.sqlite3") as connection:
        before = connection.execute("SELECT count(*) FROM ingestion_runs").fetchone()[0]
    analyzed = Orchestrator(cfg, client=None).analyze_latest()
    with sqlite3.connect(tmp_path / "data" / "market.sqlite3") as connection:
        after = connection.execute("SELECT count(*) FROM ingestion_runs").fetchone()[0]
    assert analyzed.dataset_version == ingested.dataset_version
    assert analyzed.analysis_run_id and analyzed.run_dir.exists()
    assert after == before


def test_analyze_latest_fails_closed_without_committed_dataset(tmp_path: Path):
    result = Orchestrator(AppConfig(data_root=tmp_path), client=None).analyze_latest()
    assert result.status == "FAILED"
    assert result.dataset_version is None
    assert "no committed dataset" in result.error


def test_analysis_provenance_is_bound_to_dataset_not_shared_cutoff(tmp_path: Path):
    import sqlite3

    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=300)
    orchestrator = Orchestrator(cfg, FixtureBinanceClient())
    ingested = orchestrator.ingest_once()
    with sqlite3.connect(tmp_path / "data" / "market.sqlite3") as connection:
        connection.execute(
            """INSERT INTO ingestion_runs(
                ingestion_run_id,status,cutoff,finished_at
            ) VALUES ('ingest-forged','COMMITTED',?,CURRENT_TIMESTAMP)""",
            (FixtureBinanceClient().latest_closed_cutoff(),),
        )
    analyzed = Orchestrator(cfg, client=None).analyze_latest()
    manifest = json.loads((analyzed.run_dir / "manifest.json").read_text())
    assert manifest["dataset_version"] == ingested.dataset_version
    assert manifest["ingestion_run_id"] == ingested.ingestion_run_id


def test_ingestion_dataset_version_is_foreign_key_bound(tmp_path: Path):
    import sqlite3

    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=300)
    ingested = Orchestrator(cfg, FixtureBinanceClient()).ingest_once()
    with sqlite3.connect(tmp_path / "data" / "market.sqlite3") as connection:
        row = connection.execute(
            "SELECT dataset_version FROM ingestion_runs WHERE ingestion_run_id=?",
            (ingested.ingestion_run_id,),
        ).fetchone()
        foreign_keys = connection.execute("PRAGMA foreign_key_list(ingestion_runs)").fetchall()
    assert row == (ingested.dataset_version,)
    assert any(key[2] == "datasets" and key[3] == "dataset_version" for key in foreign_keys)


def test_wrong_cutoff_bound_ingestion_cannot_replace_dataset_origin(tmp_path: Path):
    import sqlite3

    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=300)
    ingested = Orchestrator(cfg, FixtureBinanceClient()).ingest_once()
    with sqlite3.connect(tmp_path / "data" / "market.sqlite3") as connection:
        connection.execute("DROP INDEX IF EXISTS one_ingestion_origin_per_dataset")
        connection.execute(
            """INSERT INTO ingestion_runs(
                ingestion_run_id,status,cutoff,dataset_version,finished_at
            ) VALUES ('ingest-wrong-cutoff','COMMITTED',?,?,CURRENT_TIMESTAMP)""",
            (FixtureBinanceClient().latest_closed_cutoff() - 60_000, ingested.dataset_version),
        )
    before_runs = (
        set((tmp_path / "runs").glob("*/run-*")) if (tmp_path / "runs").exists() else set()
    )
    analyzed = Orchestrator(cfg, client=None).analyze_latest()
    after_runs = set((tmp_path / "runs").glob("*/run-*")) if (tmp_path / "runs").exists() else set()
    assert analyzed.status == "FAILED"
    assert "UNIQUE constraint failed" in analyzed.error
    assert after_runs == before_runs


def test_dataset_origin_is_unique_and_same_cutoff_noop_is_not_origin(tmp_path: Path):
    import sqlite3

    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=300)
    orchestrator = Orchestrator(cfg, FixtureBinanceClient())
    first = orchestrator.ingest_once()
    second = orchestrator.ingest_once()
    assert second.dataset_version == first.dataset_version
    with sqlite3.connect(tmp_path / "data" / "market.sqlite3") as connection:
        origins = connection.execute(
            "SELECT ingestion_run_id FROM ingestion_runs WHERE dataset_version=?",
            (first.dataset_version,),
        ).fetchall()
        noop = connection.execute(
            "SELECT status,dataset_version FROM ingestion_runs WHERE ingestion_run_id=?",
            (second.ingestion_run_id,),
        ).fetchone()
        indexes = connection.execute("PRAGMA index_list(ingestion_runs)").fetchall()
    assert origins == [(first.ingestion_run_id,)]
    assert noop == ("COMMITTED", None)
    assert any(index[1] == "one_ingestion_origin_per_dataset" and index[2] for index in indexes)


def test_analyze_latest_rejects_external_ingestion_override():
    import inspect

    assert "ingestion_run_id" not in inspect.signature(Orchestrator.analyze_latest).parameters


def test_legacy_duplicate_dataset_origins_are_migrated_once(tmp_path: Path):
    import sqlite3

    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=300)
    first = Orchestrator(cfg, FixtureBinanceClient()).ingest_once()
    database = tmp_path / "data" / "market.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("DROP INDEX one_ingestion_origin_per_dataset")
        connection.execute("PRAGMA user_version=0")
        connection.execute(
            """INSERT INTO ingestion_runs(
                ingestion_run_id,status,cutoff,dataset_version,finished_at
            ) SELECT 'ingest-legacy-duplicate','COMMITTED',cutoff,dataset_version,
                CURRENT_TIMESTAMP FROM ingestion_runs WHERE ingestion_run_id=?""",
            (first.ingestion_run_id,),
        )
    MarketRepository(database)
    with sqlite3.connect(database) as connection:
        origins = connection.execute(
            "SELECT ingestion_run_id FROM ingestion_runs WHERE dataset_version=?",
            (first.dataset_version,),
        ).fetchall()
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        indexes = connection.execute("PRAGMA index_list(ingestion_runs)").fetchall()
    assert len(origins) == 1
    assert version == 1
    assert any(index[1] == "one_ingestion_origin_per_dataset" for index in indexes)


def test_malformed_same_name_origin_index_fails_closed(tmp_path: Path):
    import sqlite3

    database = tmp_path / "market.sqlite3"
    MarketRepository(database)
    with sqlite3.connect(database) as connection:
        connection.execute("DROP INDEX one_ingestion_origin_per_dataset")
        connection.execute(
            "CREATE INDEX one_ingestion_origin_per_dataset ON ingestion_runs(cutoff)"
        )
    with pytest.raises(ValidationError, match="origin index contract mismatch"):
        MarketRepository(database)


@pytest.mark.parametrize(
    "predicate",
    (
        "dataset_version IS NOT NULL AND 0",
        "dataset_version IS NOT NULL OR 1",
        "(dataset_version IS NOT NULL)",
    ),
)
def test_origin_index_rejects_noncanonical_predicate(tmp_path: Path, predicate: str):
    import sqlite3

    database = tmp_path / "market.sqlite3"
    MarketRepository(database)
    with sqlite3.connect(database) as connection:
        connection.execute("DROP INDEX one_ingestion_origin_per_dataset")
        connection.execute(
            "CREATE UNIQUE INDEX one_ingestion_origin_per_dataset "
            f"ON ingestion_runs(dataset_version) WHERE {predicate}"
        )
    with pytest.raises(ValidationError, match="origin index contract mismatch"):
        MarketRepository(database)
