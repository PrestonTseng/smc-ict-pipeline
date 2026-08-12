import sqlite3
from pathlib import Path

from smc_ict.storage import MarketRepository


def test_legacy_database_is_migrated_without_data_loss(tmp_path: Path):
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE datasets(version TEXT PRIMARY KEY,cutoff INTEGER NOT NULL,committed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
        connection.execute("INSERT INTO datasets(version,cutoff) VALUES ('legacy',59999)")
    repo = MarketRepository(path)
    assert repo.latest_committed() == ("legacy", 59999)
    with sqlite3.connect(path) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(datasets)")}
        assert "status" in columns
