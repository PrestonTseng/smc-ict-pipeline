import sqlite3
from pathlib import Path

from smc_ict.backup import backup_database


def test_backup_uses_consistent_sqlite_snapshot(tmp_path: Path):
    source = tmp_path / "source.db"
    target = tmp_path / "backup.db"
    with sqlite3.connect(source) as c:
        c.execute("create table evidence(value text)")
        c.execute("insert into evidence values ('kept')")
    manifest = backup_database(source, target)
    assert target.exists() and manifest["integrity_check"] == "ok"
    with sqlite3.connect(target) as c:
        assert c.execute("select value from evidence").fetchone()[0] == "kept"
