import sqlite3
from pathlib import Path

import pytest

import smc_ict.backup as backup
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


def test_failed_backup_validation_preserves_existing_target(tmp_path: Path, monkeypatch):
    source = tmp_path / "source.db"
    target = tmp_path / "backup.db"
    with sqlite3.connect(source) as connection:
        connection.execute("create table fresh(value text)")
    target.write_bytes(b"known-good-backup")
    monkeypatch.setattr(backup, "_integrity_check", lambda _: "corrupt")
    with pytest.raises(RuntimeError, match="integrity"):
        backup_database(source, target)
    assert target.read_bytes() == b"known-good-backup"
    assert not list(tmp_path.glob(".backup.db.*.tmp"))


def test_missing_source_never_creates_database_or_replaces_target(tmp_path: Path):
    source = tmp_path / "missing.db"
    target = tmp_path / "backup.db"
    target.write_bytes(b"known-good-backup")
    with pytest.raises(FileNotFoundError):
        backup_database(source, target)
    assert not source.exists()
    assert target.read_bytes() == b"known-good-backup"


def test_post_replace_fsync_failure_restores_existing_target(tmp_path: Path, monkeypatch):
    source = tmp_path / "source.db"
    target = tmp_path / "backup.db"
    with sqlite3.connect(source) as connection:
        connection.execute("create table fresh(value text)")
    target.write_bytes(b"known-good-backup")
    real_fsync = backup.os.fsync
    calls = 0

    def fail_directory_fsync(descriptor):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected directory fsync failure")
        return real_fsync(descriptor)

    monkeypatch.setattr(backup.os, "fsync", fail_directory_fsync)
    with pytest.raises(OSError, match="injected"):
        backup_database(source, target)
    assert target.read_bytes() == b"known-good-backup"
    assert not list(tmp_path.glob(".backup.db.*"))


def test_wal_source_backup_leaves_no_temporary_sidecars(tmp_path: Path):
    source = tmp_path / "source.db"
    target = tmp_path / "backup.db"
    with sqlite3.connect(source) as writer:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("create table evidence(value text)")
        writer.execute("insert into evidence values ('kept')")
        writer.commit()
        manifest = backup_database(source, target)
    assert manifest["integrity_check"] == "ok"
    with sqlite3.connect(target) as connection:
        assert connection.execute("select value from evidence").fetchone()[0] == "kept"
    assert not list(tmp_path.glob(".backup.db.*"))
