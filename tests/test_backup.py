import sqlite3
import threading
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
    assert not [path for path in tmp_path.glob(".backup.db.*") if path.suffix != ".lock"]


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
    assert not [path for path in tmp_path.glob(".backup.db.*") if path.suffix != ".lock"]


def test_concurrent_backups_to_absent_target_publish_one_manifest(tmp_path: Path):
    sources = [tmp_path / f"source-{value}.db" for value in ("a", "b")]
    target = tmp_path / "backup.db"
    for source, value in zip(sources, ("a", "b"), strict=True):
        with sqlite3.connect(source) as connection:
            connection.execute("create table evidence(value text)")
            connection.execute("insert into evidence values (?)", (value,))
    barrier = threading.Barrier(2)
    manifests = []
    errors = []

    def run(source):
        barrier.wait()
        try:
            manifests.append(backup_database(source, target))
        except backup.BackupLockUnavailable as error:
            errors.append(error)

    threads = [threading.Thread(target=run, args=(source,)) for source in sources]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    final_hash = backup.hashlib.sha256(target.read_bytes()).hexdigest()
    assert len(manifests) == 1 and len(errors) == 1
    assert manifests[0]["sha256"] == final_hash


def test_restore_failure_preserves_primary_durability_error(tmp_path: Path, monkeypatch):
    source = tmp_path / "source.db"
    target = tmp_path / "backup.db"
    with sqlite3.connect(source) as connection:
        connection.execute("create table fresh(value text)")
    target.write_bytes(b"known-good-backup")
    real_replace = backup.os.replace
    real_fsync = backup.os.fsync
    fsync_calls = 0

    def fail_publish_fsync(descriptor):
        nonlocal fsync_calls
        fsync_calls += 1
        if fsync_calls == 2:
            raise OSError("primary durability failure")
        return real_fsync(descriptor)

    def fail_restore(source_path, target_path):
        if Path(source_path).name == ".backup.db.rollback":
            raise OSError("secondary restore failure")
        return real_replace(source_path, target_path)

    monkeypatch.setattr(backup.os, "fsync", fail_publish_fsync)
    monkeypatch.setattr(backup.os, "replace", fail_restore)
    with pytest.raises(OSError, match="primary durability failure") as caught:
        backup_database(source, target)
    assert any("secondary restore failure" in note for note in caught.value.__notes__)
    assert (tmp_path / ".backup.db.rollback").read_bytes() == b"known-good-backup"


def test_stale_rollback_failure_releases_target_lock(tmp_path: Path):
    source = tmp_path / "source.db"
    target = tmp_path / "backup.db"
    with sqlite3.connect(source) as connection:
        connection.execute("create table fresh(value text)")
    rollback = tmp_path / ".backup.db.rollback"
    rollback.write_bytes(b"recovery-required")
    for _ in range(2):
        with pytest.raises(FileExistsError, match="stale backup rollback"):
            backup_database(source, target)
