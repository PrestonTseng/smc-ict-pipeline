from __future__ import annotations

import hashlib
import os
import sqlite3
import tempfile
from pathlib import Path


def _integrity_check(path: Path) -> str:
    uri = f"{path.resolve().as_uri()}?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as connection:
        return str(connection.execute("PRAGMA integrity_check").fetchone()[0])


def backup_database(source: Path, target: Path) -> dict:
    if not source.is_file():
        raise FileNotFoundError(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    rollback = target.parent / f".{target.name}.rollback"
    if rollback.exists():
        raise FileExistsError(f"stale backup rollback exists: {rollback}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        source_uri = f"{source.resolve().as_uri()}?mode=ro"
        with sqlite3.connect(source_uri, uri=True) as src, sqlite3.connect(temporary) as dst:
            src.backup(dst)
        integrity = _integrity_check(temporary)
        if integrity != "ok":
            raise RuntimeError(f"backup integrity check failed: {integrity}")
        checksum = hashlib.sha256(temporary.read_bytes()).hexdigest()
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            had_target = target.exists()
            if had_target:
                os.replace(target, rollback)
            try:
                os.replace(temporary, target)
                os.fsync(directory_fd)
            except BaseException:
                if had_target:
                    os.replace(rollback, target)
                else:
                    target.unlink(missing_ok=True)
                os.fsync(directory_fd)
                raise
            rollback.unlink(missing_ok=True)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
        Path(f"{temporary}-wal").unlink(missing_ok=True)
        Path(f"{temporary}-shm").unlink(missing_ok=True)
    return {
        "source": str(source),
        "target": str(target),
        "integrity_check": integrity,
        "sha256": checksum,
    }
