from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path


def backup_database(source: Path, target: Path) -> dict:
    target.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(source) as src, sqlite3.connect(target) as dst:
        src.backup(dst)
    with sqlite3.connect(target) as c:
        integrity = c.execute("PRAGMA integrity_check").fetchone()[0]
    return {
        "source": str(source),
        "target": str(target),
        "integrity_check": integrity,
        "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
    }
