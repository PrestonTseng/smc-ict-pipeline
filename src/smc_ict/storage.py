"""Transactional append-only SQLite market SSOT."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from decimal import Decimal
from pathlib import Path

from .models import Bar


class ValidationError(ValueError):
    pass


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS datasets(
    version TEXT PRIMARY KEY,
    cutoff INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'COMMITTED',
    committed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS ingestion_runs(
    ingestion_run_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    cutoff INTEGER,
    error TEXT,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS bars(
    symbol TEXT, open_time INTEGER, close_time INTEGER,
    o TEXT, h TEXT, l TEXT, c TEXT, v TEXT, closed INTEGER,
    row_hash TEXT, observed_version TEXT REFERENCES datasets(version),
    PRIMARY KEY(symbol, open_time, row_hash)
);
CREATE TABLE IF NOT EXISTS dataset_bars(
    version TEXT REFERENCES datasets(version), symbol TEXT,
    open_time INTEGER, row_hash TEXT,
    PRIMARY KEY(version, symbol, open_time)
);
"""


class MarketRepository:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with self._c() as c:
            c.executescript(SCHEMA)
            columns = {row[1] for row in c.execute("PRAGMA table_info(datasets)")}
            if "status" not in columns:
                c.execute(
                    "ALTER TABLE datasets ADD COLUMN status TEXT NOT NULL DEFAULT 'COMMITTED'"
                )

    def _c(self):
        c = sqlite3.connect(self.path)
        c.execute("PRAGMA foreign_keys=ON")
        return c

    def latest_committed(self) -> tuple[str, int] | None:
        with self._c() as connection:
            row = connection.execute(
                """SELECT version,cutoff FROM datasets WHERE status='COMMITTED'
                ORDER BY committed_at DESC,rowid DESC LIMIT 1"""
            ).fetchone()
        return (str(row[0]), int(row[1])) if row else None

    def start_ingestion(self, ingestion_run_id: str, cutoff: int | None = None) -> None:
        with self._c() as connection:
            connection.execute(
                """INSERT INTO ingestion_runs(ingestion_run_id,status,cutoff)
                VALUES (?,'FETCHING',?)""",
                (ingestion_run_id, cutoff),
            )

    def set_ingestion_cutoff(self, ingestion_run_id: str, cutoff: int) -> None:
        with self._c() as connection:
            connection.execute(
                "UPDATE ingestion_runs SET cutoff=? WHERE ingestion_run_id=?",
                (cutoff, ingestion_run_id),
            )

    def fail_ingestion(self, ingestion_run_id: str, error: str) -> None:
        with self._c() as connection:
            connection.execute(
                """UPDATE ingestion_runs SET status='FAILED',error=?,finished_at=CURRENT_TIMESTAMP
                WHERE ingestion_run_id=?""",
                (error, ingestion_run_id),
            )

    def complete_ingestion(self, ingestion_run_id: str) -> None:
        with self._c() as connection:
            connection.execute(
                """UPDATE ingestion_runs SET status='COMMITTED',finished_at=CURRENT_TIMESTAMP
                WHERE ingestion_run_id=?""",
                (ingestion_run_id,),
            )

    def commit_dataset(
        self,
        batch: dict[str, list[Bar]],
        cutoff: int,
        base_version: str | None = None,
        ingestion_run_id: str | None = None,
    ) -> str:
        if set(batch) != {"BTCUSDT", "ETHUSDT"}:
            raise ValidationError("complete universe required")
        for symbol, rows in batch.items():
            rows = sorted(rows, key=lambda x: x.open_time)
            if not rows or any(
                b.symbol != symbol
                or not b.is_closed
                or b.close_time > cutoff
                or b.high < max(b.open, b.close)
                or b.low > min(b.open, b.close)
                or b.volume < 0
                for b in rows
            ):
                raise ValidationError("invalid bar")
            if any(
                b.open_time - a.open_time != 60_000 for a, b in zip(rows, rows[1:], strict=False)
            ):
                raise ValidationError("gap")
            if rows[-1].close_time != cutoff:
                raise ValidationError("watermark mismatch")
        version = "ds-" + uuid.uuid4().hex
        with self._c() as c:
            c.execute("BEGIN IMMEDIATE")
            c.execute("INSERT INTO datasets(version,cutoff) VALUES (?,?)", (version, cutoff))
            if base_version is not None:
                c.execute(
                    """INSERT INTO dataset_bars(version,symbol,open_time,row_hash)
                    SELECT ?,symbol,open_time,row_hash FROM dataset_bars WHERE version=?""",
                    (version, base_version),
                )
            for symbol, rows in batch.items():
                for b in rows:
                    vals = [str(x) for x in (b.open, b.high, b.low, b.close, b.volume)]
                    rh = hashlib.sha256(
                        json.dumps([symbol, b.open_time, *vals], separators=(",", ":")).encode()
                    ).hexdigest()
                    c.execute(
                        "INSERT OR IGNORE INTO bars VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (symbol, b.open_time, b.close_time, *vals, 1, rh, version),
                    )
                    c.execute(
                        "INSERT OR REPLACE INTO dataset_bars VALUES (?,?,?,?)",
                        (version, symbol, b.open_time, rh),
                    )
            if ingestion_run_id is not None:
                c.execute(
                    """UPDATE ingestion_runs SET status='COMMITTED',finished_at=CURRENT_TIMESTAMP
                    WHERE ingestion_run_id=?""",
                    (ingestion_run_id,),
                )
        return version

    def revision_count(self):
        with self._c() as c:
            return c.execute("SELECT count(*) FROM bars").fetchone()[0]

    def dataset_checksum(self, version: str) -> str:
        with self._c() as connection:
            rows = connection.execute(
                """SELECT symbol,open_time,row_hash FROM dataset_bars
                WHERE version=? ORDER BY symbol,open_time""",
                (version,),
            ).fetchall()
        payload = json.dumps(rows, separators=(",", ":"), ensure_ascii=True).encode()
        return hashlib.sha256(payload).hexdigest()

    def snapshot(self, version):
        return Snapshot(self.path, version)


class Snapshot:
    def __init__(self, path, version):
        self.path = path
        self.version = version

    def bars(self, symbol):
        with sqlite3.connect(self.path) as c:
            rows = c.execute(
                """SELECT b.symbol,b.open_time,b.close_time,b.o,b.h,b.l,b.c,b.v,b.closed
                FROM dataset_bars d JOIN bars b
                  ON b.symbol=d.symbol AND b.open_time=d.open_time AND b.row_hash=d.row_hash
                WHERE d.version=? AND d.symbol=? ORDER BY b.open_time""",
                (self.version, symbol),
            ).fetchall()
        return tuple(Bar(r[0], r[1], r[2], *(Decimal(x) for x in r[3:8]), bool(r[8])) for r in rows)
