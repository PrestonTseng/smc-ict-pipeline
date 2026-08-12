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
    dataset_version TEXT REFERENCES datasets(version),
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
            ingestion_columns = {row[1] for row in c.execute("PRAGMA table_info(ingestion_runs)")}
            if "dataset_version" not in ingestion_columns:
                c.execute(
                    "ALTER TABLE ingestion_runs ADD COLUMN dataset_version "
                    "TEXT REFERENCES datasets(version)"
                )
            schema_version = int(c.execute("PRAGMA user_version").fetchone()[0])
            if schema_version < 1:
                c.execute(
                    """UPDATE ingestion_runs SET dataset_version=NULL
                    WHERE dataset_version IS NOT NULL AND (
                        status!='COMMITTED' OR NOT EXISTS (
                            SELECT 1 FROM datasets d
                            WHERE d.version=ingestion_runs.dataset_version
                            AND d.status='COMMITTED' AND d.cutoff=ingestion_runs.cutoff
                        ) OR ingestion_run_id != (
                            SELECT MIN(origin.ingestion_run_id)
                            FROM ingestion_runs origin
                            WHERE origin.status='COMMITTED'
                            AND origin.dataset_version=ingestion_runs.dataset_version
                            AND origin.cutoff=ingestion_runs.cutoff
                        )
                    )"""
                )
                c.execute(
                    """UPDATE ingestion_runs SET dataset_version=(
                        SELECT MIN(version) FROM datasets
                        WHERE datasets.cutoff=ingestion_runs.cutoff
                        HAVING COUNT(*)=1
                    ) WHERE status='COMMITTED' AND dataset_version IS NULL
                    AND ingestion_run_id=(
                        SELECT MIN(origin.ingestion_run_id) FROM ingestion_runs AS origin
                        WHERE origin.status='COMMITTED'
                        AND origin.cutoff=ingestion_runs.cutoff
                    )"""
                )
            c.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS one_ingestion_origin_per_dataset
                ON ingestion_runs(dataset_version)
                WHERE dataset_version IS NOT NULL"""
            )
            index_rows = [
                row
                for row in c.execute("PRAGMA index_list(ingestion_runs)")
                if row[1] == "one_ingestion_origin_per_dataset"
            ]
            index_columns = c.execute(
                "PRAGMA index_info(one_ingestion_origin_per_dataset)"
            ).fetchall()
            index_sql_row = c.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
                ("one_ingestion_origin_per_dataset",),
            ).fetchone()
            normalized_sql = (
                " ".join(str(index_sql_row[0]).upper().split()) if index_sql_row else ""
            )
            expected_sql = (
                "CREATE UNIQUE INDEX ONE_INGESTION_ORIGIN_PER_DATASET "
                "ON INGESTION_RUNS(DATASET_VERSION) WHERE DATASET_VERSION IS NOT NULL"
            )
            if (
                len(index_rows) != 1
                or index_rows[0][2] != 1
                or index_rows[0][4] != 1
                or [row[2] for row in index_columns] != ["dataset_version"]
                or normalized_sql != expected_sql
            ):
                raise ValidationError("origin index contract mismatch")
            if schema_version < 1:
                c.execute("PRAGMA user_version=1")

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

    def committed_ingestion_for_dataset(self, dataset_version: str) -> str | None:
        with self._c() as connection:
            rows = connection.execute(
                """SELECT i.ingestion_run_id FROM ingestion_runs i
                JOIN datasets d ON d.version=i.dataset_version
                WHERE i.status='COMMITTED' AND i.dataset_version=?
                AND i.cutoff=d.cutoff AND d.status='COMMITTED'""",
                (dataset_version,),
            ).fetchall()
        return str(rows[0][0]) if len(rows) == 1 else None

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
                """UPDATE ingestion_runs SET status='COMMITTED',
                finished_at=CURRENT_TIMESTAMP WHERE ingestion_run_id=?""",
                (ingestion_run_id,),
            )

    @staticmethod
    def _row_hash(bar: Bar) -> str:
        canonical = [
            bar.symbol,
            bar.open_time,
            bar.close_time,
            str(bar.open),
            str(bar.high),
            str(bar.low),
            str(bar.close),
            str(bar.volume),
            bar.is_closed,
        ]
        return hashlib.sha256(json.dumps(canonical, separators=(",", ":")).encode()).hexdigest()

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
                or b.open_time % 60_000 != 0
                or b.close_time != b.open_time + 59_999
                or b.close_time > cutoff
                or b.high < max(b.open, b.close)
                or b.low > min(b.open, b.close)
                or b.low > b.high
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
            latest = c.execute(
                """SELECT version,cutoff FROM datasets WHERE status='COMMITTED'
                ORDER BY committed_at DESC,rowid DESC LIMIT 1"""
            ).fetchone()
            if base_version is None and latest is not None:
                raise ValidationError("non-empty repository requires latest base version")
            if base_version is not None:
                base = c.execute(
                    "SELECT cutoff,status FROM datasets WHERE version=?",
                    (base_version,),
                ).fetchone()
                if base is None or base[1] != "COMMITTED":
                    raise ValidationError("base version missing or not committed")
                if latest is None or latest[0] != base_version:
                    raise ValidationError("base version is not latest committed")
                if int(base[0]) > cutoff:
                    raise ValidationError("base version is newer than cutoff")
                lineage_starts = c.execute(
                    """SELECT symbol,MIN(open_time) FROM dataset_bars
                    WHERE version=? GROUP BY symbol ORDER BY symbol""",
                    (base_version,),
                ).fetchall()
                if (
                    len(lineage_starts) != len(batch)
                    or {row[0] for row in lineage_starts} != set(batch)
                    or len({row[1] for row in lineage_starts}) != 1
                ):
                    raise ValidationError("base version lineage start mismatch")
                base_last_open = int(base[0]) - 59_999
                if any(
                    min(row.open_time for row in rows) > base_last_open for rows in batch.values()
                ):
                    raise ValidationError("incremental batch requires overlap with base version")
            c.execute("INSERT INTO datasets(version,cutoff) VALUES (?,?)", (version, cutoff))
            if base_version is not None:
                c.execute(
                    """INSERT INTO dataset_bars(version,symbol,open_time,row_hash)
                    SELECT ?,symbol,open_time,row_hash FROM dataset_bars
                    WHERE version=? AND open_time<=?""",
                    (version, base_version, cutoff - 59_999),
                )
            for symbol, rows in batch.items():
                for b in rows:
                    vals = [str(x) for x in (b.open, b.high, b.low, b.close, b.volume)]
                    rh = self._row_hash(b)
                    c.execute(
                        "INSERT OR IGNORE INTO bars VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (symbol, b.open_time, b.close_time, *vals, 1, rh, version),
                    )
                    c.execute(
                        "INSERT OR REPLACE INTO dataset_bars VALUES (?,?,?,?)",
                        (version, symbol, b.open_time, rh),
                    )
            for symbol in sorted(batch):
                merged = c.execute(
                    """SELECT d.open_time,b.close_time FROM dataset_bars d JOIN bars b
                    ON b.symbol=d.symbol AND b.open_time=d.open_time AND b.row_hash=d.row_hash
                    WHERE d.version=? AND d.symbol=? ORDER BY d.open_time""",
                    (version, symbol),
                ).fetchall()
                if (
                    not merged
                    or merged[-1][1] != cutoff
                    or any(
                        right[0] - left[0] != 60_000
                        for left, right in zip(merged, merged[1:], strict=False)
                    )
                ):
                    raise ValidationError("merged snapshot gap or watermark mismatch")
            if ingestion_run_id is not None:
                c.execute(
                    """UPDATE ingestion_runs SET status='COMMITTED',dataset_version=?,
                    finished_at=CURRENT_TIMESTAMP WHERE ingestion_run_id=?""",
                    (version, ingestion_run_id),
                )
        return version

    def revision_count(self):
        with self._c() as c:
            return c.execute("SELECT count(*) FROM bars").fetchone()[0]

    def dataset_checksum(self, version: str) -> str:
        with self._c() as connection:
            rows = connection.execute(
                """SELECT b.symbol,b.open_time,b.close_time,b.o,b.h,b.l,b.c,b.v,b.closed
                FROM dataset_bars d JOIN bars b
                  ON b.symbol=d.symbol AND b.open_time=d.open_time AND b.row_hash=d.row_hash
                WHERE d.version=? ORDER BY b.symbol,b.open_time""",
                (version,),
            ).fetchall()
        canonical = [
            [symbol, open_time, close_time, o, h, low, close, volume, bool(closed)]
            for symbol, open_time, close_time, o, h, low, close, volume, closed in rows
        ]
        payload = json.dumps(canonical, separators=(",", ":"), ensure_ascii=True).encode()
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
