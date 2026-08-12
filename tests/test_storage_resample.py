import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pytest

from smc_ict.models import Bar
from smc_ict.resample import resample_bars
from smc_ict.storage import MarketRepository, ValidationError


def bars(n=5, start=0, symbol="BTCUSDT"):
    return [
        Bar(
            symbol,
            start + i * 60_000,
            start + (i + 1) * 60_000 - 1,
            Decimal(str(100 + i)),
            Decimal(str(102 + i)),
            Decimal(str(99 + i)),
            Decimal(str(101 + i)),
            Decimal("1"),
            True,
        )
        for i in range(n)
    ]


def test_atomic_universe_commit_idempotency_and_snapshot(tmp_path: Path):
    repo = MarketRepository(tmp_path / "m.db")
    batch = {"BTCUSDT": bars(), "ETHUSDT": bars(symbol="ETHUSDT")}
    v1 = repo.commit_dataset(batch, 299_999)
    assert repo.snapshot(v1).bars("BTCUSDT") == tuple(bars())
    v2 = repo.commit_dataset(batch, 299_999, base_version=v1)
    assert repo.revision_count() == 10
    assert repo.snapshot(v1).bars("BTCUSDT") == repo.snapshot(v2).bars("BTCUSDT")


def test_partial_universe_gap_and_open_bar_fail_closed(tmp_path: Path):
    repo = MarketRepository(tmp_path / "m.db")
    with pytest.raises(ValidationError):
        repo.commit_dataset({"BTCUSDT": bars()}, 299_999)
    bad = bars()
    bad.pop(2)
    with pytest.raises(ValidationError):
        repo.commit_dataset({"BTCUSDT": bad, "ETHUSDT": bars(symbol="ETHUSDT")}, 299_999)
    opened = bars()
    opened[-1] = opened[-1].replace(is_closed=False)
    with pytest.raises(ValidationError):
        repo.commit_dataset({"BTCUSDT": opened, "ETHUSDT": bars(symbol="ETHUSDT")}, 299_999)


def test_resample_requires_complete_utc_windows():
    out = resample_bars(bars(), 5)
    assert len(out) == 1 and out[0].open == Decimal("100") and out[0].close == Decimal("105")
    assert resample_bars(bars(4), 5) == ()


def test_commit_exception_rolls_back_dataset_and_mapping(tmp_path: Path):
    repo = MarketRepository(tmp_path / "m.db")
    batch = {"BTCUSDT": bars(), "ETHUSDT": bars(symbol="ETHUSDT")}
    original = repo._c

    class FaultyConnection:
        def __init__(self, connection):
            self.connection = connection
            self.inserts = 0

        def __enter__(self):
            self.connection.__enter__()
            return self

        def __exit__(self, *args):
            return self.connection.__exit__(*args)

        def execute(self, sql, parameters=()):
            if sql.startswith("INSERT OR REPLACE INTO dataset_bars"):
                self.inserts += 1
                if self.inserts == 2:
                    raise RuntimeError("injected commit failure")
            return self.connection.execute(sql, parameters)

    repo._c = lambda: FaultyConnection(original())
    with pytest.raises(RuntimeError, match="injected"):
        repo.commit_dataset(batch, 299_999)
    repo._c = original
    assert repo.latest_committed() is None
    assert repo.revision_count() == 0


def test_incremental_merge_gap_rolls_back(tmp_path: Path):
    repo = MarketRepository(tmp_path / "m.db")
    base = {"BTCUSDT": bars(), "ETHUSDT": bars(symbol="ETHUSDT")}
    version = repo.commit_dataset(base, 299_999)
    # Simulate a legacy/corrupt inherited mapping with a middle bar missing.
    with repo._c() as connection:
        connection.execute(
            "DELETE FROM dataset_bars WHERE version=? AND symbol='BTCUSDT' AND open_time=?",
            (version, 180_000),
        )
    overlapping = {
        "BTCUSDT": bars(3, start=240_000),
        "ETHUSDT": bars(3, start=240_000, symbol="ETHUSDT"),
    }
    with pytest.raises(ValidationError, match="merged snapshot gap"):
        repo.commit_dataset(overlapping, 419_999, base_version=version)
    assert repo.latest_committed() == (version, 299_999)


def test_incremental_merge_rejects_cross_symbol_leading_lineage_mismatch(tmp_path: Path):
    repo = MarketRepository(tmp_path / "m.db")
    base = {"BTCUSDT": bars(), "ETHUSDT": bars(symbol="ETHUSDT")}
    version = repo.commit_dataset(base, 299_999)
    with repo._c() as connection:
        connection.execute(
            "DELETE FROM dataset_bars WHERE version=? AND symbol='BTCUSDT' AND open_time=0",
            (version,),
        )
    overlapping = {
        "BTCUSDT": bars(3, start=240_000),
        "ETHUSDT": bars(3, start=240_000, symbol="ETHUSDT"),
    }
    with pytest.raises(ValidationError, match="lineage start"):
        repo.commit_dataset(overlapping, 419_999, base_version=version)
    assert repo.latest_committed() == (version, 299_999)


def test_base_version_must_exist_and_not_be_from_future(tmp_path: Path):
    repo = MarketRepository(tmp_path / "m.db")
    with pytest.raises(ValidationError, match="base version"):
        repo.commit_dataset(
            {"BTCUSDT": bars(), "ETHUSDT": bars(symbol="ETHUSDT")},
            299_999,
            base_version="missing",
        )
    future = repo.commit_dataset({"BTCUSDT": bars(), "ETHUSDT": bars(symbol="ETHUSDT")}, 299_999)
    with pytest.raises(ValidationError, match="newer than cutoff"):
        repo.commit_dataset(
            {
                "BTCUSDT": bars(4),
                "ETHUSDT": bars(4, symbol="ETHUSDT"),
            },
            239_999,
            base_version=future,
        )


def test_incremental_batch_requires_overlap_with_latest_base(tmp_path: Path):
    repo = MarketRepository(tmp_path / "m.db")
    base = repo.commit_dataset({"BTCUSDT": bars(), "ETHUSDT": bars(symbol="ETHUSDT")}, 299_999)
    without_overlap = {
        "BTCUSDT": bars(2, start=300_000),
        "ETHUSDT": bars(2, start=300_000, symbol="ETHUSDT"),
    }
    with pytest.raises(ValidationError, match="overlap"):
        repo.commit_dataset(without_overlap, 419_999, base_version=base)


def test_incremental_base_must_be_latest_committed(tmp_path: Path):
    repo = MarketRepository(tmp_path / "m.db")
    first = repo.commit_dataset({"BTCUSDT": bars(), "ETHUSDT": bars(symbol="ETHUSDT")}, 299_999)
    second = repo.commit_dataset(
        {
            "BTCUSDT": bars(3, start=240_000),
            "ETHUSDT": bars(3, start=240_000, symbol="ETHUSDT"),
        },
        419_999,
        base_version=first,
    )
    assert second != first
    with pytest.raises(ValidationError, match="latest committed"):
        repo.commit_dataset(
            {
                "BTCUSDT": bars(4, start=180_000),
                "ETHUSDT": bars(4, start=180_000, symbol="ETHUSDT"),
            },
            419_999,
            base_version=first,
        )


def test_bootstrap_without_base_is_allowed_only_for_empty_repository(tmp_path: Path):
    repo = MarketRepository(tmp_path / "m.db")
    first = repo.commit_dataset({"BTCUSDT": bars(), "ETHUSDT": bars(symbol="ETHUSDT")}, 299_999)
    with pytest.raises(ValidationError, match="requires latest base"):
        repo.commit_dataset(
            {
                "BTCUSDT": bars(4),
                "ETHUSDT": bars(4, symbol="ETHUSDT"),
            },
            239_999,
        )
    assert repo.latest_committed() == (first, 299_999)


@pytest.mark.parametrize(
    "replacement",
    [
        {"open_time": 1, "close_time": 60_000},
        {"close_time": 60_000},
        {"low": Decimal("103"), "high": Decimal("102")},
    ],
)
def test_noncanonical_one_minute_bar_fails_closed(tmp_path: Path, replacement):
    repo = MarketRepository(tmp_path / "m.db")
    btc = bars()
    btc[0] = btc[0].replace(**replacement)
    with pytest.raises(ValidationError, match="invalid bar"):
        repo.commit_dataset({"BTCUSDT": btc, "ETHUSDT": bars(symbol="ETHUSDT")}, 299_999)


def test_row_hash_covers_close_time_and_closed_flag(tmp_path: Path):
    repo = MarketRepository(tmp_path / "m.db")
    original = bars(1)[0]
    first = repo._row_hash(original)
    assert first != repo._row_hash(original.replace(close_time=original.close_time - 1))
    assert first != repo._row_hash(original.replace(is_closed=False))


def test_dataset_checksum_covers_full_pinned_rows(tmp_path: Path):
    repo = MarketRepository(tmp_path / "m.db")
    batch = {"BTCUSDT": bars(), "ETHUSDT": bars(symbol="ETHUSDT")}
    version = repo.commit_dataset(batch, 299_999)
    canonical = []
    for symbol in ("BTCUSDT", "ETHUSDT"):
        for bar in repo.snapshot(version).bars(symbol):
            canonical.append(
                [
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
            )
    expected = hashlib.sha256(json.dumps(canonical, separators=(",", ":")).encode()).hexdigest()
    assert repo.dataset_checksum(version) == expected
