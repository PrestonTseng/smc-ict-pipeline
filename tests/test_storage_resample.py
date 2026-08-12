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
    v2 = repo.commit_dataset(batch, 299_999)
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
