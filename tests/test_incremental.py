from decimal import Decimal
from pathlib import Path

from smc_ict.config import AppConfig
from smc_ict.models import Bar
from smc_ict.pipeline.orchestrator import Orchestrator
from smc_ict.storage import MarketRepository


def bars(symbol, start, count):
    return [
        Bar(
            symbol,
            start + i * 60_000,
            start + (i + 1) * 60_000 - 1,
            Decimal("1"),
            Decimal("2"),
            Decimal("0.5"),
            Decimal("1.5"),
            Decimal("1"),
            True,
        )
        for i in range(count)
    ]


def test_new_dataset_inherits_prior_snapshot_and_replaces_overlap(tmp_path: Path):
    repo = MarketRepository(tmp_path / "m.db")
    first = {s: bars(s, 0, 5) for s in ("BTCUSDT", "ETHUSDT")}
    v1 = repo.commit_dataset(first, 299_999)
    revised = {s: bars(s, 180_000, 4) for s in ("BTCUSDT", "ETHUSDT")}
    revised["BTCUSDT"][0] = revised["BTCUSDT"][0].replace(close=Decimal("1.7"))
    v2 = repo.commit_dataset(revised, 419_999, base_version=v1)
    assert len(repo.snapshot(v1).bars("BTCUSDT")) == 5
    latest = repo.snapshot(v2).bars("BTCUSDT")
    assert len(latest) == 7 and latest[3].close == Decimal("1.7")
    assert repo.latest_committed() == (v2, 419_999)


class RecordingClient:
    def __init__(self):
        self.calls = []

    def latest_closed_cutoff(self):
        return 419_999

    def fetch_1m(self, symbol, start, end):
        self.calls.append((symbol, start, end))
        return bars(symbol, start, (end - start + 1) // 60_000)


def test_orchestrator_uses_overlap_after_bootstrap(tmp_path: Path):
    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=5, overlap_bars=2)
    client = RecordingClient()
    orchestrator = Orchestrator(cfg, client)
    assert orchestrator.run_once().status != "FAILED"
    client.calls.clear()
    client.latest_closed_cutoff = lambda: 539_999
    assert orchestrator.run_once().status != "FAILED"
    assert {start for _, start, _ in client.calls} == {300_000}


def test_same_cutoff_reuses_dataset_but_publishes_new_analysis(tmp_path: Path):
    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=5, overlap_bars=2)
    client = RecordingClient()
    orchestrator = Orchestrator(cfg, client)
    first = orchestrator.run_once()
    client.calls.clear()
    second = orchestrator.run_once()
    assert second.dataset_version == first.dataset_version
    assert second.analysis_run_id != first.analysis_run_id
    assert client.calls == []


def test_older_cutoff_fails_without_new_artifact(tmp_path: Path):
    cfg = AppConfig(data_root=tmp_path, bootstrap_bars=5, overlap_bars=2)
    client = RecordingClient()
    orchestrator = Orchestrator(cfg, client)
    assert orchestrator.run_once().status != "FAILED"
    before = set((tmp_path / "runs" / "1970-01-01").iterdir())
    client.latest_closed_cutoff = lambda: 359_999
    result = orchestrator.run_once()
    assert result.status == "FAILED" and "regressed" in result.error
    assert set((tmp_path / "runs" / "1970-01-01").iterdir()) == before
