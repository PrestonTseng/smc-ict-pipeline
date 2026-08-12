import json
from pathlib import Path

from smc_ict.cli import main
from smc_ict.config import load_config
from smc_ict.data.binance import FixtureBinanceClient
from smc_ict.pipeline.analysis import analyze_symbol
from smc_ict.storage import MarketRepository


def test_toml_config_and_fixture_cli(tmp_path: Path, capsys):
    config = tmp_path / "config.toml"
    config.write_text(
        '[app]\nbootstrap_bars=300\n[paths]\ndata_root="' + str(tmp_path / "var") + '"\n'
    )
    assert load_config(config).bootstrap_bars == 300
    assert main(["run-once", "--config", str(config), "--fixture"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] in {"NO_SETUP", "ARMED", "ORDER_PENDING", "TRADE"}
    assert payload["dataset_version"].startswith("ds-")
    assert (tmp_path / "var-fixture" / "data" / "market.sqlite3").exists()
    assert not (tmp_path / "var" / "data" / "market.sqlite3").exists()


def test_default_fixture_is_isolated_from_live_data_root(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert main(["run-once", "--fixture"]) == 0
    assert (tmp_path / "var-fixture" / "data" / "market.sqlite3").exists()
    assert not (tmp_path / "var" / "data" / "market.sqlite3").exists()


def test_fixture_accepts_current_directory_data_root(tmp_path: Path, monkeypatch):
    config = tmp_path / "config.toml"
    config.write_text('[app]\nbootstrap_bars=300\n[paths]\ndata_root="."\n')
    monkeypatch.chdir(tmp_path)
    assert main(["run-once", "--config", str(config), "--fixture"]) == 0
    assert (tmp_path / "var-fixture" / "data" / "market.sqlite3").exists()


def test_analysis_emits_every_gate_from_one_snapshot(tmp_path: Path):
    client = FixtureBinanceClient()
    cutoff = client.latest_closed_cutoff()
    start = cutoff + 1 - 300 * 60_000
    repo = MarketRepository(tmp_path / "market.db")
    version = repo.commit_dataset(
        {s: client.fetch_1m(s, start, cutoff) for s in ("BTCUSDT", "ETHUSDT")}, cutoff
    )
    result = analyze_symbol(repo.snapshot(version), "BTCUSDT", load_config(None).strategy)
    assert set(result["indicators"]) == {
        "smc_4h_structure",
        "smc_1h_dealing_range",
        "smc_1h_order_block",
        "ict_5m_liquidity",
        "ict_5m_displacement",
        "ict_5m_mss",
        "ict_5m_fvg",
        "risk",
    }
    assert result["decision"]["status"] in {"NO_SETUP", "ARMED", "ORDER_PENDING", "TRADE"}


def test_process_lock_fail_closed(tmp_path: Path):
    from smc_ict.lock import LockUnavailable, ProcessLock

    lock = tmp_path / "run.lock"
    with ProcessLock(lock):
        try:
            with ProcessLock(lock):
                pass
        except LockUnavailable:
            pass
        else:
            raise AssertionError("second lock must fail")


def test_displacement_candidates_start_after_sweep_and_fvg_starts_at_displacement_leg():
    from smc_ict.pipeline.analysis import execution_windows

    displacement_indices, fvg_start = execution_windows(
        sweep_index=10, displacement_index=12, displacement_window=3
    )
    assert displacement_indices == range(11, 14)
    assert fvg_start == 10


def test_poi_touch_is_resolved_on_5m_not_delayed_to_1h_close():
    from decimal import Decimal

    from smc_ict.models import Bar
    from smc_ict.pipeline.analysis import first_zone_touch

    bars = tuple(
        Bar(
            "BTCUSDT",
            i * 300_000,
            (i + 1) * 300_000 - 1,
            Decimal("12"),
            Decimal("13"),
            Decimal("9" if i == 2 else "11"),
            Decimal("12"),
            Decimal("1"),
            True,
        )
        for i in range(12)
    )
    assert first_zone_touch(bars, Decimal("9"), Decimal("10"), after=bars[0].close_time) == 2
