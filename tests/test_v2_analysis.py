from decimal import Decimal

from smc_ict.config import StrategyConfig
from smc_ict.indicators.swings import Swing
from smc_ict.models import Bar, Bias, IndicatorResult, Status


class Snapshot:
    def __init__(self, bars=("canonical-1m",)):
        self._bars = bars

    def bars(self, symbol):
        assert symbol == "BTCUSDT"
        return self._bars


def result(status, bias=None, reason=()):
    value = {"bias": bias.value} if bias is not None else {}
    return IndicatorResult(status, value, reason_codes=reason)


def bars(minutes, count=40):
    size = minutes * 60_000
    return tuple(
        Bar(
            "BTCUSDT",
            i * size,
            (i + 1) * size - 1,
            Decimal(i),
            Decimal(i + 3),
            Decimal(i - 1),
            Decimal(i + 2),
            Decimal("1"),
        )
        for i in range(count)
    )


def _mock_poi_discovery(monkeypatch, module, aggregates):
    monkeypatch.setattr(
        module,
        "confirmed_swings",
        lambda source, length: (
            Swing(2, 4, "low", Decimal("1")),
            Swing(5, 7, "high", Decimal("6")),
        ),
    )
    monkeypatch.setattr(
        module,
        "order_block",
        lambda *_args, **_kwargs: IndicatorResult(
            Status.PASS,
            {"low": "8", "high": "12", "origin_index": 3, "first_touch_index": 10},
            aggregates[240][10].close_time,
            aggregates[240][10].close_time,
            ("first_touch_ob",),
        ),
    )


def test_v2_uses_only_1d_4h_1h_aggregates(monkeypatch):
    import smc_ict.pipeline.v2_analysis as module

    calls = []
    aggregates = {1440: bars(1440), 240: bars(240), 60: bars(60)}

    def resample(source, minutes):
        calls.append((source, minutes))
        return aggregates[minutes]

    monkeypatch.setattr(module, "resample_bars", resample)
    _mock_poi_discovery(monkeypatch, module, aggregates)
    structures = iter(
        [
            result(Status.PASS, Bias.BULLISH, ("bullish_bos",)),
            result(Status.FAIL, Bias.NEUTRAL, ("no_confirmed_bos",)),
        ]
    )
    monkeypatch.setattr(module, "structure", lambda bars, strategy: next(structures))

    analyzed = module.analyze_symbol_v2(Snapshot(), "BTCUSDT", StrategyConfig())

    assert calls == [
        (("canonical-1m",), 1440),
        (("canonical-1m",), 240),
        (("canonical-1m",), 60),
    ]
    assert analyzed["decision"]["failed_gate"] == "smc_4h_structure"
    assert analyzed["decision"]["passed_gates"] == ["smc_1d_regime"]


def test_v2_stops_at_unconfirmed_daily_regime(monkeypatch):
    import smc_ict.pipeline.v2_analysis as module

    aggregates = {1440: bars(1440), 240: bars(240), 60: bars(60)}
    monkeypatch.setattr(module, "resample_bars", lambda source, minutes: aggregates[minutes])
    _mock_poi_discovery(monkeypatch, module, aggregates)
    monkeypatch.setattr(
        module,
        "structure",
        lambda bars, strategy: result(Status.FAIL, Bias.NEUTRAL, ("no_confirmed_bos",)),
    )

    analyzed = module.analyze_symbol_v2(Snapshot(), "BTCUSDT", StrategyConfig())

    assert analyzed["decision"] == {
        "status": "NO_SETUP",
        "failed_gate": "smc_1d_regime",
        "passed_gates": [],
        "reason_codes": ["no_confirmed_bos"],
    }
    assert analyzed["indicators"]["smc_4h_structure"]["status"] == "UNAVAILABLE"
    assert analyzed["indicators"]["smc_4h_structure"]["reason_codes"] == [
        "upstream_gate_not_passed"
    ]


def test_v2_long_only_stops_after_bearish_daily_bos(monkeypatch):
    import smc_ict.pipeline.v2_analysis as module

    aggregates = {1440: bars(1440), 240: bars(240), 60: bars(60)}
    monkeypatch.setattr(module, "resample_bars", lambda source, minutes: aggregates[minutes])
    _mock_poi_discovery(monkeypatch, module, aggregates)
    monkeypatch.setattr(
        module,
        "structure",
        lambda bars, strategy: result(Status.PASS, Bias.BEARISH, ("bearish_bos",)),
    )

    analyzed = module.analyze_symbol_v2(Snapshot(), "BTCUSDT", StrategyConfig())

    assert analyzed["decision"] == {
        "status": "NO_SETUP",
        "failed_gate": "smc_4h_structure",
        "passed_gates": ["smc_1d_regime"],
        "reason_codes": ["v2_long_only"],
    }
    assert analyzed["indicators"]["smc_4h_structure"]["value"] == {}


def test_v2_full_chain_executes_on_4h_poi_then_one_hour_timeline(monkeypatch):
    import smc_ict.pipeline.v2_analysis as module

    aggregates = {1440: bars(1440), 240: bars(240), 60: bars(60)}
    monkeypatch.setattr(module, "resample_bars", lambda source, minutes: aggregates[minutes])
    structures = iter(
        [
            result(Status.PASS, Bias.BULLISH, ("bullish_bos",)),
            result(Status.PASS, Bias.BULLISH, ("bullish_bos",)),
        ]
    )
    observed_context_lengths = []

    def causal_structure(source, _strategy):
        observed_context_lengths.append(len(source))
        return next(structures)

    monkeypatch.setattr(module, "structure", causal_structure)
    monkeypatch.setattr(
        module,
        "dealing",
        lambda source, _bias: observed_context_lengths.append(len(source)) or result(Status.PASS),
    )
    monkeypatch.setattr(
        module,
        "confirmed_swings",
        lambda source, length: (
            (Swing(2, 4, "low", Decimal("1")), Swing(5, 7, "high", Decimal("6")))
            if source is aggregates[240]
            else (Swing(6, 8, "low", Decimal("5")), Swing(7, 9, "high", Decimal("10")))
        ),
    )
    monkeypatch.setattr(
        module,
        "order_block",
        lambda *_args, **_kwargs: IndicatorResult(
            Status.PASS,
            {"low": "8", "high": "12", "origin_index": 3, "first_touch_index": 10},
            aggregates[240][10].close_time,
            aggregates[240][10].close_time,
            ("first_touch_ob",),
        ),
    )
    observed_after = []

    def touch(*_args, **kwargs):
        observed_after.append(kwargs["after"])
        return 10

    monkeypatch.setattr(module, "first_zone_touch", touch)
    monkeypatch.setattr(
        module,
        "liquidity",
        lambda *_args, **_kwargs: IndicatorResult(
            Status.PASS,
            {"sweep_extreme": "4"},
            aggregates[60][11].close_time,
            aggregates[60][11].close_time,
            ("sweep_reclaim",),
        ),
    )
    monkeypatch.setattr(
        module,
        "displacement",
        lambda source, direction, index, strategy: (
            result(Status.PASS) if index == 12 else result(Status.FAIL)
        ),
    )
    monkeypatch.setattr(module, "mss", lambda *_: result(Status.PASS))
    monkeypatch.setattr(
        module,
        "fvg",
        lambda *_: IndicatorResult(
            Status.PASS,
            {"low": "10", "high": "12", "entry": "11"},
            aggregates[60][13].close_time,
            aggregates[60][14].close_time,
            ("first_fvg_retraced",),
        ),
    )
    monkeypatch.setattr(module, "risk", lambda *_: result(Status.PASS))

    analyzed = module.analyze_symbol_v2(Snapshot(), "BTCUSDT", StrategyConfig(atr_period=3))

    assert analyzed["decision"] == {
        "status": "TRADE",
        "passed_gates": list(module.V2_GATES),
        "reason_codes": ["all_gates_passed"],
    }
    assert observed_after == [aggregates[240][10].close_time]
    assert observed_context_lengths == [1, 11, 11]
