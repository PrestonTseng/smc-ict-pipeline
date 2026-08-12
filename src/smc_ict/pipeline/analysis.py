"""Strict gate analysis over a pinned repository snapshot."""

from __future__ import annotations

from decimal import Decimal

from ..indicators.ict_5m_displacement import evaluate as displacement
from ..indicators.ict_5m_fvg import evaluate as fvg
from ..indicators.ict_5m_liquidity import evaluate as liquidity
from ..indicators.ict_5m_mss import evaluate as mss
from ..indicators.risk import evaluate as risk
from ..indicators.smc_1h_dealing_range import evaluate as dealing
from ..indicators.smc_1h_order_block import evaluate as order_block
from ..indicators.smc_4h_structure import evaluate as structure
from ..indicators.swings import confirmed_swings
from ..models import Bias, IndicatorResult, Status
from ..resample import resample_bars
from .state_machine import GATES, decide


def unavailable(reason: str) -> IndicatorResult:
    return IndicatorResult(Status.UNAVAILABLE, {}, reason_codes=(reason,))


def _finish(results: dict[str, IndicatorResult]) -> dict:
    return {
        "indicators": {name: results[name].to_dict() for name in GATES},
        "decision": decide(results),
    }


def _first_index_at_or_after(bars, timestamp: int) -> int | None:
    return next((i for i, bar in enumerate(bars) if bar.close_time >= timestamp), None)


def first_zone_touch(bars, low: Decimal, high: Decimal, after: int) -> int | None:
    return next(
        (
            index
            for index, bar in enumerate(bars)
            if bar.open_time > after and bar.low <= high and bar.high >= low
        ),
        None,
    )


def _atr(bars, end_index: int, period: int) -> Decimal | None:
    start = end_index - period + 1
    if start < 0:
        return None
    true_ranges = []
    for i in range(start, end_index + 1):
        previous_close = bars[i - 1].close if i else bars[i].open
        true_ranges.append(
            max(
                bars[i].high - bars[i].low,
                abs(bars[i].high - previous_close),
                abs(bars[i].low - previous_close),
            )
        )
    return sum(true_ranges, Decimal(0)) / Decimal(period)


def execution_windows(
    sweep_index: int, displacement_index: int, displacement_window: int
) -> tuple[range, int]:
    candidates = range(sweep_index + 1, sweep_index + displacement_window + 1)
    return candidates, max(0, displacement_index - 2)


def analyze_symbol(snapshot, symbol: str, strategy) -> dict:
    bars1 = snapshot.bars(symbol)
    bars5 = resample_bars(bars1, 5)
    bars60 = resample_bars(bars1, 60)
    bars240 = resample_bars(bars1, 240)
    results = {name: unavailable("upstream_gate_not_passed") for name in GATES}

    if not bars240:
        results["smc_4h_structure"] = unavailable("insufficient_4h_warmup")
        return _finish(results)
    results["smc_4h_structure"] = structure(bars240, strategy)
    if results["smc_4h_structure"].status != Status.PASS:
        return _finish(results)

    bias = Bias(results["smc_4h_structure"].value["bias"])
    if bias != Bias.BULLISH:
        results["smc_1h_dealing_range"] = IndicatorResult(
            Status.FAIL, {}, reason_codes=("v0_1_long_only",)
        )
        return _finish(results)
    if not bars60 or not bars5:
        results["smc_1h_dealing_range"] = unavailable("insufficient_lower_timeframes")
        return _finish(results)

    results["smc_1h_dealing_range"] = dealing(bars60, bias)
    if results["smc_1h_dealing_range"].status != Status.PASS:
        return _finish(results)

    swings60 = confirmed_swings(bars60, strategy.swing_length)
    broken_highs = [
        swing
        for swing in swings60
        if swing.kind == "high"
        and any(bar.close > swing.price for bar in bars60[swing.known_index + 1 :])
    ]
    if not broken_highs:
        results["smc_1h_order_block"] = unavailable("no_confirmed_1h_bos")
        return _finish(results)
    broken = broken_highs[-1]
    break_index = next(
        i for i in range(broken.known_index + 1, len(bars60)) if bars60[i].close > broken.price
    )
    prior_lows = [s for s in swings60 if s.kind == "low" and s.known_index < break_index]
    impulse_start = prior_lows[-1].event_index if prior_lows else max(0, broken.event_index - 3)
    results["smc_1h_order_block"] = order_block(
        bars60, bias, impulse_start=impulse_start, break_index=break_index
    )
    if results["smc_1h_order_block"].status != Status.PASS:
        return _finish(results)

    poi_touch_1h = int(results["smc_1h_order_block"].value["first_touch_index"])
    ob_low = Decimal(results["smc_1h_order_block"].value["low"])
    ob_high = Decimal(results["smc_1h_order_block"].value["high"])
    poi_touch_5m = first_zone_touch(bars5, ob_low, ob_high, after=bars60[break_index].close_time)
    if poi_touch_5m is None:
        results["ict_5m_liquidity"] = unavailable("poi_touch_not_in_5m_snapshot")
        return _finish(results)
    swings5 = confirmed_swings(bars5, strategy.swing_length)
    frozen_lows = [
        swing for swing in swings5 if swing.kind == "low" and swing.known_index <= poi_touch_5m
    ]
    if not frozen_lows:
        results["ict_5m_liquidity"] = unavailable("no_frozen_sell_side_liquidity")
        return _finish(results)
    reference_low = frozen_lows[-1]
    observation_end = min(len(bars5), poi_touch_5m + strategy.poi_timeout_bars + 1)
    observation = bars5[:observation_end]
    results["ict_5m_liquidity"] = liquidity(
        observation,
        "long",
        reference_low.price,
        reference_low.event_index,
        search_start_index=poi_touch_5m,
    )
    if results["ict_5m_liquidity"].status != Status.PASS:
        return _finish(results)
    sweep_index = _first_index_at_or_after(bars5, int(results["ict_5m_liquidity"].known_at or 0))
    if sweep_index is None:
        results["ict_5m_displacement"] = unavailable("sweep_index_missing")
        return _finish(results)

    displacement_index = None
    candidate_indices, _ = execution_windows(
        sweep_index, sweep_index + 1, strategy.displacement_window
    )
    for index in candidate_indices:
        if index >= len(bars5):
            break
        candidate = displacement(bars5, "long", index, strategy)
        if candidate.status == Status.PASS:
            results["ict_5m_displacement"] = candidate
            displacement_index = index
            break
    if displacement_index is None:
        results["ict_5m_displacement"] = IndicatorResult(
            Status.FAIL, {}, reason_codes=("displacement_window_expired",)
        )
        return _finish(results)

    frozen_highs = [
        swing for swing in swings5 if swing.kind == "high" and swing.known_index <= sweep_index
    ]
    if not frozen_highs:
        results["ict_5m_mss"] = unavailable("no_frozen_opposing_swing")
        return _finish(results)
    opposing_5m = frozen_highs[-1]
    results["ict_5m_mss"] = mss(bars5[: displacement_index + 1], "long", opposing_5m.price)
    if results["ict_5m_mss"].status != Status.PASS:
        return _finish(results)

    _, fvg_start = execution_windows(sweep_index, displacement_index, strategy.displacement_window)
    fvg_end = min(len(bars5) - 1, displacement_index + 2)
    results["ict_5m_fvg"] = fvg(bars5, "long", fvg_start, fvg_end, strategy)
    if results["ict_5m_fvg"].status != Status.PASS:
        return _finish(results)
    entry = Decimal(results["ict_5m_fvg"].value["entry"])
    fill_index = _first_index_at_or_after(bars5, int(results["ict_5m_fvg"].known_at or 0))
    atr = _atr(bars5, fill_index or 0, strategy.atr_period)
    target_swings = [s for s in swings60 if s.kind == "high" and s.known_index <= poi_touch_1h]
    if atr is None or not target_swings:
        results["risk"] = unavailable("insufficient_frozen_risk_reference")
        return _finish(results)
    target = target_swings[-1].price
    sweep_extreme = Decimal(results["ict_5m_liquidity"].value["sweep_extreme"])
    results["risk"] = risk("long", entry, sweep_extreme, target, atr, strategy)
    return _finish(results)
