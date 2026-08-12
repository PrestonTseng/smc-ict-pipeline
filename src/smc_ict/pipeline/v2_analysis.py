"""Strict v2 analysis over one pinned canonical snapshot."""

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
from .analysis import _atr, _first_index_at_or_after, execution_windows, first_zone_touch
from .v2_state_machine import V2_GATES, decide_v2


def unavailable(reason: str) -> IndicatorResult:
    return IndicatorResult(Status.UNAVAILABLE, {}, reason_codes=(reason,))


def _finish(results: dict[str, IndicatorResult]) -> dict:
    return {
        "indicators": {name: results[name].to_dict() for name in V2_GATES},
        "decision": decide_v2(results),
    }


def analyze_symbol_v2(snapshot, symbol: str, strategy) -> dict:
    bars1 = snapshot.bars(symbol)
    bars1440 = resample_bars(bars1, 1440)
    bars240 = resample_bars(bars1, 240)
    bars60 = resample_bars(bars1, 60)
    results = {name: unavailable("upstream_gate_not_passed") for name in V2_GATES}

    if not bars1440:
        results["smc_1d_regime"] = unavailable("insufficient_1d_warmup")
        return _finish(results)
    results["smc_1d_regime"] = structure(bars1440, strategy)
    if results["smc_1d_regime"].status != Status.PASS:
        return _finish(results)

    bias = Bias(results["smc_1d_regime"].value["bias"])
    if bias != Bias.BULLISH:
        results["smc_4h_structure"] = IndicatorResult(
            Status.FAIL, {}, reason_codes=("v2_long_only",)
        )
        return _finish(results)
    if not bars240 or not bars60:
        results["smc_4h_structure"] = unavailable("insufficient_lower_timeframes")
        return _finish(results)

    results["smc_4h_structure"] = structure(bars240, strategy)
    if results["smc_4h_structure"].status != Status.PASS:
        return _finish(results)
    if Bias(results["smc_4h_structure"].value["bias"]) != Bias.BULLISH:
        results["smc_4h_structure"] = IndicatorResult(
            Status.FAIL, {}, reason_codes=("4h_not_aligned_bullish",)
        )
        return _finish(results)

    results["smc_4h_dealing_range"] = dealing(bars240, Bias.BULLISH)
    if results["smc_4h_dealing_range"].status != Status.PASS:
        return _finish(results)

    swings240 = confirmed_swings(bars240, strategy.swing_length)
    broken_highs = [
        swing
        for swing in swings240
        if swing.kind == "high"
        and any(bar.close > swing.price for bar in bars240[swing.known_index + 1 :])
    ]
    if not broken_highs:
        results["smc_4h_order_block"] = unavailable("no_confirmed_4h_bos")
        return _finish(results)
    broken = broken_highs[-1]
    break_index = next(
        i for i in range(broken.known_index + 1, len(bars240)) if bars240[i].close > broken.price
    )
    prior_lows = [s for s in swings240 if s.kind == "low" and s.known_index < break_index]
    impulse_start = prior_lows[-1].event_index if prior_lows else max(0, broken.event_index - 3)
    results["smc_4h_order_block"] = order_block(
        bars240, Bias.BULLISH, impulse_start=impulse_start, break_index=break_index
    )
    if results["smc_4h_order_block"].status != Status.PASS:
        return _finish(results)

    poi_touch_4h = int(results["smc_4h_order_block"].value["first_touch_index"])
    ob_low = Decimal(results["smc_4h_order_block"].value["low"])
    ob_high = Decimal(results["smc_4h_order_block"].value["high"])
    poi_touch_1h = first_zone_touch(bars60, ob_low, ob_high, after=bars240[break_index].close_time)
    if poi_touch_1h is None:
        results["ict_1h_liquidity"] = unavailable("poi_touch_not_in_1h_snapshot")
        return _finish(results)

    swings60 = confirmed_swings(bars60, strategy.swing_length)
    frozen_lows = [
        swing for swing in swings60 if swing.kind == "low" and swing.known_index <= poi_touch_1h
    ]
    if not frozen_lows:
        results["ict_1h_liquidity"] = unavailable("no_frozen_sell_side_liquidity")
        return _finish(results)
    reference_low = frozen_lows[-1]
    observation_end = min(len(bars60), poi_touch_1h + strategy.poi_timeout_bars + 1)
    results["ict_1h_liquidity"] = liquidity(
        bars60[:observation_end],
        "long",
        reference_low.price,
        reference_low.event_index,
        search_start_index=poi_touch_1h,
    )
    if results["ict_1h_liquidity"].status != Status.PASS:
        return _finish(results)
    sweep_index = _first_index_at_or_after(bars60, int(results["ict_1h_liquidity"].known_at or 0))
    if sweep_index is None:
        results["ict_1h_displacement"] = unavailable("sweep_index_missing")
        return _finish(results)

    displacement_index = None
    candidate_indices, _ = execution_windows(
        sweep_index, sweep_index + 1, strategy.displacement_window
    )
    for index in candidate_indices:
        if index >= len(bars60):
            break
        candidate = displacement(bars60, "long", index, strategy)
        if candidate.status == Status.PASS:
            results["ict_1h_displacement"] = candidate
            displacement_index = index
            break
    if displacement_index is None:
        results["ict_1h_displacement"] = IndicatorResult(
            Status.FAIL, {}, reason_codes=("displacement_window_expired",)
        )
        return _finish(results)

    frozen_highs = [
        swing for swing in swings60 if swing.kind == "high" and swing.known_index <= sweep_index
    ]
    if not frozen_highs:
        results["ict_1h_mss"] = unavailable("no_frozen_opposing_swing")
        return _finish(results)
    opposing_1h = frozen_highs[-1]
    results["ict_1h_mss"] = mss(bars60[: displacement_index + 1], "long", opposing_1h.price)
    if results["ict_1h_mss"].status != Status.PASS:
        return _finish(results)

    _, fvg_start = execution_windows(sweep_index, displacement_index, strategy.displacement_window)
    fvg_end = min(len(bars60) - 1, displacement_index + 2)
    results["ict_1h_fvg"] = fvg(bars60, "long", fvg_start, fvg_end, strategy)
    if results["ict_1h_fvg"].status != Status.PASS:
        return _finish(results)

    entry = Decimal(results["ict_1h_fvg"].value["entry"])
    fill_index = _first_index_at_or_after(bars60, int(results["ict_1h_fvg"].known_at or 0))
    atr = _atr(bars60, fill_index or 0, strategy.atr_period)
    target_swings = [s for s in swings240 if s.kind == "high" and s.known_index <= poi_touch_4h]
    if atr is None or not target_swings:
        results["risk"] = unavailable("insufficient_frozen_risk_reference")
        return _finish(results)
    target = target_swings[-1].price
    sweep_extreme = Decimal(results["ict_1h_liquidity"].value["sweep_extreme"])
    results["risk"] = risk("long", entry, sweep_extreme, target, atr, strategy)
    return _finish(results)
