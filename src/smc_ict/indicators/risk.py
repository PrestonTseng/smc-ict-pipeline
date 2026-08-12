from decimal import Decimal

from ..models import IndicatorResult, Status


def evaluate(direction, entry, sweep_extreme, target, atr, config):
    stop = (
        sweep_extreme - atr * config.atr_buffer
        if direction == "long"
        else sweep_extreme + atr * config.atr_buffer
    )
    risk = entry - stop if direction == "long" else stop - entry
    reward = target - entry if direction == "long" else entry - target
    costs = entry * (config.fee_bps + config.slippage_bps) * Decimal(2) / Decimal(10000)
    net = (reward - costs) / (risk + costs) if risk > 0 else Decimal("-1")
    return IndicatorResult(
        Status.PASS if net >= config.minimum_r else Status.FAIL,
        {"entry": str(entry), "stop": str(stop), "target": str(target), "net_r": str(net)},
    )
