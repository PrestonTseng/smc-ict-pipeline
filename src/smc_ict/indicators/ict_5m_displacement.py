from decimal import Decimal
from statistics import median

from ..models import IndicatorResult, Status


def evaluate(bars, direction, index, config):
    b = bars[index]
    prior = bars[max(0, index - config.displacement_lookback) : index]
    if not prior:
        return IndicatorResult(Status.UNAVAILABLE, {})
    body = abs(b.close - b.open)
    med = median(abs(x.close - x.open) for x in prior)
    span = b.high - b.low
    ratio = body / span if span else Decimal(0)
    edge = (
        (b.high - b.close) / span
        if direction == "long" and span
        else (b.close - b.low) / span
        if span
        else Decimal(1)
    )
    directional = b.close > b.open if direction == "long" else b.close < b.open
    ok = (
        directional
        and body >= med * config.displacement_body_multiple
        and ratio >= config.displacement_body_ratio
        and edge <= config.displacement_close_edge
    )
    return IndicatorResult(
        Status.PASS if ok else Status.FAIL,
        {"body": str(body), "median_body": str(med), "body_ratio": str(ratio)},
        b.close_time,
        b.close_time,
    )
