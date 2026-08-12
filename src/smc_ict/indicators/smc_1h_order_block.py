from ..models import Bias, IndicatorResult, Status


def evaluate(bars, bias: Bias, impulse_start: int, break_index: int):
    candidates = []
    for i in range(impulse_start, break_index):
        bearish = bars[i].close < bars[i].open
        bullish = bars[i].close > bars[i].open
        if (bias == Bias.BULLISH and bearish) or (bias == Bias.BEARISH and bullish):
            candidates.append(i)
    if not candidates:
        return IndicatorResult(Status.FAIL, {}, reason_codes=("no_directional_ob",))
    i = candidates[-1]
    ob = bars[i]
    # Any penetration before the break means the candidate was already mitigated.
    middle = bars[i + 1 : break_index]
    mitigated = (
        any(x.low < ob.low for x in middle)
        if bias == Bias.BULLISH
        else any(x.high > ob.high for x in middle)
    )
    if mitigated:
        return IndicatorResult(Status.FAIL, {}, reason_codes=("prior_mitigation",))
    after = bars[break_index + 1 :]
    touch = None
    for j, x in enumerate(after, break_index + 1):
        if x.low <= ob.high and x.high >= ob.low:
            touch = j
            break
    if touch is None:
        return IndicatorResult(Status.FAIL, {}, reason_codes=("no_first_touch",))
    return IndicatorResult(
        Status.PASS,
        {"low": str(ob.low), "high": str(ob.high), "origin_index": i, "first_touch_index": touch},
        bars[touch].close_time,
        bars[touch].close_time,
        ("first_touch_ob",),
    )
