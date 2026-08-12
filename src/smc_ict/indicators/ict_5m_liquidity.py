from ..models import IndicatorResult, Status


def evaluate(bars, direction, level, reference_index, search_start_index=None):
    start = max(
        reference_index + 1, (search_start_index + 1) if search_start_index is not None else 0
    )
    later = bars[start:]
    breach_index = next(
        (
            index
            for index, bar in enumerate(later, start)
            if (bar.low < level if direction == "long" else bar.high > level)
        ),
        None,
    )
    if breach_index is None:
        return IndicatorResult(Status.FAIL, {}, reason_codes=("no_liquidity_breach",))
    breach = bars[breach_index]
    for reclaim_index in range(breach_index, min(len(bars), breach_index + 2)):
        reclaim = bars[reclaim_index]
        reclaimed = reclaim.close > level if direction == "long" else reclaim.close < level
        if reclaimed:
            return IndicatorResult(
                Status.PASS,
                {"sweep_extreme": str(breach.low if direction == "long" else breach.high)},
                breach.close_time,
                reclaim.close_time,
                ("sweep_reclaim",),
                {"liquidity": str(level)},
            )
    return IndicatorResult(Status.FAIL, {}, reason_codes=("reclaim_window_expired",))
