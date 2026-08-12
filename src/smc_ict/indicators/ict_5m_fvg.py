from ..models import IndicatorResult, Status


def evaluate(bars, direction, start, end, config):
    formation = None
    for i in range(max(start + 2, 2), min(end + 1, len(bars))):
        a, c = bars[i - 2], bars[i]
        if direction == "long" and c.low > a.high:
            lo, hi = a.high, c.low
        elif direction == "short" and c.high < a.low:
            lo, hi = c.high, a.low
        else:
            continue
        entry = lo + (hi - lo) * config.fvg_entry_fraction
        formation = (i, c, lo, hi, entry)
        break
    if formation is None:
        return IndicatorResult(Status.FAIL, {}, reason_codes=("no_fvg",))
    i, formed_bar, lo, hi, entry = formation
    for fill in bars[i + 1 : i + 1 + config.fvg_wait_bars]:
        touched = fill.low <= entry if direction == "long" else fill.high >= entry
        if touched:
            return IndicatorResult(
                Status.PASS,
                {"low": str(lo), "high": str(hi), "entry": str(entry)},
                formed_bar.close_time,
                fill.close_time,
                ("first_fvg_retraced",),
            )
    return IndicatorResult(
        Status.FAIL,
        {"low": str(lo), "high": str(hi), "entry": str(entry)},
        formed_bar.close_time,
        bars[min(len(bars) - 1, i + config.fvg_wait_bars)].close_time,
        ("fvg_retracement_expired",),
    )
