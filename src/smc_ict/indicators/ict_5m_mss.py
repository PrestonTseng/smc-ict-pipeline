from ..models import IndicatorResult, Status


def evaluate(bars, direction, level):
    b = bars[-1]
    ok = b.close > level if direction == "long" else b.close < level
    return IndicatorResult(
        Status.PASS if ok else Status.FAIL,
        {"level": str(level)},
        b.close_time,
        b.close_time,
        ("close_mss",) if ok else ("no_close_mss",),
    )
