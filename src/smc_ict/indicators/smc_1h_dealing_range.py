from ..models import Bias, IndicatorResult, Status


def evaluate(bars, bias: Bias):
    hi = max(x.high for x in bars)
    lo = min(x.low for x in bars)
    mid = (hi + lo) / 2
    price = bars[-1].close
    zone = "discount" if price < mid else "premium" if price > mid else "equilibrium"
    eligible = (bias == Bias.BULLISH and zone == "discount") or (
        bias == Bias.BEARISH and zone == "premium"
    )
    return IndicatorResult(
        Status.PASS if eligible else Status.FAIL,
        {"zone": zone, "high": str(hi), "low": str(lo), "mid": str(mid)},
        bars[-1].close_time,
        bars[-1].close_time,
    )
