from ..models import Bias, IndicatorResult, Status
from .swings import confirmed_swings


def evaluate(bars, config):
    swings = confirmed_swings(tuple(bars), config.swing_length)
    highs = [s for s in swings if s.kind == "high"]
    lows = [s for s in swings if s.kind == "low"]
    bias = Bias.NEUTRAL
    reasons = ["no_confirmed_bos"]
    if highs and bars[-1].close > highs[-1].price:
        bias = Bias.BULLISH
        reasons = ["bullish_bos"]
    elif lows and bars[-1].close < lows[-1].price:
        bias = Bias.BEARISH
        reasons = ["bearish_bos"]
    return IndicatorResult(
        Status.PASS if bias != Bias.NEUTRAL else Status.FAIL,
        {"bias": bias.value},
        bars[-1].close_time,
        bars[-1].close_time,
        tuple(reasons),
    )
