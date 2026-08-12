from decimal import Decimal

from smc_ict.indicators.smc_1h_order_block import evaluate
from smc_ict.models import Bar, Bias, Status


def b(i, o, h, low, c):
    return Bar(
        "BTCUSDT",
        i * 3_600_000,
        (i + 1) * 3_600_000 - 1,
        *map(Decimal, map(str, (o, h, low, c))),
        Decimal("1"),
        True,
    )


def test_first_touch_bullish_order_block_associated_with_impulse():
    bars = (b(0, 10, 11, 8, 9), b(1, 9, 15, 9, 14), b(2, 14, 16, 12, 15), b(3, 15, 15, 9, 10))
    r = evaluate(bars, Bias.BULLISH, impulse_start=0, break_index=2)
    assert r.status == Status.PASS
    assert r.value["low"] == "8" and r.value["first_touch_index"] == 3


def test_order_block_rejects_prior_mitigation():
    bars = (b(0, 10, 11, 8, 9), b(1, 9, 15, 7, 14), b(2, 14, 16, 12, 15), b(3, 15, 15, 9, 10))
    assert evaluate(bars, Bias.BULLISH, 0, 2).status == Status.FAIL
