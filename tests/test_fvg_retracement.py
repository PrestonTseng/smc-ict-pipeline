from decimal import Decimal

from smc_ict.config import StrategyConfig
from smc_ict.indicators.ict_5m_fvg import evaluate
from smc_ict.models import Bar, Status


def bar(i, high, low, close):
    return Bar(
        "BTCUSDT",
        i * 300_000,
        (i + 1) * 300_000 - 1,
        Decimal(str(close)),
        Decimal(str(high)),
        Decimal(str(low)),
        Decimal(str(close)),
        Decimal("1"),
        True,
    )


def test_fvg_requires_later_midpoint_retracement_and_known_at_fill():
    cfg = StrategyConfig(fvg_wait_bars=2)
    bars = (
        bar(0, 10, 8, 9),
        bar(1, 13, 10, 12),
        bar(2, 15, 12, 14),
        bar(3, 16, 13, 15),
        bar(4, 14, 10, 11),
    )
    result = evaluate(bars, "long", 0, 2, cfg)
    assert result.status == Status.PASS
    assert Decimal(result.value["entry"]) == Decimal("11")
    assert result.known_at == bars[4].close_time


def test_fvg_expires_without_retracement():
    cfg = StrategyConfig(fvg_wait_bars=2)
    bars = (
        bar(0, 10, 8, 9),
        bar(1, 13, 10, 12),
        bar(2, 15, 12, 14),
        bar(3, 16, 13, 15),
        bar(4, 17, 14, 16),
    )
    assert evaluate(bars, "long", 0, 2, cfg).status == Status.FAIL
