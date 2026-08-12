from decimal import Decimal

from smc_ict.config import StrategyConfig
from smc_ict.indicators.ict_5m_displacement import evaluate as displacement
from smc_ict.indicators.ict_5m_fvg import evaluate as fvg
from smc_ict.indicators.ict_5m_liquidity import evaluate as liquidity
from smc_ict.indicators.ict_5m_mss import evaluate as mss
from smc_ict.indicators.risk import evaluate as risk
from smc_ict.indicators.smc_1h_dealing_range import evaluate as dealing
from smc_ict.indicators.smc_4h_structure import evaluate as structure
from smc_ict.indicators.swings import confirmed_swings
from smc_ict.models import Bar, Bias, Status


def mk(highs, lows=None, closes=None):
    lows = lows or [x - 2 for x in highs]
    closes = closes or [x - 1 for x in highs]
    return tuple(
        Bar(
            "BTCUSDT",
            i * 300000,
            (i + 1) * 300000 - 1,
            Decimal(str(closes[i - 1] if i else closes[i])),
            Decimal(str(highs[i])),
            Decimal(str(lows[i])),
            Decimal(str(closes[i])),
            Decimal("1"),
            True,
        )
        for i in range(len(highs))
    )


def test_pivot_known_at_is_right_confirmed_not_backfilled():
    s = confirmed_swings(mk([1, 2, 5, 2, 1]), 2)
    assert s[0].event_index == 2 and s[0].known_index == 4
    assert confirmed_swings(mk([1, 2, 5, 2]), 2) == ()


def test_structure_and_dealing_range():
    cfg = StrategyConfig(swing_length=1)
    r = structure(mk([1, 3, 2, 5, 4], [0, 1, 1, 2, 2], [1, 2, 2, 5, 3]), cfg)
    assert r.status in (Status.PASS, Status.FAIL)
    d = dealing(mk([10, 12, 11], [4, 5, 6], [7, 8, 7]), Bias.BULLISH)
    assert d.value["zone"] in {"discount", "premium", "equilibrium"}


def test_ict_chain_and_risk_are_quantitative():
    cfg = StrategyConfig(displacement_lookback=3)
    assert (
        liquidity(
            mk([10, 11, 12, 11, 13], [7, 8, 9, 7, 10], [9, 10, 11, 10, 12]), "long", Decimal("8"), 2
        ).status
        == Status.PASS
    )
    assert (
        displacement(
            mk([10, 10, 10, 10, 14], [8, 8, 8, 8, 9], [9, 9, 9, 9, 14]), "long", 4, cfg
        ).status
        == Status.PASS
    )
    assert (
        mss(mk([10, 11, 12], [8, 9, 10], [9, 10, 12]), "long", Decimal("11")).status == Status.PASS
    )
    assert (
        fvg(mk([10, 12, 15, 14], [8, 10, 13, 9], [9, 11, 14, 10]), "long", 0, 2, cfg).status
        == Status.PASS
    )
    rr = risk("long", Decimal("100"), Decimal("95"), Decimal("112"), Decimal("1"), cfg)
    assert rr.status == Status.PASS and Decimal(rr.value["net_r"]) >= 2


def test_sweep_may_reclaim_on_next_bar():
    bars = mk([11, 10, 12], [9, 7, 9], [10, 8, 11])
    result = liquidity(bars, "long", Decimal("8.5"), 0)
    assert result.status == Status.PASS
    assert result.event_time == bars[1].close_time
    assert result.known_at == bars[2].close_time


def test_sweep_search_starts_only_after_poi_touch():
    bars = mk([11, 10, 12, 11, 13], [9, 7, 9, 7, 9], [10, 9, 11, 8, 12])
    result = liquidity(bars, "long", Decimal("8.5"), reference_index=0, search_start_index=2)
    assert result.status == Status.PASS
    assert result.event_time == bars[3].close_time
