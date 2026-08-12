from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from smc_ict.config import StrategyConfig, config_hash
from smc_ict.models import IndicatorResult, Status


def test_config_is_immutable_and_validated():
    cfg = StrategyConfig()
    assert cfg.displacement_body_multiple == Decimal("1.25")
    with pytest.raises(FrozenInstanceError):
        cfg.minimum_r = Decimal("1")
    with pytest.raises(ValueError):
        StrategyConfig(fvg_wait_bars=0)


def test_config_hash_and_result_wire_are_deterministic():
    cfg = StrategyConfig()
    assert config_hash(cfg) == config_hash(StrategyConfig())
    result = IndicatorResult(Status.PASS, {"price": "10"}, 1, 2, ("ok",))
    assert result.to_dict()["known_at"] == 2
