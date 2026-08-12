from smc_ict.models import IndicatorResult, Status
from smc_ict.pipeline.state_machine import decide

GATES = (
    "smc_4h_structure",
    "smc_1h_dealing_range",
    "smc_1h_order_block",
    "ict_5m_liquidity",
    "ict_5m_displacement",
    "ict_5m_mss",
    "ict_5m_fvg",
    "risk",
)


def results(statuses):
    return {name: IndicatorResult(statuses.get(name, Status.PASS), {}) for name in GATES}


def test_trade_requires_every_gate_to_pass():
    decision = decide(results({}))
    assert decision["status"] == "TRADE"
    assert decision["passed_gates"] == list(GATES)


def test_state_stops_at_first_failed_gate():
    decision = decide(results({"ict_5m_mss": Status.FAIL}))
    assert decision["status"] == "NO_SETUP"
    assert decision["failed_gate"] == "ict_5m_mss"
    assert "ict_5m_fvg" not in decision["passed_gates"]


def test_unavailable_is_not_misreported_as_no_signal():
    decision = decide(results({"smc_1h_order_block": Status.UNAVAILABLE}))
    assert decision["status"] == "BLOCKED"
    assert decision["failed_gate"] == "smc_1h_order_block"
