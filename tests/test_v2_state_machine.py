import pytest

EXPECTED_V2_GATES = (
    "smc_1d_regime",
    "smc_4h_structure",
    "smc_4h_dealing_range",
    "smc_4h_order_block",
    "ict_1h_liquidity",
    "ict_1h_displacement",
    "ict_1h_mss",
    "ict_1h_fvg",
    "risk",
)


def test_v2_gate_contract_is_exact_and_excludes_5m_15m():
    from smc_ict.pipeline.v2_state_machine import V2_GATES, V2_STRATEGY_VERSION

    assert V2_STRATEGY_VERSION == "v2-1d-4h-1h"
    assert V2_GATES == EXPECTED_V2_GATES
    assert all("5m" not in gate and "15m" not in gate for gate in V2_GATES)


@pytest.mark.parametrize("failed_index", range(len(EXPECTED_V2_GATES)))
def test_v2_state_machine_fails_at_first_nonpassing_gate(failed_index):
    from smc_ict.models import IndicatorResult, Status
    from smc_ict.pipeline.v2_state_machine import decide_v2

    results = {
        gate: IndicatorResult(Status.PASS, {})
        if i < failed_index
        else IndicatorResult(Status.FAIL, {}, reason_codes=("expected_failure",))
        if i == failed_index
        else IndicatorResult(Status.UNAVAILABLE, {}, reason_codes=("upstream_gate_not_passed",))
        for i, gate in enumerate(EXPECTED_V2_GATES)
    }
    decision = decide_v2(results)
    assert decision == {
        "status": "NO_SETUP",
        "failed_gate": EXPECTED_V2_GATES[failed_index],
        "passed_gates": list(EXPECTED_V2_GATES[:failed_index]),
        "reason_codes": ["expected_failure"],
    }


def test_v2_state_machine_passes_only_when_every_gate_passes():
    from smc_ict.models import IndicatorResult, Status
    from smc_ict.pipeline.v2_state_machine import decide_v2

    results = {gate: IndicatorResult(Status.PASS, {}) for gate in EXPECTED_V2_GATES}
    assert decide_v2(results) == {
        "status": "TRADE",
        "passed_gates": list(EXPECTED_V2_GATES),
        "reason_codes": ["all_gates_passed"],
    }
