"""Strict, non-weighted v2 1D→4H→1H gate state machine."""

from __future__ import annotations

from ..models import IndicatorResult, Status

V2_STRATEGY_VERSION = "v2-1d-4h-1h"
V2_GATES = (
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


def decide_v2(results: dict[str, IndicatorResult]) -> dict:
    passed: list[str] = []
    for gate in V2_GATES:
        result = results[gate]
        if result.status == Status.PASS:
            passed.append(gate)
            continue
        status = "BLOCKED" if result.status == Status.UNAVAILABLE else "NO_SETUP"
        return {
            "status": status,
            "failed_gate": gate,
            "passed_gates": passed,
            "reason_codes": list(result.reason_codes),
        }
    return {"status": "TRADE", "passed_gates": passed, "reason_codes": ["all_gates_passed"]}
