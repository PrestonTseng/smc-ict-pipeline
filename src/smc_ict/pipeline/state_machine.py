"""Strict, non-weighted SMC→ICT gate state machine."""

from __future__ import annotations

from ..models import IndicatorResult, Status

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


def decide(results: dict[str, IndicatorResult]) -> dict:
    passed: list[str] = []
    for gate in GATES:
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
