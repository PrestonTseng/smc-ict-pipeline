"""Immutable domain contracts."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from decimal import Decimal
from enum import StrEnum
from typing import Any


class Status(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    UNAVAILABLE = "UNAVAILABLE"
    ERROR = "ERROR"


class Bias(StrEnum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"


@dataclass(frozen=True, slots=True)
class Bar:
    symbol: str
    open_time: int
    close_time: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    is_closed: bool = True

    def replace(self, **changes: Any) -> Bar:
        return replace(self, **changes)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        for k in ("open", "high", "low", "close", "volume"):
            d[k] = str(d[k])
        return d


@dataclass(frozen=True, slots=True)
class IndicatorResult:
    status: Status
    value: Any
    event_time: int | None = None
    known_at: int | None = None
    reason_codes: tuple[str, ...] = ()
    reference_levels: dict[str, str] | None = None
    input_hash: str = ""
    config_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "value": self.value,
            "event_time": self.event_time,
            "known_at": self.known_at,
            "reason_codes": list(self.reason_codes),
            "reference_levels": self.reference_levels or {},
            "input_hash": self.input_hash,
            "config_hash": self.config_hash,
        }
