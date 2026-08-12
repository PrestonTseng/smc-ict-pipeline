from dataclasses import dataclass
from decimal import Decimal

from ..models import Bar


@dataclass(frozen=True, slots=True)
class Swing:
    event_index: int
    known_index: int
    kind: str
    price: Decimal


def confirmed_swings(bars: tuple[Bar, ...], length: int) -> tuple[Swing, ...]:
    out = []
    for i in range(length, len(bars) - length):
        window = bars[i - length : i + length + 1]
        if (
            bars[i].high == max(x.high for x in window)
            and sum(x.high == bars[i].high for x in window) == 1
        ):
            out.append(Swing(i, i + length, "high", bars[i].high))
        if (
            bars[i].low == min(x.low for x in window)
            and sum(x.low == bars[i].low for x in window) == 1
        ):
            out.append(Swing(i, i + length, "low", bars[i].low))
    return tuple(out)
