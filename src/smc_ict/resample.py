"""Deterministic UTC aggregation from canonical 1m bars."""

from decimal import Decimal

from .models import Bar


def resample_bars(bars, minutes: int) -> tuple[Bar, ...]:
    if not bars:
        return ()
    size = minutes * 60_000
    groups = {}
    for b in sorted(bars, key=lambda x: x.open_time):
        groups.setdefault((b.open_time // size) * size, []).append(b)
    out = []
    for start, g in sorted(groups.items()):
        if len(g) != minutes or [x.open_time for x in g] != list(
            range(start, start + size, 60_000)
        ):
            continue
        out.append(
            Bar(
                g[0].symbol,
                start,
                start + size - 1,
                g[0].open,
                max(x.high for x in g),
                min(x.low for x in g),
                g[-1].close,
                sum((x.volume for x in g), Decimal(0)),
                all(x.is_closed for x in g),
            )
        )
    return tuple(out)
