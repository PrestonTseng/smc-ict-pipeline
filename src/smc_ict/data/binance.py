"""Read-only Binance USD-M public market adapter."""

from __future__ import annotations

import json
import time
from decimal import Decimal
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ..models import Bar

BASE = "https://fapi.binance.com"


class BinanceClient:
    def __init__(self, opener=urlopen, timeout=20, retries=3, sleeper=time.sleep):
        self.opener = opener
        self.timeout = timeout
        self.retries = retries
        self.sleeper = sleeper

    def _get(self, path, params=None):
        url = BASE + path + ("?" + urlencode(params) if params else "")
        request = Request(url, headers={"User-Agent": "smc-ict-pipeline/0.1"})
        for attempt in range(self.retries + 1):
            try:
                with self.opener(request, timeout=self.timeout) as response:
                    return json.loads(response.read())
            except Exception:
                if attempt == self.retries:
                    raise
                self.sleeper(2**attempt)
        raise AssertionError("retry loop must return or raise")

    def server_time(self):
        return int(self._get("/fapi/v1/time")["serverTime"])

    def latest_closed_cutoff(self):
        return (self.server_time() // 60_000) * 60_000 - 1

    def fetch_1m(self, symbol, start, end):
        rows = []
        cursor = start
        while cursor <= end:
            payload = self._get(
                "/fapi/v1/klines",
                {
                    "symbol": symbol,
                    "interval": "1m",
                    "startTime": cursor,
                    "endTime": end,
                    "limit": 1500,
                },
            )
            if not payload:
                break
            rows.extend(payload)
            nxt = int(payload[-1][0]) + 60_000
            if nxt <= cursor:
                raise RuntimeError("non-advancing Binance pagination")
            cursor = nxt
            if len(payload) < 1500:
                break
        return [
            Bar(symbol, int(x[0]), int(x[6]), *(Decimal(str(v)) for v in x[1:6]), int(x[6]) <= end)
            for x in rows
        ]


class FixtureBinanceClient:
    def __init__(self, fail_symbol=None):
        self.fail_symbol = fail_symbol

    def latest_closed_cutoff(self):
        return 17_999_999

    def fetch_1m(self, symbol, start, end):
        if symbol == self.fail_symbol:
            raise RuntimeError("fixture failure")
        n = (end - start + 1) // 60_000
        out = []
        for i in range(n):
            t = start + i * 60_000
            p = Decimal("100") + Decimal(i % 37) / 10
            out.append(
                Bar(symbol, t, t + 59_999, p, p + 1, p - 1, p + Decimal("0.2"), Decimal("10"), True)
            )
        return out
