import json
from urllib.error import URLError

from smc_ict.data.binance import BinanceClient


class Response:
    def __init__(self, payload):
        self.payload = payload

    def read(self):
        return json.dumps(self.payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def row(open_time):
    return [open_time, "1", "3", "0.5", "2", "4", open_time + 59_999, "8", 2, "1", "2", "0"]


def test_kline_pagination_advances_until_short_page():
    requests = []

    def opener(request, timeout):
        requests.append(request.full_url)
        start = int(request.full_url.split("startTime=")[1].split("&")[0])
        count = 1500 if len(requests) == 1 else 2
        return Response([row(start + i * 60_000) for i in range(count)])

    bars = BinanceClient(opener=opener).fetch_1m("BTCUSDT", 0, 1501 * 60_000 + 59_999)
    assert len(bars) == 1502 and len(requests) == 2
    assert "startTime=90000000" in requests[1]


def test_transient_failure_retries_without_changing_cursor():
    calls = 0
    sleeps = []

    def opener(request, timeout):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise URLError("temporary")
        return Response([row(0)])

    bars = BinanceClient(opener=opener, retries=2, sleeper=sleeps.append).fetch_1m(
        "BTCUSDT", 0, 59_999
    )
    assert len(bars) == 1 and calls == 2 and sleeps == [1]
