import json

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


def test_server_cutoff_and_closed_kline_mapping():
    calls = []

    def opener(request, timeout):
        calls.append((request.full_url, timeout))
        if request.full_url.endswith("/fapi/v1/time"):
            return Response({"serverTime": 180_500})
        return Response([[60_000, "1", "3", "0.5", "2", "4", 119_999, "8", 2, "1", "2", "0"]])

    c = BinanceClient(opener=opener, timeout=7)
    assert c.latest_closed_cutoff() == 179_999
    bars = c.fetch_1m("BTCUSDT", 60_000, 119_999)
    assert len(bars) == 1 and bars[0].is_closed and calls[-1][1] == 7
    assert "symbol=BTCUSDT" in calls[-1][0] and "interval=1m" in calls[-1][0]
