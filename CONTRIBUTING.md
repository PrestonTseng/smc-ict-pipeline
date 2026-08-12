# Contributing

Use uv for every Python operation:

```bash
uv sync --locked
uv run pytest
uv run ruff format --check .
uv run ruff check .
```

Write a failing behavioral test before production changes. Preserve point-in-time semantics, closed-bar-only inputs, the single Binance SSOT, and dataset-version pinning. Never add credentials, real-order execution, private paths, or TradingView OHLCV as a pipeline input.
