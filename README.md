# SMC/ICT Pipeline

A point-in-time-safe research pipeline that uses **SMC for 4H/1H context** and **ICT for 5m execution gates**. Binance USDⓈ-M closed 1m klines are the only market-data source of truth; 5m, 1H, and 4H bars are derived locally from one committed snapshot.

> Research software only. It does not place orders, is not financial advice, and makes no profitability claim.

## Architecture

```text
cron → lock → Binance 1m fetch → validate complete BTC/ETH universe
     → SQLite atomic dataset commit → pin dataset version
     → deterministic 5m/1H/4H aggregation
     → independent SMC/ICT indicators → strict gate state machine
     → immutable analysis artifact
```

TradingView is deliberately outside the numeric data path. It may be used manually to visualize results, never as a second OHLCV truth source.

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Network access to Binance public USDⓈ-M endpoints for live runs

No Binance API key is needed. There is no real-order integration.

## Quick start

```bash
uv sync --locked
uv run pytest
uv run ruff check .
uv run smc-ict run-once --config configs/default.toml --fixture
uv run smc-ict run-once --config configs/default.toml
```

A live run writes runtime data under `var/` by default:

```text
var/data/market.sqlite3
var/runs/YYYY-MM-DD/<analysis-run-id>/
```

Runtime files are ignored by Git.

## Scheduling

Use one cron entry, not separate ingest and indicator jobs. The CLI acquires a non-blocking lock and does not run indicators before the full dataset transaction commits.

```cron
# Five minutes after every 5-minute boundary
5-59/5 * * * * cd /path/to/smc-ict-pipeline && uv run smc-ict run-once --config configs/default.toml
```

Capture stdout/stderr with your scheduler. `SKIPPED_LOCKED` is a safe no-op.

## Data guarantees

- Canonical grain: closed Binance 1m klines.
- BTCUSDT and ETHUSDT must reach the same cutoff.
- Gaps, open bars, symbol mismatches, or partial-universe fetches fail closed.
- Dataset versions and observed revisions are append-only.
- Every analysis run pins one dataset version and config hash.
- Confirmed pivots record both event index and later known index to prevent lookahead.

## Indicator modules

Each module under `src/smc_ict/indicators/` is independent and returns a standard `IndicatorResult`:

- 4H structure and bias
- 1H dealing range and first-touch order block
- 5m liquidity sweep/reclaim
- displacement
- close-confirmed MSS
- first FVG
- cost-adjusted structural risk

Downstream gates remain `UNAVAILABLE` when an upstream gate did not pass; that is distinct from a valid `FAIL` or “no signal.”

## Backup

Backup is explicit and not on the signal path:

```bash
uv run smc-ict backup --source var/data/market.sqlite3 --target var/backups/market.sqlite3
```

The command uses SQLite's online backup API and reports SHA-256 plus `PRAGMA integrity_check`.

## Configuration governance

`configs/default.toml` contains all tunable values. Every run freezes the resolved configuration and hash. Daily review should create a candidate config; do not silently rewrite evidence from prior runs.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). Design details and limitations are in [docs/architecture.md](docs/architecture.md).
