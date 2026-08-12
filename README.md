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
uv run smc-ict ingest-once --config configs/default.toml
uv run smc-ict analyze-once --config configs/default.toml
```

A live run writes runtime data under `var/` by default:

```text
var/data/market.sqlite3
var/runs/YYYY-MM-DD/<analysis-run-id>/
```

Runtime files are ignored by Git.
Fixture runs use the sibling `var-fixture/` root (or `<configured-root>-fixture`) so
deterministic test data can never share the live SQLite database.

## Scheduling

Scheduled forward capture separates cadence from data grain. `ingest-once`
fetches and atomically commits every missing closed 1m bar. `analyze-once`
never fetches market data; it pins the latest committed dataset and publishes
one immutable analysis run. Both commands share the same non-blocking process
lock, so analysis cannot overlap ingestion.

```cron
# Every minute: canonical closed 1m ingestion.
* * * * * cd /path/to/smc-ict-pipeline && scripts/scheduled-ingest.sh
# Every five minutes: analyze the latest pinned snapshot and rebuild casebook.
2-59/5 * * * * cd /path/to/smc-ict-pipeline && scripts/scheduled-analysis.sh
# Daily 10:00 in an Asia/Taipei scheduler: evidence-only human receipt.
0 10 * * * cd /path/to/smc-ict-pipeline && scripts/daily-summary.py
```

`run-once` remains available for manual ingest→analyze compatibility. Scheduled
wrappers log successful JSON receipts under ignored `var/logs/`; non-zero exits
remain visible to the scheduler. `SKIPPED_LOCKED` is a safe no-op.

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

Backups to the same target are serialized with a persistent sibling lock file;
an overlapping invocation fails closed instead of publishing a stale manifest.
Do not remove the `.lock` file while a backup may be running.

The command uses SQLite's online backup API and reports SHA-256 plus `PRAGMA integrity_check`.

## Point-in-time casebook

Build a deterministic research casebook from verified immutable runs:

```bash
uv run smc-ict casebook \
  --runs-root var/runs \
  --output var/evidence/casebook.json \
  --milestone-target 20
```

The reader verifies the exact run artifact set, manifest identities, config and
decision SHA-256 values, per-indicator provenance, and aggregate decision
consistency before emitting one observation row per eligible run and symbol.
Legacy runs are verified and explicitly excluded rather than silently treated as
forward evidence. `NO_SETUP` rows remain part of the denominator.

Case rows are observations, not automatically unique signals or trades. Review
`unique_dataset_cutoffs` separately from `eligible_cases`; neither count proves
profitability. This command creates no orders, computes no PnL, changes no
strategy parameters, and does not enable scheduling or promotion.

## Configuration governance

`configs/default.toml` contains all tunable values. Every run freezes the resolved configuration and hash. Daily review should create a candidate config; do not silently rewrite evidence from prior runs.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). Design details and limitations are in [docs/architecture.md](docs/architecture.md).
