# SMC/ICT Pipeline

A point-in-time-safe research pipeline whose current strategy is **1D regime → 4H context/POI → 1H execution**. Binance USDⓈ-M closed 1m klines are the only market-data source of truth; complete UTC-aligned 1D, 4H, and 1H bars are derived locally from one committed snapshot. Historical schema-v1 evidence remains readable as `v1-4h-1h-5m` but is not extended after the v2 cutover.

> Research software only. It does not place orders, is not financial advice, and makes no profitability claim.

## Architecture

```text
cron → lock → Binance 1m fetch → validate complete BTC/ETH universe
     → SQLite atomic dataset commit → pin dataset version
     → deterministic UTC 1D/4H/1H aggregation
     → strict v2 SMC/ICT gate state machine
     → schema-v2 immutable artifact + versioned casebook
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
at most one immutable analysis run per closed UTC 1H boundary. A repeated
boundary returns `SKIPPED_ALREADY_ANALYZED` without adding an artifact. Both
commands share the same non-blocking process lock, so analysis cannot overlap
ingestion.

```cron
# Every minute: canonical closed 1m ingestion.
* * * * * cd /path/to/smc-ict-pipeline && scripts/scheduled-ingest.sh
# Minute 1 each hour: analyze the latest complete closed-1H boundary and rebuild casebook.
1 * * * * cd /path/to/smc-ict-pipeline && scripts/scheduled-analysis.sh
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

- 1D regime and 4H aligned structure
- 4H dealing range and first-touch order block
- 1H liquidity sweep/reclaim
- 1H displacement and close-confirmed MSS
- first 1H FVG retracement
- cost-adjusted structural risk

The selected 4H POI touch is also the causal cutoff: the 1D regime and 4H context are recomputed only from bars complete by that touch, and structural target swings must have been known strictly before it. This prevents later higher-timeframe information from validating an earlier setup.

Some pure indicator modules retain their historical v1 filenames; v2 passes
only the documented 4H or 1H bars into those primitives. Artifact gate names
and `strategy_version` are the public strategy contract.

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
  --snapshot-output var/evidence/snapshots/<analysis-boundary> \
  --milestone-target 20
```

Each case includes `pipeline_steps` in authoritative gate order. Every step keeps
its status, reason codes, value, reference levels, event time, known-at time,
input hash, and config hash from the immutable `decision.json`; the casebook does
not recompute historical indicators. The immutable snapshot directory contains
`casebook.json`, an hourly boundary × symbol Markdown matrix with detailed step
evidence, and a one-row-per-step CSV for filtering and pivoting. The scheduled
analysis wrapper publishes one new snapshot directory per hourly boundary.

The reader verifies the exact run artifact set, manifest identities, config and
decision SHA-256 values, per-indicator provenance, and aggregate decision
consistency before emitting one observation row per eligible run and symbol.
Legacy runs are verified and explicitly excluded rather than silently treated as
forward evidence. `NO_SETUP` rows remain part of the denominator.
Top-level active counts are always v2, including zero-v2 startup. Schema-v2 rows additionally require an exact repository `PUBLISHED` claim commitment to the run manifest; crash-left or unauthenticated directories fail closed.

Case rows are observations, not automatically unique signals or trades. Review
`unique_dataset_cutoffs` separately from `eligible_cases`; neither count proves
profitability. This command creates no orders, computes no PnL, changes no
strategy parameters, and does not enable scheduling or promotion.

## Configuration governance

`configs/default.toml` contains all tunable values. Every run freezes the resolved configuration and hash. Daily review should create a candidate config; do not silently rewrite evidence from prior runs.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). Design details and limitations are in [docs/architecture.md](docs/architecture.md).
