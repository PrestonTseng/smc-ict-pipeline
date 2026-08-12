# Data and execution architecture

## Boundaries

`BinanceClient` is the only remote market adapter. `MarketRepository` is the only SQLite boundary. `Snapshot` pins one committed dataset version. Indicators are pure functions over immutable bars and config. `Orchestrator` is the sole public path that may fetch, commit, analyze, and publish.

## Point-in-time rules

- Only bars whose close time is at or before the Binance-derived cutoff are accepted.
- All higher timeframes are complete UTC windows derived from canonical 1m bars.
- A pivot is available only after its configured right-side confirmation bars close.
- Historical event time and signal availability (`known_at`) remain distinct.
- Analysis never queries “latest” after pinning a dataset version.

## Publication and recovery

The complete BTC/ETH universe is validated before a single SQLite transaction inserts a dataset and its rows. Analysis artifacts are written under `.tmp` and renamed to a unique final directory. Failed ingestion creates no analysis artifact. A committed dataset may be analyzed again after an analysis-only failure.

## Known POC limitations

- This is deterministic research infrastructure, not a proven trading edge.
- The first implementation intentionally keeps SMC/ICT definitions compact and inspectable; semantic QA cases should be converted into golden tests before relying on large backtests.
- Fixture data proves orchestration, not market profitability.
- Live public Binance availability can vary by jurisdiction/network.
- There is no order placement, leverage, liquidation, portfolio sizing, or exchange account state.
