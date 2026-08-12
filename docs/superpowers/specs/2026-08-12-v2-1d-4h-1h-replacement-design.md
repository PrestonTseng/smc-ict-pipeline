# v2 1D→4H→1H Replacement Design

## Objective

Replace future production analysis from `4H→1H→5m` (v1) to `1D→4H→1H` (v2) without interrupting canonical closed-1m ingestion or rewriting prior immutable evidence.

## Data contract

- Binance USDⓈ-M closed 1m remains the only canonical market-data grain.
- BTCUSDT and ETHUSDT still publish atomically as one `COMMITTED` pinned dataset.
- 1D, 4H, and 1H bars are complete UTC-aligned local aggregates of 1440, 240, and 60 canonical 1m rows.
- Missing input makes the affected gate `UNAVAILABLE`; developing higher-timeframe bars are never used.

## Decision tree

Strict long-only fail-fast order:

1. `smc_1d_regime`: close-confirmed bullish BOS over confirmed daily swings.
2. `smc_4h_structure`: close-confirmed bullish BOS aligned with 1D.
3. `smc_4h_dealing_range`: latest 4H close lies in discount.
4. `smc_4h_order_block`: latest directional candle before confirmed 4H BOS is unmitigated and receives its first post-break touch.
5. `ict_1h_liquidity`: strictly after the 4H POI touch bar has closed (`order_block.known_at`), breach and reclaim a pre-existing confirmed 1H swing low; never search inside the still-forming 4H touch bar.
6. `ict_1h_displacement`: qualifying bullish displacement within the configured post-sweep window.
7. `ict_1h_mss`: displacement closes above the opposing 1H swing high frozen before the sweep.
8. `ict_1h_fvg`: first bullish FVG retraces to its configured 50% entry within the wait window.
9. `risk`: stop beyond the 1H sweep extreme plus ATR buffer; target the frozen opposing 4H swing high; require fee/slippage-adjusted net R ≥ 2.

The existing configuration bar counts become v2 execution-bar counts and are frozen in every run. They are not silently optimized from evidence.

## Version and evidence boundary

- New manifests declare `strategy_version = "v2-1d-4h-1h"`.
- Existing schema-v1 artifacts without this key are interpreted only as `v1-4h-1h-5m` history.
- Casebook rows carry strategy version and summaries expose per-version denominators.
- Historical bytes are never rewritten; v1 and v2 rows are never pooled for strategy conclusions.

## Scheduler cutover

- Closed-1m ingestion remains every minute.
- After reviewed publication and successful CI, remove the v1 five-minute analysis job and create a v2 job at minute 1 of every hour.
- v2 derives `analysis_boundary` as the latest complete UTC 1H close at or before the pinned dataset cutoff. It analyzes at most once per new boundary; later minute-level datasets within the same boundary return an auditable skip receipt and create no artifact.
- Daily summary remains at 10:00 Asia/Taipei and reports versioned denominators.

## Safety

Research-only; no exchange credentials, orders, PnL fabrication, automatic tuning, historical backfill, or rule promotion.
