# v2 1D→4H→1H Replacement Design

## Objective

Replace future production analysis from `4H→1H→5m` (v1) to `1D→4H→1H` (v2) without interrupting canonical closed-1m ingestion. Preserve v1 bytes through controlled v2 verification, then delete v1 strategy-specific artifacts under Preston's explicit approval; never delete the shared canonical market-data SSOT.

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

The 4H POI touch close is the common causal cutoff for all higher-timeframe gates: 1D regime, 4H structure, and 4H dealing range are recomputed using only bars complete at or before that cutoff. Later 1D/4H information may not qualify an earlier POI. Every frozen 4H target swing must be known strictly before the POI touch.

The existing configuration bar counts become v2 execution-bar counts and are frozen in every run. They are not silently optimized from evidence.

## Version and evidence boundary

- New manifests declare `strategy_version = "v2-1d-4h-1h"`.
- Existing schema-v1 artifacts without this key are interpreted only as `v1-4h-1h-5m` history.
- Casebook rows carry strategy version and summaries expose per-version denominators.
- Historical bytes are never rewritten. Before approved v1 deletion, v1 and v2 rows remain separate and are never pooled for strategy conclusions.
- Active summaries, milestone gates, status/reason counts, and unique closed-1H denominators are v2-only. Any total case count is inventory, not strategy evidence.
- A schema-v2 run is eligible only when the canonical SQLite repository contains a matching `PUBLISHED` claim committed to the exact strategy, boundary, run ID, day, and manifest SHA-256. A crash-left public directory under a `CLAIMED` row is not evidence.
- Every non-null indicator event/knowledge timestamp is at or before its manifest boundary and aligned to the corresponding UTC timeframe close.

## Scheduler cutover

- Closed-1m ingestion remains every minute.
- After reviewed publication and successful CI, update the existing v1 analysis job in place to the v2 name and minute 1 of every hour; do not create a duplicate job.
- v2 derives `analysis_boundary` as the latest complete UTC 1H close at or before the pinned dataset cutoff. A repository-backed unique claim makes publication atomic across callers. Later minute-level datasets within the same boundary return an auditable receipt containing the attempted dataset identity, strategy version, and boundary, and create no second artifact. An interrupted unpublished claim fails visibly rather than risking duplicate evidence.
- Daily summary remains at 10:00 Asia/Taipei and reports versioned denominators.

## Safety

Research-only; no exchange credentials, orders, PnL fabrication, automatic tuning, historical backfill, or rule promotion.

After the first controlled production v2 artifact and casebook are verified, deletion is limited to v1/legacy runtime run artifacts, old casebook output/rows, and old analysis logs/receipts. No runtime archive is created: old implementation and schema history remain in Git. The canonical closed-1m SQLite database, committed datasets, ingestion lineage, ingestion logs, and backups remain intact because they are the shared v2 SSOT.
