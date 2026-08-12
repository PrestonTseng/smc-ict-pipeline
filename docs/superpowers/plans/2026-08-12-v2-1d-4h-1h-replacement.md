# v2 1D→4H→1H Replacement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace future v1 analysis with a strategy-versioned, point-in-time v2 `1D→4H→1H` decision tree while preserving canonical closed-1m ingestion and immutable v1 history.

**Architecture:** Add a v2-specific analyzer and gate contract while reusing pure indicator primitives. Extend artifact/casebook version identity without rewriting prior files. Publish only after live verification, then atomically replace the five-minute v1 analysis scheduler with an hourly v2 scheduler.

**Tech Stack:** Python 3.13, uv, pytest, Ruff, SQLite, Bash, Hermes scheduler.

## Global Constraints

- Binance USDⓈ-M closed 1m remains the only canonical market-data grain.
- Ingestion remains every minute with overlap, pagination, continuity validation, and BTC/ETH atomic publication.
- Only complete UTC-aligned 1D/4H/1H bars may be used.
- v1 immutable artifacts remain byte-unchanged and retain their denominator.
- v2 is long-only, research-only, deterministic, fail-closed, and creates no order or PnL.
- No scheduler cutover before exact review, push, and CI success.

---

### Task 1: Freeze the v2 gate and aggregation contract

**Files:**
- Create: `src/smc_ict/pipeline/v2_analysis.py`
- Create: `src/smc_ict/pipeline/v2_state_machine.py`
- Test: `tests/test_v2_analysis.py`
- Test: `tests/test_v2_state_machine.py`

**Interfaces:**
- Consumes: `snapshot.bars(symbol)`, `StrategyConfig`, existing indicator primitives.
- Produces: `V2_STRATEGY_VERSION`, `V2_GATES`, `analyze_symbol_v2(snapshot, symbol, strategy) -> dict`.

- [ ] Write parametrized RED tests proving exact gate order, complete local 1440/240/60 aggregation, closed-bar behavior, daily/4H alignment, long-only rejection, frozen references, event ordering, and fail-fast downstream `UNAVAILABLE` states.
- [ ] Run `uv run pytest tests/test_v2_state_machine.py tests/test_v2_analysis.py -q` and verify failures are missing v2 modules/symbols.
- [ ] Implement the minimum v2 state machine and analyzer using confirmed swings and existing pure indicator functions; 1H execution must begin only after the 4H POI touch is known at the 4H close.
- [ ] Rerun focused tests and `uv run pytest -q`.

### Task 2: Version immutable artifacts and casebook cohorts

**Files:**
- Modify: `src/smc_ict/pipeline/orchestrator.py`
- Modify: `src/smc_ict/casebook.py`
- Modify: `src/smc_ict/cli.py`
- Modify: `scripts/daily-summary.py`
- Test: `tests/test_manifest_contract.py`
- Test: `tests/test_casebook.py`
- Test: `tests/test_cli_analysis.py`

**Interfaces:**
- Consumes: analyzer strategy identity.
- Produces: manifests and rows with explicit `strategy_version`; summaries with `strategy_version_counts` and independent unique cutoffs.

- [ ] Write RED tests proving v2 manifests declare `v2-1d-4h-1h`, legacy strict schema-v1 runs map only to `v1-4h-1h-5m`, unknown versions fail closed, and version denominators do not pool.
- [ ] Run focused tests and record expected schema failures.
- [ ] Implement explicit version envelopes and backward-compatible strict reading without modifying old artifacts.
- [ ] Rerun focused and full suites.

### Task 3: Enforce one v2 observation per closed 1H boundary

**Files:**
- Modify: `src/smc_ict/storage.py`
- Modify: `src/smc_ict/pipeline/orchestrator.py`
- Modify: `src/smc_ict/cli.py`
- Modify: `scripts/scheduled-analysis.sh`
- Test: `tests/test_scheduled_scripts.py`
- Test: `tests/test_pipeline.py`

**Interfaces:**
- Produces: an analysis receipt with exact status, strategy version, dataset version, and 1H boundary; duplicate boundary creates no artifact.

- [ ] Write RED tests for flooring minute-level dataset cutoffs to the latest complete UTC 1H boundary, same-boundary duplicate execution, later-boundary execution, lock retry, invalid receipt, and UTC boundary behavior.
- [ ] Implement repository-backed v2 boundary claim/publication semantics and a strict wrapper.
- [ ] Run Bash syntax, focused tests, and full suite.

### Task 4: Verify, publish, and cut over atomically

**Files:**
- Modify: `README.md`
- Modify: scheduler thin wrappers under `/opt/data/scripts/` only after CI success.

**Interfaces:**
- Consumes: published reviewed SHA.
- Produces: unchanged 1m ingestion cadence, the existing analysis job updated in place to v2 hourly cadence, and a version-aware daily receipt.

- [ ] Run `uv lock --check`, `uv sync --locked`, Ruff format/check, full pytest, `uv build`, Bash syntax, diff/runtime/secret scans.
- [ ] Exercise v2 against a copy of the live SQLite dataset; verify pinned lineage, no market fetch, immutable artifact, and no mutation of v1 evidence.
- [ ] Stage exact files, compute digest, obtain adversarial review with empty security/logic arrays.
- [ ] Commit, push, wait for CI success, and prove local/remote SHA equality.
- [ ] Pause/remove only job `c5b5c84e6e18`, update SHA-pinned wrappers, create the hourly v2 job, controlled-run it, and verify no new v1 decision appears after cutover.
- [ ] Rebuild daily casebook, verify versioned denominators, and update canonical plan/status.
