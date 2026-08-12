from __future__ import annotations

import argparse
import json
from dataclasses import replace
from pathlib import Path

from .backup import backup_database
from .config import load_config
from .data.binance import BinanceClient, FixtureBinanceClient
from .lock import LockUnavailable, ProcessLock
from .pipeline.orchestrator import Orchestrator


def main(argv=None):
    p = argparse.ArgumentParser(prog="smc-ict")
    sub = p.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run-once")
    run.add_argument("--config", type=Path)
    run.add_argument("--fixture", action="store_true")
    back = sub.add_parser("backup")
    back.add_argument("--source", type=Path, required=True)
    back.add_argument("--target", type=Path, required=True)
    args = p.parse_args(argv)
    if args.command == "backup":
        print(json.dumps(backup_database(args.source, args.target), sort_keys=True))
        return 0
    cfg = load_config(args.config)
    if args.fixture:
        fixture_root = (
            cfg.data_root.with_name(f"{cfg.data_root.name}-fixture")
            if cfg.data_root.name
            else cfg.data_root / "var-fixture"
        )
        cfg = replace(
            cfg,
            data_root=fixture_root,
        )
    client = FixtureBinanceClient() if args.fixture else BinanceClient(timeout=cfg.request_timeout)
    try:
        with ProcessLock(cfg.data_root / "locks" / "run-once.lock"):
            r = Orchestrator(cfg, client).run_once()
    except LockUnavailable:
        print(json.dumps({"status": "SKIPPED_LOCKED"}, sort_keys=True))
        return 0
    print(
        json.dumps(
            {
                "status": r.status,
                "dataset_version": r.dataset_version,
                "analysis_run_id": r.analysis_run_id,
                "run_dir": str(r.run_dir) if r.run_dir else None,
                "error": r.error,
            },
            sort_keys=True,
        )
    )
    return 0 if r.status != "FAILED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
