from __future__ import annotations

import argparse
import json
from dataclasses import replace
from pathlib import Path

from .backup import backup_database
from .casebook import publish_casebook
from .config import load_config
from .data.binance import BinanceClient, FixtureBinanceClient
from .lock import LockUnavailable, ProcessLock
from .pipeline.orchestrator import Orchestrator


def main(argv=None):
    p = argparse.ArgumentParser(prog="smc-ict")
    sub = p.add_subparsers(dest="command", required=True)
    for command in ("run-once", "ingest-once", "analyze-once"):
        run = sub.add_parser(command)
        run.add_argument("--config", type=Path)
        run.add_argument("--fixture", action="store_true")
    back = sub.add_parser("backup")
    back.add_argument("--source", type=Path, required=True)
    back.add_argument("--target", type=Path, required=True)
    casebook = sub.add_parser("casebook")
    casebook.add_argument("--runs-root", type=Path, required=True)
    casebook.add_argument("--output", type=Path, required=True)
    casebook.add_argument("--milestone-target", type=int, default=20)
    args = p.parse_args(argv)
    if args.command == "backup":
        print(json.dumps(backup_database(args.source, args.target), sort_keys=True))
        return 0
    if args.command == "casebook":
        print(
            json.dumps(
                publish_casebook(args.runs_root, args.output, args.milestone_target),
                sort_keys=True,
            )
        )
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
    client = (
        None
        if args.command == "analyze-once"
        else FixtureBinanceClient()
        if args.fixture
        else BinanceClient(timeout=cfg.request_timeout)
    )
    try:
        with ProcessLock(cfg.data_root / "locks" / "run-once.lock"):
            orchestrator = Orchestrator(cfg, client)
            r = (
                orchestrator.ingest_once()
                if args.command == "ingest-once"
                else orchestrator.analyze_latest()
                if args.command == "analyze-once"
                else orchestrator.run_once()
            )
    except LockUnavailable:
        print(json.dumps({"status": "SKIPPED_LOCKED"}, sort_keys=True))
        return 0
    print(
        json.dumps(
            {
                "status": r.status,
                "dataset_version": r.dataset_version,
                "ingestion_run_id": r.ingestion_run_id,
                "analysis_run_id": r.analysis_run_id,
                "run_dir": str(r.run_dir) if r.run_dir else None,
                "error": r.error,
                "strategy_version": r.strategy_version,
                "analysis_boundary": r.analysis_boundary,
            },
            sort_keys=True,
        )
    )
    return 0 if r.status != "FAILED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
