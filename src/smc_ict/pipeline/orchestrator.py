"""Single ingest→commit→analyze→publish orchestration."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path

from ..casebook import build_casebook
from ..config import AppConfig, config_dict, config_hash
from ..storage import MarketRepository
from .v2_analysis import analyze_symbol_v2
from .v2_state_machine import V2_STRATEGY_VERSION


def _source_version() -> str:
    package_root = Path(__file__).parents[1]
    digest = hashlib.sha256()
    for path in sorted(package_root.rglob("*.py")):
        digest.update(str(path.relative_to(package_root)).encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _bars_hash(bars) -> str:
    payload = json.dumps([bar.to_dict() for bar in bars], sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def aggregate_status(results: dict) -> str:
    priority = ("FAILED", "BLOCKED", "TRADE", "ORDER_PENDING", "ARMED", "NO_SETUP")
    statuses = {result["decision"]["status"] for result in results.values()}
    return next(status for status in priority if status in statuses)


@dataclass(frozen=True)
class RunResult:
    status: str
    dataset_version: str | None = None
    ingestion_run_id: str | None = None
    analysis_run_id: str | None = None
    run_dir: Path | None = None
    error: str | None = None


class Orchestrator:
    def __init__(self, config: AppConfig, client, analyzer=analyze_symbol_v2):
        self.config = config
        self.client = client
        self.analyzer = analyzer

    def run_once(self):
        ingested = self.ingest_once()
        if ingested.status == "FAILED":
            return ingested
        return self.analyze_latest()

    def ingest_once(self):
        repo = MarketRepository(self.config.data_root / "data" / "market.sqlite3")
        ingestion_run_id = "ingest-" + uuid.uuid4().hex
        repo.start_ingestion(ingestion_run_id)
        try:
            cutoff = self.client.latest_closed_cutoff()
            repo.set_ingestion_cutoff(ingestion_run_id, cutoff)
            latest = repo.latest_committed()
            if latest and cutoff < latest[1]:
                raise ValueError("Binance cutoff regressed below latest committed cutoff")
            base_version = latest[0] if latest else None
            if latest and cutoff == latest[1]:
                version = latest[0]
                repo.complete_ingestion(ingestion_run_id)
            else:
                start = (
                    latest[1] + 1 - self.config.overlap_bars * 60_000
                    if latest
                    else cutoff + 1 - self.config.bootstrap_bars * 60_000
                )
                batch = {s: self.client.fetch_1m(s, start, cutoff) for s in self.config.symbols}
                version = repo.commit_dataset(
                    batch,
                    cutoff,
                    base_version=base_version,
                    ingestion_run_id=ingestion_run_id,
                )  # hard boundary: no analysis before this returns
        except Exception as e:
            repo.fail_ingestion(ingestion_run_id, f"{type(e).__name__}: {e}")
            return RunResult(
                "FAILED", ingestion_run_id=ingestion_run_id, error=f"{type(e).__name__}: {e}"
            )

        return RunResult("COMMITTED", version, ingestion_run_id=ingestion_run_id)

    def analyze_latest(self):
        latest: tuple[str, int] | None = None
        ingestion_run_id: str | None = None
        try:
            repo = MarketRepository(self.config.data_root / "data" / "market.sqlite3")
            latest = repo.latest_committed()
            if latest is None:
                raise ValueError("no committed dataset")
            version, cutoff = latest
            analysis_boundary = ((cutoff + 1) // 3_600_000) * 3_600_000 - 1
            existing = self._existing_boundary_run(analysis_boundary)
            if existing is not None:
                run_dir, row = existing
                return RunResult(
                    "SKIPPED_ALREADY_ANALYZED",
                    row["dataset_version"],
                    row["ingestion_run_id"],
                    row["analysis_run_id"],
                    run_dir,
                )
            ingestion_run_id = repo.committed_ingestion_for_dataset(version)
            if ingestion_run_id is None:
                raise ValueError("no committed ingestion for latest dataset cutoff")
            snap = repo.snapshot(version)
            results = {s: self.analyzer(snap, s, self.config.strategy) for s in self.config.symbols}
            config_digest = config_hash(self.config)
            for symbol, result in results.items():
                input_digest = _bars_hash(snap.bars(symbol))
                for indicator in result["indicators"].values():
                    indicator["input_hash"] = input_digest
                    indicator["config_hash"] = config_digest
            return self._publish(
                ingestion_run_id,
                version,
                cutoff,
                analysis_boundary,
                results,
                repo.dataset_checksum(version),
                config_digest,
            )
        except Exception as e:
            return RunResult(
                "FAILED",
                dataset_version=latest[0] if latest else None,
                ingestion_run_id=ingestion_run_id,
                error=f"{type(e).__name__}: {e}",
            )

    def _existing_boundary_run(self, analysis_boundary: int) -> tuple[Path, dict] | None:
        runs_root = self.config.data_root / "runs"
        if not runs_root.exists():
            return None
        casebook = build_casebook(runs_root)
        rows = [
            row
            for row in casebook["cases"]
            if row["strategy_version"] == V2_STRATEGY_VERSION
            and row["analysis_boundary"] == analysis_boundary
        ]
        run_ids = {row["analysis_run_id"] for row in rows}
        if len(run_ids) > 1:
            raise ValueError("duplicate v2 analysis boundary")
        if not rows:
            return None
        row = rows[0]
        day = (
            __import__("datetime")
            .datetime.fromtimestamp(row["cutoff"] / 1000, __import__("datetime").timezone.utc)
            .date()
            .isoformat()
        )
        return runs_root / day / row["analysis_run_id"], row

    def _publish(
        self,
        ingestion_run_id,
        version,
        cutoff,
        analysis_boundary,
        results,
        dataset_checksum,
        config_digest,
    ):
        run_id = "run-" + uuid.uuid4().hex
        day = (
            "1970-01-01"
            if cutoff < 86_400_000
            else __import__("datetime")
            .datetime.fromtimestamp(cutoff / 1000, __import__("datetime").timezone.utc)
            .date()
            .isoformat()
        )
        base = self.config.data_root / "runs" / day
        tmp = self.config.data_root / "runs" / ".tmp" / run_id
        final = base / run_id
        tmp.mkdir(parents=True)
        (tmp / "indicators").mkdir()
        run_status = aggregate_status(results)
        decision = {"status": run_status, "symbols": results}
        cfg = config_dict(self.config)
        cfg["_config_hash"] = config_digest
        (tmp / "config-snapshot.json").write_text(json.dumps(cfg, sort_keys=True, indent=2) + "\n")
        (tmp / "decision.json").write_text(json.dumps(decision, sort_keys=True, indent=2) + "\n")
        manifest = {
            "schema_version": "2",
            "strategy_version": V2_STRATEGY_VERSION,
            "analysis_boundary": analysis_boundary,
            "ingestion_run_id": ingestion_run_id,
            "analysis_run_id": run_id,
            "dataset_version": version,
            "dataset_checksum": dataset_checksum,
            "cutoff": cutoff,
            "config_hash": config_digest,
            "code_version": _source_version(),
            "files": {},
        }
        for name in ("config-snapshot.json", "decision.json"):
            manifest["files"][name] = hashlib.sha256((tmp / name).read_bytes()).hexdigest()
        (tmp / "manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
        base.mkdir(parents=True, exist_ok=True)
        os.rename(tmp, final)
        return RunResult(run_status, version, ingestion_run_id, run_id, final)
