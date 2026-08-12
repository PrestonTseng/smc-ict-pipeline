"""Single ingest→commit→analyze→publish orchestration."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path

from ..config import AppConfig, config_dict, config_hash
from ..storage import MarketRepository
from .analysis import analyze_symbol


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
    analysis_run_id: str | None = None
    run_dir: Path | None = None
    error: str | None = None


class Orchestrator:
    def __init__(self, config: AppConfig, client, analyzer=analyze_symbol):
        self.config = config
        self.client = client
        self.analyzer = analyzer

    def run_once(self):
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
            return RunResult("FAILED", error=f"{type(e).__name__}: {e}")

        try:
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
                results,
                repo.dataset_checksum(version),
                config_digest,
            )
        except Exception as e:
            return RunResult("FAILED", error=f"{type(e).__name__}: {e}")

    def _publish(self, ingestion_run_id, version, cutoff, results, dataset_checksum, config_digest):
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
            "schema_version": "1",
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
        return RunResult(run_status, version, run_id, final)
