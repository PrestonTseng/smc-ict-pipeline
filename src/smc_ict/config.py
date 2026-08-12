"""Central immutable configuration."""

from __future__ import annotations

import hashlib
import json
import tomllib
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    swing_length: int = 2
    poi_timeout_bars: int = 12
    displacement_window: int = 3
    displacement_lookback: int = 20
    displacement_body_multiple: Decimal = Decimal("1.25")
    displacement_body_ratio: Decimal = Decimal("0.70")
    displacement_close_edge: Decimal = Decimal("0.20")
    fvg_wait_bars: int = 6
    fvg_entry_fraction: Decimal = Decimal("0.50")
    atr_period: int = 14
    atr_buffer: Decimal = Decimal("0.10")
    minimum_r: Decimal = Decimal("2.0")
    fee_bps: Decimal = Decimal("4")
    slippage_bps: Decimal = Decimal("2")
    time_stop_hours: int = 6

    def __post_init__(self):
        if (
            min(
                self.swing_length,
                self.poi_timeout_bars,
                self.displacement_window,
                self.displacement_lookback,
                self.fvg_wait_bars,
                self.atr_period,
                self.time_stop_hours,
            )
            <= 0
        ):
            raise ValueError("bar windows must be positive")
        if self.minimum_r < Decimal("1"):
            raise ValueError("minimum_r must be >= 1")


@dataclass(frozen=True, slots=True)
class AppConfig:
    data_root: Path = Path("var")
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")
    bootstrap_bars: int = 43200
    overlap_bars: int = 5
    request_timeout: int = 20
    strategy: StrategyConfig = StrategyConfig()


def _wire(obj):
    if hasattr(obj, "__dataclass_fields__"):
        return {k: _wire(v) for k, v in asdict(obj).items()}
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, tuple):
        return [_wire(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _wire(v) for k, v in obj.items()}
    return obj


def config_hash(config) -> str:
    raw = json.dumps(_wire(config), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()


def config_dict(config) -> dict:
    return _wire(config)


def load_config(path: Path | None) -> AppConfig:
    if path is None:
        return AppConfig()
    raw = tomllib.loads(Path(path).read_text())
    app = raw.get("app", {})
    paths = raw.get("paths", {})
    strategy = raw.get("strategy", {})
    decimal_fields = {
        "displacement_body_multiple",
        "displacement_body_ratio",
        "displacement_close_edge",
        "fvg_entry_fraction",
        "atr_buffer",
        "minimum_r",
        "fee_bps",
        "slippage_bps",
    }
    normalized = {k: (Decimal(str(v)) if k in decimal_fields else v) for k, v in strategy.items()}
    return AppConfig(
        data_root=Path(paths.get("data_root", "var")),
        symbols=tuple(app.get("symbols", ["BTCUSDT", "ETHUSDT"])),
        bootstrap_bars=int(app.get("bootstrap_bars", 1500)),
        overlap_bars=int(app.get("overlap_bars", 5)),
        request_timeout=int(app.get("request_timeout", 20)),
        strategy=StrategyConfig(**normalized),
    )
