"""Simulator configuration. Defaults are the "Pune, ~3 months after launch" scenario."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal

from simulator.chaos import ChaosConfig
from simulator.clock import ist_midnight, parse_ts


@dataclass
class SimConfig:
    mode: Literal["live", "backfill"] = "live"
    start: str | None = None  # ISO-8601; live default = now, backfill default = launch date
    duration_s: float = 600.0  # live: wall-clock seconds before auto-stop
    speed: float = 1.0  # live: simulated seconds per wall second
    days: float | None = None  # backfill: simulated days (default = launch -> today 00:00 IST)
    max_eps: float | None = 500.0  # live: hard cap on events per second
    seed: int | None = None  # run seed (traffic randomness); None = random
    world_seed: int = 42  # entity seed: same restaurants/customers/drivers every run
    launch_date: str = "2026-07-01"  # the platform "launched" in Pune at 00:00 IST
    n_restaurants: int = 300
    n_customers: int = 20_000
    n_drivers: int = 400
    base_orders_per_min: float = 3.7  # city-wide, at hour_curve == 1 and growth == 1
    reference_active_customers: int = 5_000  # active customers at which growth == 1
    max_growth: float = 1.5
    no_driver_timeout_s: float = 600.0
    dispatch_retry_s: float = 30.0
    max_search_ring_k: int = 8  # ~4 km at H3 res 8
    driver_attendance: float = 0.88
    ping_interval_s: float = 30.0
    emit_pings: bool = True
    force_rain: bool = False
    chaos: ChaosConfig = field(default_factory=ChaosConfig)

    @property
    def launch_ts(self) -> float:
        return parse_ts(f"{self.launch_date}T00:00:00+05:30")

    def window(self, now: float | None = None) -> tuple[float, float]:
        """Simulated [start, end) in epoch seconds."""
        now = time.time() if now is None else now
        if self.mode == "live":
            start = parse_ts(self.start) if self.start else now
            return start, start + self.duration_s * self.speed
        start = parse_ts(self.start) if self.start else self.launch_ts
        if self.days is not None:
            return start, start + self.days * 86400
        return start, max(start + 86400, ist_midnight(now))
