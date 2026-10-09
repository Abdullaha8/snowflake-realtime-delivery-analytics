"""Time helpers and the live-mode pacer.

The simulator always runs on *simulated* time (float epoch seconds, UTC). In live mode the
Pacer maps simulated time onto the wall clock (optionally accelerated); in backfill mode no
pacer is used and the simulator runs as fast as the CPU allows.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30), name="IST")


def iso(ts: float) -> str:
    """Epoch seconds -> '2026-10-10T07:32:11.482Z'."""
    return datetime.fromtimestamp(ts, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_ts(value: str) -> float:
    """ISO-8601 (with Z or offset; naive = IST) -> epoch seconds."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    return dt.timestamp()


def to_ist(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, IST)


def ist_hour(ts: float) -> float:
    """Fractional hour of day in IST, e.g. 19.5 for 19:30."""
    d = to_ist(ts)
    return d.hour + d.minute / 60 + d.second / 3600


def ist_midnight(ts: float) -> float:
    d = to_ist(ts)
    return d.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()


class Pacer:
    """Sleeps so that events leave at (sim time / speed) on the wall clock, capped at max_eps."""

    def __init__(
        self,
        sim_start: float,
        speed: float,
        max_eps: float | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.sim_start = sim_start
        self.speed = speed
        self.max_eps = max_eps
        self._clock = clock
        self._sleep = sleep
        self._wall_start = clock()
        self._next_slot = self._wall_start

    def wait_until(self, sim_ts: float) -> None:
        target = self._wall_start + (sim_ts - self.sim_start) / self.speed
        delay = target - self._clock()
        if delay > 0:
            self._sleep(delay)
        if self.max_eps:
            now = self._clock()
            if now < self._next_slot:
                self._sleep(self._next_slot - now)
                now = self._next_slot
            self._next_slot = max(self._next_slot, now) + 1 / self.max_eps
