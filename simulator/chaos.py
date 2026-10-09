"""Injects duplicate, late and out-of-order delivery into an otherwise correct event stream.

The engine produces events in perfect event-time order. ChaosStage decides *when* each event
is emitted (its emit time) and whether a duplicate copy is sent. Events are released in
emit-time order, so downstream sees the disorder. Every anomaly is reported to `on_anomaly`
(ground-truth manifest); nothing in the event itself says it was tampered with.
"""

from __future__ import annotations

import copy
import heapq
import itertools
import random
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace

from simulator.clock import iso
from simulator.events import OutEvent

NETWORK_LATENCY_S = (0.05, 0.5)


@dataclass(frozen=True)
class ChaosConfig:
    duplicate_rate: float = 0.02  # all events: producer retry re-sends the same event_id
    duplicate_max_delay_s: float = 5.0
    late_rate: float = 0.03  # late-eligible sources only: phone offline, buffered
    late_min_s: float = 60.0
    late_max_s: float = 1800.0
    very_late_rate: float = 0.002  # late-eligible sources only
    very_late_min_s: float = 7200.0
    very_late_max_s: float = 21600.0
    out_of_order_rate: float = 0.05  # all events: held back until after the next event per key
    out_of_order_max_hold_s: float = 600.0
    late_sources: tuple[str, ...] = ("driver_app",)

    @classmethod
    def disabled(cls) -> ChaosConfig:
        return cls(duplicate_rate=0.0, late_rate=0.0, very_late_rate=0.0, out_of_order_rate=0.0)


AnomalySink = Callable[[dict], None]


class ChaosStage:
    def __init__(
        self, cfg: ChaosConfig, rng: random.Random, on_anomaly: AnomalySink | None = None
    ) -> None:
        self.cfg = cfg
        self.rng = rng
        self.on_anomaly = on_anomaly or (lambda _: None)
        self._seq = itertools.count()
        # (emit_ts, seq, kind, payload); kind "emit" -> (OutEvent, anomaly | None)
        #                                 kind "deadline" -> (hold_key, token)
        self._heap: list[tuple[float, int, str, tuple]] = []
        self._held: dict[tuple[str, str], tuple[int, OutEvent, bool]] = {}

    # -- input ---------------------------------------------------------------------------
    def submit(self, ev: OutEvent) -> None:
        cfg, rng = self.cfg, self.rng
        dup = rng.random() < cfg.duplicate_rate
        category = None
        if ev.value["source"] in cfg.late_sources:
            r = rng.random()
            if r < cfg.very_late_rate:
                category = "VERY_LATE"
            elif r < cfg.very_late_rate + cfg.late_rate:
                category = "LATE"
        if category is None and rng.random() < cfg.out_of_order_rate:
            category = "OUT_OF_ORDER"

        base_emit = ev.event_ts + rng.uniform(*NETWORK_LATENCY_S)
        hold_key = (ev.topic, ev.key)
        held = self._held.pop(hold_key, None)
        if held is not None and category == "OUT_OF_ORDER":
            category = None  # one swap per pair is enough

        if category == "VERY_LATE":
            emit = ev.event_ts + rng.uniform(cfg.very_late_min_s, cfg.very_late_max_s)
            self._schedule(ev, emit, {"anomaly": "VERY_LATE"}, dup)
        elif category == "LATE":
            emit = ev.event_ts + rng.uniform(cfg.late_min_s, cfg.late_max_s)
            self._schedule(ev, emit, {"anomaly": "LATE"}, dup)
        elif category == "OUT_OF_ORDER":
            token = next(self._seq)
            self._held[hold_key] = (token, ev, dup)
            deadline = ev.event_ts + cfg.out_of_order_max_hold_s
            heapq.heappush(self._heap, (deadline, next(self._seq), "deadline", (hold_key, token)))
        else:
            self._schedule(ev, base_emit, None, dup)

        if held is not None:
            # The held predecessor goes out just after this (newer) event.
            _, held_ev, held_dup = held
            swapped = category is None
            info = {
                "anomaly": "OUT_OF_ORDER",
                "swapped_with": ev.value["event_id"] if swapped else None,
            }
            self._schedule(held_ev, base_emit + rng.uniform(0.01, 1.0), info, held_dup)

    def _schedule(self, ev: OutEvent, emit: float, anomaly: dict | None, dup: bool) -> None:
        heapq.heappush(self._heap, (emit, next(self._seq), "emit", (ev, anomaly)))
        if dup:
            copy_emit = emit + self.rng.uniform(0.0, self.cfg.duplicate_max_delay_s)
            info = {"anomaly": "DUPLICATE", "duplicate_of": ev.value["event_id"]}
            heapq.heappush(self._heap, (copy_emit, next(self._seq), "emit", (ev, info)))

    # -- output --------------------------------------------------------------------------
    def drain(self, until: float) -> Iterator[tuple[float, OutEvent]]:
        """Release everything due at or before `until`, in emit-time order."""
        while self._heap and self._heap[0][0] <= until:
            yield from self._pop(None)

    def flush(self, end: float) -> Iterator[tuple[float, OutEvent]]:
        """End of run: release held and future events, stamped no later than `end`."""
        for hold_key in list(self._held):
            token = self._held[hold_key][0]
            heapq.heappush(self._heap, (end, next(self._seq), "deadline", (hold_key, token)))
        while self._heap:
            yield from self._pop(end)

    def _pop(self, cap: float | None) -> Iterator[tuple[float, OutEvent]]:
        emit, _, kind, payload = heapq.heappop(self._heap)
        if kind == "deadline":
            hold_key, token = payload
            held = self._held.get(hold_key)
            if held is not None and held[0] == token:
                del self._held[hold_key]
                _, ev, dup = held
                info = {"anomaly": "OUT_OF_ORDER", "swapped_with": None}
                self._schedule(ev, emit, info, dup)
            return
        ev, anomaly = payload
        flushed = cap is not None and emit > cap
        if flushed:
            emit = cap
        value = copy.copy(ev.value)
        value["emitted_ts"] = iso(emit)
        out = replace(ev, value=value)
        if anomaly is not None:
            self.on_anomaly(
                {
                    "event_id": value["event_id"],
                    "event_type": value["event_type"],
                    "topic": ev.topic,
                    "key": ev.key,
                    **anomaly,
                    "event_ts": value["event_ts"],
                    "emitted_ts": value["emitted_ts"],
                    "delay_s": round(emit - ev.event_ts, 3),
                    "flushed_at_shutdown": flushed,
                }
            )
        yield emit, out
