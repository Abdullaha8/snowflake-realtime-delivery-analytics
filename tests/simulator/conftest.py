from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

import pytest

from simulator.chaos import ChaosConfig
from simulator.config import SimConfig
from simulator.engine import RunStats, Simulator
from simulator.events import TOPIC_ORDERS, OutEvent
from simulator.sinks import ListSink, ManifestWriter

# Friday evening in the monsoon: dinner peak, likely rain.
TEST_START = "2026-08-14T17:00"


@dataclass
class Run:
    events: list[OutEvent]
    manifest: list[dict]
    stats: RunStats

    @property
    def values(self) -> list[dict]:
        return [e.value for e in self.events]

    def orders(self) -> dict[str, list[dict]]:
        """order_id -> its events in emitted order."""
        out: dict[str, list[dict]] = defaultdict(list)
        for e in self.events:
            if e.topic == TOPIC_ORDERS:
                out[e.key].append(e.value)
        return out


def simulate(**overrides) -> Run:
    params = {
        "mode": "backfill",
        "start": TEST_START,
        "days": 0.25,
        "seed": 1,
        "emit_pings": True,
        **overrides,
    }
    sink, manifest = ListSink(), ManifestWriter(None)
    stats = Simulator(SimConfig(**params), sink, run_id="test", manifest=manifest).run()
    return Run(sink.events, manifest.records, stats)


@pytest.fixture(scope="session")
def clean_run() -> Run:
    return simulate(chaos=ChaosConfig.disabled())


@pytest.fixture(scope="session")
def chaos_run() -> Run:
    return simulate(seed=2)
