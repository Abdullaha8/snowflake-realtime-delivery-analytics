import gzip
import json

import pytest

from simulator.clock import Pacer
from simulator.events import EVENT_SPECS, build_event, json_schemas
from simulator.report import iter_events, load_manifest, summarize
from simulator.sinks import JsonlSink, KafkaSink

from .conftest import simulate


def _event(ts: float = 1_760_000_000.0):
    ev = build_event("WEATHER_UPDATED", "e1", ts, {
        "city": "PUNE", "condition": "CLEAR", "intensity": None, "demand_multiplier": 1.0,
        "supply_multiplier": 1.0, "travel_time_multiplier": 1.0,
    })  # fmt: skip
    ev.value["emitted_ts"] = "2025-10-09T08:53:20.000Z"
    return ev


def test_jsonl_sink_partitions_by_topic_and_emitted_date(tmp_path):
    sink = JsonlSink(tmp_path, "r1", compress=True)
    sink.write(_event())
    sink.close()
    path = tmp_path / "fooddel.reference.v1" / "dt=2025-10-09" / "part-r1.jsonl.gz"
    with gzip.open(path, "rt") as f:
        assert json.loads(f.readline())["event_id"] == "e1"


class FakeProducer:
    def __init__(self, fail_first: bool = False, error: str | None = None):
        self.sent, self.fail_first, self.error = [], fail_first, error

    def produce(self, topic, key, value, on_delivery):
        if self.fail_first:
            self.fail_first = False
            raise BufferError
        self.sent.append((topic, key, value))
        on_delivery(self.error, None)

    def poll(self, _timeout):
        return 0

    def flush(self, _timeout):
        return 0


def test_kafka_sink_keys_and_retries():
    producer = FakeProducer(fail_first=True)
    sink = KafkaSink("unused", producer=producer)
    sink.write(_event())
    sink.close()
    topic, key, value = producer.sent[0]
    assert topic == "fooddel.reference.v1" and key == b"PUNE"
    assert json.loads(value)["event_type"] == "WEATHER_UPDATED"


def test_kafka_sink_raises_on_delivery_errors():
    sink = KafkaSink("unused", producer=FakeProducer(error="broker down"))
    sink.write(_event())
    with pytest.raises(RuntimeError, match="broker down"):
        sink.close()


def test_pacer_maps_sim_time_to_accelerated_wall_time():
    now = [0.0]
    slept = []

    def sleep(s):
        slept.append(s)
        now[0] += s

    pacer = Pacer(sim_start=1000.0, speed=60.0, clock=lambda: now[0], sleep=sleep)
    pacer.wait_until(1060.0)  # one simulated minute = one wall second at 60x
    assert now[0] == pytest.approx(1.0)


def test_pacer_caps_events_per_second():
    now = [0.0]

    def sleep(s):
        now[0] += s

    pacer = Pacer(0.0, speed=1e9, max_eps=10, clock=lambda: now[0], sleep=sleep)
    for _ in range(21):
        pacer.wait_until(0.0)
    assert now[0] == pytest.approx(2.0)


def test_json_schemas_cover_every_event_type():
    schemas = json_schemas()
    assert set(schemas) == set(EVENT_SPECS)
    placed = schemas["ORDER_PLACED"]
    assert placed["properties"]["event_type"] == {"const": "ORDER_PLACED"}
    assert "promised_eta_min" in placed["properties"]["data"]["properties"]


def test_report_on_jsonl_run(tmp_path):
    from simulator.config import SimConfig
    from simulator.engine import Simulator
    from simulator.sinks import ManifestWriter

    cfg = SimConfig(mode="backfill", start="2026-08-14T19:00", days=0.05, seed=3)
    sink = JsonlSink(tmp_path, "r1")
    manifest = ManifestWriter(tmp_path / "_manifest" / "r1.jsonl")
    stats = Simulator(cfg, sink, run_id="r1", manifest=manifest).run()

    events = sorted(iter_events(tmp_path), key=lambda e: e["emitted_ts"])
    s = summarize(events, load_manifest(tmp_path))
    assert s["events_total"] == stats.emitted
    assert s["events_total"] - s["events_unique"] == s["chaos_observed"]["duplicate_copies"]
    assert s["chaos_injected"] == dict(stats.anomalies)
    assert s["orders"]["placed"] == stats.orders_placed


def test_simulate_helper_smoke():
    run = simulate(days=0.01, seed=4)
    assert run.stats.emitted == len(run.events) > 0


def test_report_warns_when_a_directory_mixes_runs(tmp_path):
    from simulator.report import report_dir, run_ids

    for run in ("r1", "r2"):
        sink = JsonlSink(tmp_path, run)
        sink.write(_event())
        sink.close()
    assert run_ids(tmp_path) == ["r1", "r2"]
    assert report_dir(tmp_path).startswith("WARNING")


def test_cli_overwrite_leaves_a_single_run(tmp_path):
    from simulator.__main__ import main
    from simulator.report import run_ids

    args = ["run", "--mode", "backfill", "--start", "2026-08-14T19:00", "--days", "0.005"]
    args += ["--sink", "jsonl", "--out", str(tmp_path / "out")]
    main([*args, "--seed", "1"])
    main([*args, "--seed", "2", "--overwrite"])
    assert len(run_ids(tmp_path / "out")) == 1
