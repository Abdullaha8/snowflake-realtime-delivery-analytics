"""Late / duplicate / out-of-order injection: unit tests on ChaosStage + rates in a real run."""

import random
from collections import Counter
from dataclasses import replace

import pytest

from simulator.chaos import ChaosConfig, ChaosStage
from simulator.clock import parse_ts
from simulator.events import build_event

ZERO = ChaosConfig.disabled()


def _ping(i: int, ts: float, driver: str = "drv_0001") -> object:
    data = {
        "driver_id": driver, "lat": 18.5, "lng": 73.8, "h3_r8": "883c8a4d27fffff",
        "zone_id": None, "status": "IDLE", "order_id": None, "speed_kmh": 0.0, "battery_pct": 80,
    }  # fmt: skip
    return build_event("DRIVER_LOCATION", f"ev{i}", ts, data)


def _accepted(i: int, ts: float, order: str) -> object:
    data = {"order_id": order, "restaurant_id": "res_0001", "estimated_prep_min": 15}
    return build_event("ORDER_ACCEPTED", f"ev{i}", ts, data)


def _run(cfg: ChaosConfig, events, end: float = 1e9):
    manifest: list[dict] = []
    stage = ChaosStage(cfg, random.Random(0), manifest.append)
    out = []
    for ev in events:
        out.extend(stage.drain(ev.event_ts))
        stage.submit(ev)
    out.extend(stage.drain(end))
    out.extend(stage.flush(end))
    return out, manifest


def test_no_chaos_preserves_order():
    evs = [_ping(i, 1000.0 + 30 * i) for i in range(50)]
    out, manifest = _run(ZERO, evs)
    assert [e.value["event_id"] for _, e in out] == [f"ev{i}" for i in range(50)]
    assert manifest == []
    for emit, e in out:
        assert 0.05 <= emit - e.event_ts <= 0.5
        assert e.value["emitted_ts"] is not None


def test_duplicates_share_event_id_and_follow_quickly():
    out, manifest = _run(replace(ZERO, duplicate_rate=1.0), [_ping(0, 1000.0)])
    assert len(out) == 2
    (t1, a), (t2, b) = out
    assert a.value["event_id"] == b.value["event_id"]
    assert 0 <= t2 - t1 <= 5.0
    assert manifest[0]["anomaly"] == "DUPLICATE"


def test_late_applies_only_to_driver_app():
    cfg = replace(ZERO, late_rate=1.0)
    out, manifest = _run(cfg, [_ping(0, 1000.0), _accepted(1, 1000.0, "ord_1")])
    by_id = {e.value["event_id"]: emit for emit, e in out}
    assert 60 <= by_id["ev0"] - 1000.0 <= 1800  # driver_app -> late
    assert by_id["ev1"] - 1000.0 < 1  # restaurant_app -> on time
    assert [m["anomaly"] for m in manifest] == ["LATE"]
    # the late event is emitted after the on-time one: an event-time inversion
    assert [e.value["event_id"] for _, e in out] == ["ev1", "ev0"]


def test_out_of_order_swaps_with_successor_on_same_key():
    cfg = replace(ZERO, out_of_order_rate=1.0)
    evs = [_accepted(0, 1000.0, "ord_1"), _accepted(1, 1010.0, "ord_1")]
    out, manifest = _run(cfg, evs)
    assert [e.value["event_id"] for _, e in out] == ["ev1", "ev0"]
    assert manifest[0]["anomaly"] == "OUT_OF_ORDER" and manifest[0]["swapped_with"] == "ev1"


def test_out_of_order_hold_times_out_without_successor():
    cfg = replace(ZERO, out_of_order_rate=1.0)
    out, manifest = _run(cfg, [_accepted(0, 1000.0, "ord_1")])
    assert out[0][0] == pytest.approx(1000.0 + cfg.out_of_order_max_hold_s)
    assert manifest[0]["swapped_with"] is None


def test_flush_caps_emit_time_at_end_of_run():
    cfg = replace(ZERO, very_late_rate=1.0)
    out, manifest = _run(cfg, [_ping(0, 1000.0)], end=1100.0)
    assert out[0][0] == 1100.0
    assert manifest[0]["flushed_at_shutdown"] is True


# -- rates in a real 6-hour run ------------------------------------------------------------
def test_stream_is_emitted_in_emit_time_order(chaos_run):
    emitted = [parse_ts(v["emitted_ts"]) for v in chaos_run.values]
    assert emitted == sorted(emitted)


def test_observed_rates_match_config(chaos_run):
    cfg = ChaosConfig()
    values = chaos_run.values
    unique = {v["event_id"] for v in values}
    copies = len(values) - len(unique)
    assert copies / len(unique) == pytest.approx(cfg.duplicate_rate, abs=0.004)

    counts = Counter(m["anomaly"] for m in chaos_run.manifest)
    assert counts["DUPLICATE"] == copies
    driver_app = sum(1 for v in values if v["source"] == "driver_app")
    assert counts["LATE"] / driver_app == pytest.approx(cfg.late_rate, abs=0.005)
    assert counts["VERY_LATE"] / driver_app == pytest.approx(cfg.very_late_rate, abs=0.001)
    assert counts["OUT_OF_ORDER"] / len(unique) == pytest.approx(cfg.out_of_order_rate, abs=0.01)


def test_manifest_matches_what_downstream_sees(chaos_run):
    by_id: dict[str, list[dict]] = {}
    for v in chaos_run.values:
        by_id.setdefault(v["event_id"], []).append(v)
    for m in chaos_run.manifest:
        copies = by_id[m["event_id"]]
        if m["anomaly"] == "DUPLICATE":
            assert len(copies) >= 2
        elif m["anomaly"] == "LATE" and not m["flushed_at_shutdown"]:
            assert 60 <= m["delay_s"] <= 1800
        elif m["anomaly"] == "VERY_LATE" and not m["flushed_at_shutdown"]:
            assert 7200 <= m["delay_s"] <= 21600


def test_swapped_events_really_arrive_after_their_successor(chaos_run):
    position = {}
    for i, v in enumerate(chaos_run.values):
        position.setdefault(v["event_id"], i)
    swapped = [
        m for m in chaos_run.manifest if m["anomaly"] == "OUT_OF_ORDER" and m["swapped_with"]
    ]
    assert len(swapped) > 100
    for m in swapped:
        assert position[m["swapped_with"]] < position[m["event_id"]]
