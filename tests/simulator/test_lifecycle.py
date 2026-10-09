"""Correctness of the clean (chaos-free) stream: what the pipeline should reconstruct."""

import json

import h3

from simulator.clock import parse_ts
from simulator.events import validate_event
from simulator.zones import ZoneMap

TERMINAL = {"ORDER_DELIVERED", "ORDER_CANCELLED"}


def test_every_event_matches_its_schema(clean_run):
    for v in clean_run.values:
        validate_event(v)
        json.dumps(v)  # serialisable


def test_clean_stream_is_in_event_time_order(clean_run):
    """Without chaos, only network jitter (<= 0.5 s) separates emit order from event order,
    and events sharing a Kafka key are always in order."""
    newest = float("-inf")
    last_by_key: dict[tuple[str, str], float] = {}
    for e in clean_run.events:
        assert e.event_ts > newest - 0.5
        newest = max(newest, e.event_ts)
        k = (e.topic, e.key)
        assert e.event_ts >= last_by_key.get(k, float("-inf")), k
        last_by_key[k] = e.event_ts
        # >= 0 rather than >= 0.05: events in the last 0.5 s are flushed at end-of-run
        assert 0 <= parse_ts(e.value["emitted_ts"]) - e.event_ts <= 0.501


def test_order_lifecycles_are_valid(clean_run):
    orders = clean_run.orders()
    assert len(orders) > 500
    outcomes = {"ORDER_DELIVERED": 0, "ORDER_CANCELLED": 0}
    for oid, evs in orders.items():
        types = [e["event_type"] for e in evs]
        assert types[0] == "ORDER_PLACED", oid
        assert len(types) == len(set(types)), f"repeated event in {oid}: {types}"
        seen: set[str] = set()
        for t in types:
            assert not (seen & TERMINAL), f"event after terminal in {oid}: {types}"
            if t in ("DRIVER_ASSIGNED", "FOOD_READY"):
                assert "ORDER_ACCEPTED" in seen, types
            if t == "ORDER_PICKED_UP":
                assert {"DRIVER_ASSIGNED", "FOOD_READY"} <= seen, types
            if t == "ORDER_DELIVERED":
                assert "ORDER_PICKED_UP" in seen, types
            if t == "ORDER_CANCELLED":
                assert "ORDER_PICKED_UP" not in seen, types
            seen.add(t)
        for t in seen & TERMINAL:
            outcomes[t] += 1
    # most orders finish inside a 6h window, and most of those are delivered
    finished = sum(outcomes.values())
    assert finished > 0.9 * len(orders)
    assert outcomes["ORDER_DELIVERED"] > 0.8 * finished


def test_delivery_minutes_and_money_add_up(clean_run):
    for evs in clean_run.orders().values():
        placed = evs[0]["data"]
        assert placed["total_inr"] == (
            placed["subtotal_inr"]
            + placed["delivery_fee_inr"]
            + placed["surge_fee_inr"]
            - placed["discount_inr"]
        )
        assert placed["subtotal_inr"] == sum(
            i["qty"] * i["unit_price_inr"] for i in placed["items"]
        )
        for e in evs:
            if e["event_type"] == "ORDER_DELIVERED":
                minutes = (parse_ts(e["event_ts"]) - parse_ts(evs[0]["event_ts"])) / 60
                assert abs(minutes - e["data"]["actual_delivery_min"]) < 0.01


def test_h3_cells_and_zones_are_consistent(clean_run):
    zone_map = ZoneMap.load()
    for v in clean_run.values:
        d = v["data"]
        if v["event_type"] == "ORDER_PLACED":
            assert h3.latlng_to_cell(d["delivery_lat"], d["delivery_lng"], 8) == d["h3_r8"]
            assert zone_map.zone_of_cell(d["h3_r8"]) == d["zone_id"]
        elif v["event_type"] == "DRIVER_LOCATION":
            assert h3.latlng_to_cell(d["lat"], d["lng"], 8) == d["h3_r8"]
            assert zone_map.zone_of_cell(d["h3_r8"]) == d["zone_id"]


def test_a_driver_carries_one_order_at_a_time(clean_run):
    active: dict[str, str] = {}  # driver -> order
    for v in sorted(clean_run.values, key=lambda v: v["event_ts"]):
        d, t = v["data"], v["event_type"]
        if t == "DRIVER_ASSIGNED":
            assert d["driver_id"] not in active, d
            active[d["driver_id"]] = d["order_id"]
        elif t in TERMINAL:
            for drv, oid in list(active.items()):
                if oid == d["order_id"]:
                    del active[drv]


def test_reference_data_is_emitted(clean_run):
    types = {v["event_type"] for v in clean_run.values}
    assert {"RESTAURANT_UPDATED", "WEATHER_UPDATED", "DRIVER_LOCATION"} <= types
    snapshots = [
        v
        for v in clean_run.values
        if v["event_type"] == "RESTAURANT_UPDATED" and v["data"]["change_type"] == "SNAPSHOT"
    ]
    assert len(snapshots) == 300


def test_driver_pings_every_30s(clean_run):
    pings: dict[str, list[float]] = {}
    for e in clean_run.events:
        if e.value["event_type"] == "DRIVER_LOCATION" and e.value["data"]["status"] != "OFFLINE":
            pings.setdefault(e.key, []).append(e.event_ts)
    gaps = [b - a for ts in pings.values() for a, b in zip(ts, ts[1:], strict=False)]
    assert len(pings) > 100
    assert sum(abs(g - 30) < 1e-6 for g in gaps) > 0.95 * len(gaps)  # rest = shift gaps
