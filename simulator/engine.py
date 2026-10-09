"""Discrete-event simulation of a Pune food-delivery marketplace.

A priority queue holds future actions (place order, restaurant decision, dispatch, pickup,
...). The loop pops the next action, first releasing every event whose emit time has come
(via ChaosStage), then runs the action, which may emit events and schedule more actions.
"""

from __future__ import annotations

import heapq
import itertools
import random
import time
import uuid
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field

import h3

from simulator import demand
from simulator.chaos import ChaosStage
from simulator.clock import Pacer, ist_hour, ist_midnight
from simulator.config import SimConfig
from simulator.entities import Driver, Leg, Order, Restaurant, World
from simulator.events import build_event
from simulator.sinks import ManifestWriter, Sink
from simulator.zones import ZoneMap, cell_of, road_km

DEMAND_TICK_S = 60.0
PAYMENT_METHODS = (["UPI", "CARD", "COD", "WALLET"], [0.62, 0.18, 0.12, 0.08])
FREE_DELIVERY_ABOVE_INR = 399
PROMISE_BUFFER_MIN = 12
BATTERY_DRAIN_PER_MIN = 0.12


def make_run_id(mode: str, seed: int) -> str:
    return f"{mode}-{time.strftime('%Y%m%dT%H%M%S')}-s{seed}"


@dataclass
class RunStats:
    run_id: str
    sim_start: float
    sim_end: float
    events_by_type: Counter = field(default_factory=Counter)
    emitted: int = 0
    anomalies: Counter = field(default_factory=Counter)
    orders_placed: int = 0
    wall_seconds: float = 0.0


class Simulator:
    def __init__(
        self,
        cfg: SimConfig,
        sink: Sink,
        *,
        run_id: str | None = None,
        manifest: ManifestWriter | None = None,
        now: float | None = None,
    ) -> None:
        self.cfg = cfg
        self.sink = sink
        seed = cfg.seed if cfg.seed is not None else random.SystemRandom().randrange(2**32)
        self.seed = seed
        self.rng = random.Random(seed)
        self.start, self.end = cfg.window(now)
        self.run_id = run_id or make_run_id(cfg.mode, seed)
        self.stats = RunStats(self.run_id, self.start, self.end)

        self.zone_map = ZoneMap.load()
        self.world = World.generate(
            self.zone_map,
            world_seed=cfg.world_seed,
            launch_ts=cfg.launch_ts,
            n_restaurants=cfg.n_restaurants,
            n_customers=cfg.n_customers,
            n_drivers=cfg.n_drivers,
        )
        self.manifest = manifest or ManifestWriter(None)
        self.chaos = ChaosStage(
            cfg.chaos, random.Random(self.rng.getrandbits(64)), on_anomaly=self._on_anomaly
        )
        self.weather_model = demand.WeatherModel(
            random.Random(self.rng.getrandbits(64)), cfg.force_rain
        )
        self.weather = demand.CLEAR
        self.pacer = Pacer(self.start, cfg.speed, cfg.max_eps) if cfg.mode == "live" else None

        self._queue: list[tuple[float, int, Callable[..., None], tuple]] = []
        self._seq = itertools.count()
        self.orders: dict[str, Order] = {}
        # dicts used as ordered sets: deterministic iteration regardless of PYTHONHASHSEED
        self.idle_by_cell: dict[str, dict[str, None]] = {}
        self.unassigned_by_zone: Counter = Counter()
        self._total_weight = sum(z.weight for z in self.zone_map.zones)

    # -- plumbing ------------------------------------------------------------------------
    def schedule(self, ts: float, fn: Callable[..., None], *args: object) -> None:
        if ts < self.end:
            heapq.heappush(self._queue, (ts, next(self._seq), fn, args))

    def _new_id(self, prefix: str = "") -> str:
        u = uuid.UUID(int=self.rng.getrandbits(128), version=4)
        return f"{prefix}{u.hex[:16]}" if prefix else str(u)

    def emit(self, ts: float, event_type: str, data: dict, source: str | None = None) -> None:
        self.chaos.submit(build_event(event_type, self._new_id(), ts, data, source))
        self.stats.events_by_type[event_type] += 1

    def _on_anomaly(self, record: dict) -> None:
        self.stats.anomalies[record["anomaly"]] += 1
        self.manifest(record)

    def _release(self, releases) -> None:
        for emit_ts, ev in releases:
            if self.pacer is not None:
                self.pacer.wait_until(emit_ts)
            self.sink.write(ev)
            self.stats.emitted += 1

    def run(self) -> RunStats:
        t0 = time.monotonic()
        try:
            self._bootstrap()
            while self._queue:
                ts, _, fn, args = heapq.heappop(self._queue)
                self._release(self.chaos.drain(ts))
                fn(ts, *args)
            self._release(self.chaos.drain(self.end))
            self._release(self.chaos.flush(self.end))
        finally:
            self.sink.close()
            self.manifest.close()
            self.stats.wall_seconds = time.monotonic() - t0
        return self.stats

    # -- bootstrap -----------------------------------------------------------------------
    def _bootstrap(self) -> None:
        start = self.start
        self.weather = self.weather_model.initial(start)
        self._emit_weather(start)
        self.schedule(start + demand.WEATHER_STEP_S, self._weather_step)
        for r in self.world.restaurants.values():
            self.emit(
                start,
                "RESTAURANT_UPDATED",
                {**r.snapshot(), "change_type": "SNAPSHOT", "changed_fields": []},
            )
        for c in self.world.customer_signups_between(start, self.end):
            self.schedule(c.signup_ts, self._customer_signup, c.customer_id)
        # Plan yesterday too, so night shifts that cross midnight are already on the road.
        day = ist_midnight(start) - 86400
        self._plan_day(start, day)
        self._plan_day(start, day + 86400)
        self.schedule(day + 2 * 86400, self._plan_next_day, day + 2 * 86400)
        self.schedule(start, self._demand_tick)

    def _plan_next_day(self, ts: float, day: float) -> None:
        self._plan_day(ts, day)
        self.schedule(day + 86400, self._plan_next_day, day + 86400)

    def _plan_day(self, now: float, day: float) -> None:
        rng = self.rng
        for d in self.world.drivers.values():
            for h0, h1 in d.shifts:
                on = day + h0 * 3600 + rng.uniform(-900, 900)
                off = day + h1 * 3600 + rng.uniform(-900, 900)
                if off > now:
                    self.schedule(max(on, now), self._driver_online, d.driver_id, off)
        for r in self.world.restaurants.values():
            if rng.random() < 0.03:
                ts = day + rng.uniform(0, 86400)
                if ts >= now:
                    self.schedule(ts, self._restaurant_update, r.restaurant_id)

    # -- weather / reference data --------------------------------------------------------
    def _emit_weather(self, ts: float) -> None:
        w = self.weather
        self.emit(
            ts,
            "WEATHER_UPDATED",
            {
                "city": "PUNE",
                "condition": w.condition,
                "intensity": w.intensity,
                "demand_multiplier": w.demand_multiplier,
                "supply_multiplier": w.supply_multiplier,
                "travel_time_multiplier": w.travel_time_multiplier,
            },
        )

    def _weather_step(self, ts: float) -> None:
        new = self.weather_model.step(ts)
        if new is not None:
            started_raining = new.is_raining and not self.weather.is_raining
            self.weather = new
            self._emit_weather(ts)
            if started_raining:  # some idle drivers call it a day when the rain starts
                for d in list(self.world.drivers.values()):
                    if d.status == "IDLE" and self.rng.random() < 1 - new.supply_multiplier:
                        self._go_offline_now(ts, d)
        self.schedule(ts + demand.WEATHER_STEP_S, self._weather_step)

    def _restaurant_update(self, ts: float, restaurant_id: str) -> None:
        r = self.world.restaurants[restaurant_id]
        roll = self.rng.random()
        if roll < 0.6:
            r.rating = round(min(5.0, max(1.0, r.rating + self.rng.choice([-0.1, 0.1]))), 1)
            changed = ["rating"]
        elif roll < 0.9:
            r.avg_prep_min = max(5.0, r.avg_prep_min + self.rng.choice([-2.0, 2.0]))
            changed = ["avg_prep_min"]
        else:
            r.is_active = not r.is_active
            changed = ["is_active"]
            if not r.is_active:  # temporarily closed; reopens in 1-3 days
                self.schedule(
                    ts + self.rng.uniform(1, 3) * 86400, self._restaurant_reopen, r.restaurant_id
                )
        self.emit(
            ts,
            "RESTAURANT_UPDATED",
            {**r.snapshot(), "change_type": "UPDATE", "changed_fields": changed},
        )

    def _restaurant_reopen(self, ts: float, restaurant_id: str) -> None:
        r = self.world.restaurants[restaurant_id]
        if not r.is_active:
            r.is_active = True
            self.emit(
                ts,
                "RESTAURANT_UPDATED",
                {**r.snapshot(), "change_type": "UPDATE", "changed_fields": ["is_active"]},
            )

    def _customer_signup(self, ts: float, customer_id: str) -> None:
        c = self.world.customers[customer_id]
        self.emit(ts, "CUSTOMER_REGISTERED", self.world.customer_profile(c))

    # -- demand --------------------------------------------------------------------------
    def _demand_tick(self, ts: float) -> None:
        cfg, rng = self.cfg, self.rng
        hour = ist_hour(ts)
        weekend = demand.is_weekend(ts)
        growth = min(
            cfg.max_growth, self.world.active_customers(ts) / cfg.reference_active_customers
        )
        city_rate = (
            cfg.base_orders_per_min
            * demand.hour_curve(hour)
            * growth
            * self.weather.demand_multiplier
        )
        for z in self.zone_map.zones:
            lam = (
                city_rate
                * z.weight
                / self._total_weight
                * demand.profile_factor(z.profile, hour, weekend)
            )
            for _ in range(demand.poisson(rng, lam * DEMAND_TICK_S / 60)):
                self.schedule(ts + rng.uniform(0, DEMAND_TICK_S), self._place_order, z.zone_id)
        self.schedule(ts + DEMAND_TICK_S, self._demand_tick)

    def _pick_restaurant(self, zone_id: str) -> Restaurant | None:
        rng = self.rng
        zid = zone_id if rng.random() < 0.75 else rng.choice(self.world.neighbour_zones[zone_id])
        pool = [r for r in self.world.restaurants_by_zone[zid] if r.is_active]
        if not pool:
            return None
        return rng.choices(pool, [r.rating**3 for r in pool])[0]

    def _surge_fee(self, zone_id: str) -> int:
        """Simple in-simulator surge: open unassigned orders vs idle drivers in the zone."""
        idle = sum(len(self.idle_by_cell.get(c, ())) for c in self.zone_map.by_id[zone_id].cells)
        ratio = self.unassigned_by_zone[zone_id] / max(1, idle)
        fee = 0 if ratio < 1 else 10 if ratio < 2 else 20 if ratio < 3 else 30
        return fee + (10 if self.weather.is_raining else 0)

    def _place_order(self, ts: float, zone_id: str) -> None:
        rng = self.rng
        customer = self.world.pick_customer(zone_id, ts, rng)
        restaurant = self._pick_restaurant(zone_id)
        if customer is None or restaurant is None:
            return
        n_items = rng.choices([1, 2, 3], [0.5, 0.35, 0.15])[0]
        items = [
            {
                "item_id": m.item_id,
                "name": m.name,
                "qty": rng.choices([1, 2, 3], [0.75, 0.2, 0.05])[0],
                "unit_price_inr": m.price_inr,
            }
            for m in rng.sample(restaurant.menu, k=min(n_items, len(restaurant.menu)))
        ]
        subtotal = sum(i["qty"] * i["unit_price_inr"] for i in items)
        dist = road_km((restaurant.lat, restaurant.lng), (customer.lat, customer.lng))
        delivery_fee = 0 if subtotal >= FREE_DELIVERY_ABOVE_INR else min(60, 25 + 5 * int(dist))
        surge = self._surge_fee(zone_id)
        discount = min(100, round(subtotal * 0.2)) if rng.random() < 0.3 else 0
        promised = round(
            restaurant.avg_prep_min + demand.estimated_travel_minutes(dist, ts) + PROMISE_BUFFER_MIN
        )

        order = Order(
            order_id=self._new_id("ord_"),
            customer_id=customer.customer_id,
            restaurant_id=restaurant.restaurant_id,
            zone_id=customer.zone_id,
            placed_ts=ts,
            delivery_lat=customer.lat,
            delivery_lng=customer.lng,
            delivery_cell=customer.cell,
            road_km=dist,
            promised_eta_min=promised,
        )
        self.orders[order.order_id] = order
        self.unassigned_by_zone[order.zone_id] += 1
        self.stats.orders_placed += 1
        self._order_event(
            ts,
            order,
            "ORDER_PLACED",
            {
                "order_id": order.order_id,
                "customer_id": customer.customer_id,
                "restaurant_id": restaurant.restaurant_id,
                "zone_id": customer.zone_id,
                "h3_r8": customer.cell,
                "delivery_lat": customer.lat,
                "delivery_lng": customer.lng,
                "items": items,
                "subtotal_inr": subtotal,
                "delivery_fee_inr": delivery_fee,
                "surge_fee_inr": surge,
                "discount_inr": discount,
                "total_inr": subtotal + delivery_fee + surge - discount,
                "payment_method": rng.choices(*PAYMENT_METHODS)[0],
                "promised_eta_min": promised,
                "is_raining": self.weather.is_raining,
            },
        )
        self.schedule(ts + rng.uniform(30, 180), self._restaurant_decision, order.order_id)
        if rng.random() < 0.03:
            self.schedule(ts + rng.uniform(60, 900), self._customer_cancel, order.order_id)

    def _order_event(
        self, ts: float, order: Order, event_type: str, data: dict, source: str | None = None
    ) -> None:

        order.last_event = event_type.removeprefix("ORDER_")
        self.emit(ts, event_type, data, source)

    # -- order lifecycle -----------------------------------------------------------------
    def _live_order(self, order_id: str) -> Order | None:
        o = self.orders.get(order_id)
        return o if o is not None and o.status not in ("DELIVERED", "CANCELLED") else None

    def _restaurant_decision(self, ts: float, order_id: str) -> None:
        o = self._live_order(order_id)
        if o is None or o.status != "PLACED":
            return
        r = self.world.restaurants[o.restaurant_id]
        p_reject = r.reject_prob * (1.5 if demand.is_peak(ist_hour(ts)) else 1.0)
        roll = self.rng.random()
        if roll < p_reject:
            self._cancel(ts, o, "RESTAURANT", "RESTAURANT_REJECTED")
            return
        if roll < p_reject + 0.01:
            self._cancel(ts, o, "RESTAURANT", "ITEM_UNAVAILABLE")
            return
        o.status = "ACCEPTED"
        o.dispatch_deadline = ts + self.cfg.no_driver_timeout_s
        self._order_event(
            ts,
            o,
            "ORDER_ACCEPTED",
            {
                "order_id": o.order_id,
                "restaurant_id": r.restaurant_id,
                "estimated_prep_min": round(r.avg_prep_min),
            },
        )
        prep = demand.actual_prep_minutes(r.avg_prep_min, ts, self.rng)
        self.schedule(ts + prep * 60, self._food_ready, o.order_id, prep)
        self.schedule(ts + self.rng.uniform(5, 40), self._dispatch, o.order_id)

    def _find_driver(self, cell: str, lat: float, lng: float) -> tuple[Driver, int] | None:
        """Nearest idle driver, searching outward ring by ring."""
        for k in range(self.cfg.max_search_ring_k + 1):
            ring = [cell] if k == 0 else h3.grid_ring(cell, k)
            candidates = [did for c in ring for did in self.idle_by_cell.get(c, ())]
            if candidates:
                drivers = [self.world.drivers[did] for did in candidates]
                best = min(
                    drivers, key=lambda d: (road_km((d.lat, d.lng), (lat, lng)), d.driver_id)
                )
                return best, k
        return None

    def _dispatch(self, ts: float, order_id: str) -> None:
        o = self._live_order(order_id)
        if o is None or o.driver_id is not None:
            return
        o.assign_attempts += 1
        r = self.world.restaurants[o.restaurant_id]
        found = self._find_driver(r.cell, r.lat, r.lng)
        if found is None:
            if ts + self.cfg.dispatch_retry_s > o.dispatch_deadline:
                self._cancel(ts, o, "PLATFORM", "NO_DRIVER_AVAILABLE")
            else:
                self.schedule(ts + self.cfg.dispatch_retry_s, self._dispatch, order_id)
            return
        d, k = found
        self._set_busy(d)
        d.status, d.order_id = "ASSIGNED", o.order_id
        dist = road_km((d.lat, d.lng), (r.lat, r.lng))
        arrive = ts + demand.travel_minutes(dist, ts, self.weather, self.rng) * 60
        d.leg = Leg(d.lat, d.lng, r.lat, r.lng, ts, arrive, dist)
        o.driver_id = d.driver_id
        self.unassigned_by_zone[o.zone_id] -= 1
        self._order_event(
            ts,
            o,
            "DRIVER_ASSIGNED",
            {
                "order_id": o.order_id,
                "driver_id": d.driver_id,
                "assign_attempts": o.assign_attempts,
                "driver_distance_km": round(dist, 2),
                "search_ring_k": k,
            },
        )
        self.schedule(arrive, self._driver_at_restaurant, o.order_id, d.driver_id)

    def _food_ready(self, ts: float, order_id: str, prep_min: float) -> None:
        o = self._live_order(order_id)
        if o is None:
            return
        o.food_ready = True
        self._order_event(
            ts,
            o,
            "FOOD_READY",
            {
                "order_id": o.order_id,
                "restaurant_id": o.restaurant_id,
                "actual_prep_min": round(prep_min, 2),
            },
        )
        if o.driver_at_restaurant:
            self.schedule(ts + self.rng.uniform(30, 90), self._pickup, o.order_id)

    def _driver_at_restaurant(self, ts: float, order_id: str, driver_id: str) -> None:
        o = self._live_order(order_id)
        if o is None or o.driver_id != driver_id:
            return
        r = self.world.restaurants[o.restaurant_id]
        d = self.world.drivers[driver_id]
        d.status, d.leg = "AT_RESTAURANT", None
        d.lat, d.lng, d.cell = r.lat, r.lng, r.cell
        o.driver_at_restaurant = True
        if o.food_ready:
            self.schedule(ts + self.rng.uniform(60, 180), self._pickup, o.order_id)

    def _pickup(self, ts: float, order_id: str) -> None:
        o = self._live_order(order_id)
        if o is None or o.status == "PICKED_UP":
            return
        d = self.world.drivers[o.driver_id]
        o.status = "PICKED_UP"
        self._order_event(
            ts,
            o,
            "ORDER_PICKED_UP",
            {"order_id": o.order_id, "driver_id": d.driver_id, "lat": d.lat, "lng": d.lng},
        )
        d.status = "DELIVERING"
        drive = demand.travel_minutes(o.road_km, ts, self.weather, self.rng) * 60
        d.leg = Leg(d.lat, d.lng, o.delivery_lat, o.delivery_lng, ts, ts + drive, o.road_km)
        self.schedule(ts + drive + self.rng.uniform(60, 240), self._deliver, o.order_id)

    def _deliver(self, ts: float, order_id: str) -> None:
        o = self._live_order(order_id)
        if o is None:
            return
        d = self.world.drivers[o.driver_id]
        o.status = "DELIVERED"
        self._order_event(
            ts,
            o,
            "ORDER_DELIVERED",
            {
                "order_id": o.order_id,
                "driver_id": d.driver_id,
                "lat": o.delivery_lat,
                "lng": o.delivery_lng,
                "distance_km": round(o.road_km, 2),
                "actual_delivery_min": round((ts - o.placed_ts) / 60, 2),
            },
        )
        d.lat, d.lng, d.cell, d.leg = o.delivery_lat, o.delivery_lng, o.delivery_cell, None
        self._release_driver(ts, d)
        del self.orders[order_id]

    def _customer_cancel(self, ts: float, order_id: str) -> None:
        o = self._live_order(order_id)
        if o is not None and o.status in ("PLACED", "ACCEPTED"):
            self._cancel(ts, o, "CUSTOMER", "CUSTOMER_CHANGED_MIND")

    def _cancel(self, ts: float, o: Order, by: str, reason: str) -> None:
        state = o.last_event
        o.status = "CANCELLED"
        source = {
            "CUSTOMER": "customer_app",
            "RESTAURANT": "restaurant_app",
            "PLATFORM": "platform",
        }[by]
        self._order_event(
            ts,
            o,
            "ORDER_CANCELLED",
            {
                "order_id": o.order_id,
                "cancelled_by": by,
                "reason": reason,
                "state_at_cancel": state,
            },
            source,
        )
        if o.driver_id is None:
            self.unassigned_by_zone[o.zone_id] -= 1
        else:
            d = self.world.drivers[o.driver_id]
            d.lat, d.lng, _ = d.position_at(ts)
            d.cell, d.leg = cell_of(d.lat, d.lng), None
            self._release_driver(ts, d)
        del self.orders[o.order_id]

    # -- drivers -------------------------------------------------------------------------
    def _set_idle(self, d: Driver) -> None:
        d.status, d.order_id = "IDLE", None
        self.idle_by_cell.setdefault(d.cell, {})[d.driver_id] = None

    def _set_busy(self, d: Driver) -> None:
        cell_set = self.idle_by_cell.get(d.cell)
        if cell_set is not None:
            cell_set.pop(d.driver_id, None)
            if not cell_set:
                del self.idle_by_cell[d.cell]

    def _release_driver(self, ts: float, d: Driver) -> None:
        if d.end_shift_after_delivery:
            self._go_offline_now(ts, d)
        else:
            self._set_idle(d)

    def _driver_online(self, ts: float, driver_id: str, off_ts: float) -> None:
        d = self.world.drivers[driver_id]
        if d.status != "OFFLINE":
            return  # overlapping shift windows
        if self.rng.random() > self.cfg.driver_attendance * self.weather.supply_multiplier:
            return  # skipped this shift
        d.gen += 1
        d.lat, d.lng, d.cell = self.zone_map.random_point(d.home_zone_id, self.rng)
        d.online_ts, d.battery_start = ts, self.rng.uniform(55, 100)
        d.end_shift_after_delivery, d.leg = False, None
        self._set_idle(d)
        if self.cfg.emit_pings:
            self.schedule(
                ts + self.rng.uniform(0, self.cfg.ping_interval_s), self._ping, driver_id, d.gen
            )
        self.schedule(off_ts, self._driver_offline, driver_id, d.gen)

    def _driver_offline(self, ts: float, driver_id: str, gen: int) -> None:
        d = self.world.drivers[driver_id]
        if d.gen != gen or d.status == "OFFLINE":
            return
        if d.status == "IDLE":
            self._go_offline_now(ts, d)
        else:
            d.end_shift_after_delivery = True

    def _go_offline_now(self, ts: float, d: Driver) -> None:
        self._set_busy(d)
        d.status, d.order_id = "OFFLINE", None
        if self.cfg.emit_pings:
            self._emit_ping(ts, d)

    def _ping(self, ts: float, driver_id: str, gen: int) -> None:
        d = self.world.drivers[driver_id]
        if d.gen != gen or d.status == "OFFLINE":
            return
        self._emit_ping(ts, d)
        self.schedule(ts + self.cfg.ping_interval_s, self._ping, driver_id, gen)

    def _emit_ping(self, ts: float, d: Driver) -> None:
        lat, lng, speed = d.position_at(ts)
        cell = cell_of(lat, lng)
        battery = d.battery_start - BATTERY_DRAIN_PER_MIN * (ts - d.online_ts) / 60
        self.emit(
            ts,
            "DRIVER_LOCATION",
            {
                "driver_id": d.driver_id,
                "lat": lat,
                "lng": lng,
                "h3_r8": cell,
                "zone_id": self.zone_map.zone_of_cell(cell),
                "status": d.status,
                "order_id": d.order_id,
                "speed_kmh": round(speed * self.rng.uniform(0.85, 1.15), 1) if speed else 0.0,
                "battery_pct": max(5, int(battery)),
            },
        )
