"""Entities (restaurants, customers, drivers, orders) and the static World.

The World is generated from `world_seed` only, so restaurant/customer/driver IDs and
attributes are identical across runs. Traffic randomness uses a separate run seed.
"""

from __future__ import annotations

import bisect
import math
import random
from dataclasses import dataclass

from faker import Faker

from simulator.zones import ZoneMap

# cuisine -> (dish names, typical price band 1..3)
CUISINES: dict[str, tuple[list[str], int]] = {
    "North Indian": (["Paneer Tikka", "Butter Chicken", "Dal Makhani", "Butter Naan",
                      "Chole Bhature", "Kadai Paneer", "Jeera Rice", "Tandoori Roti"], 2),
    "South Indian": (["Masala Dosa", "Idli Sambar", "Medu Vada", "Uttapam",
                      "Rava Dosa", "Filter Coffee", "Curd Rice"], 1),
    "Maharashtrian": (["Misal Pav", "Vada Pav", "Pithla Bhakri", "Sabudana Khichdi",
                       "Kolhapuri Chicken", "Puran Poli", "Thalipeeth"], 1),
    "Biryani": (["Chicken Dum Biryani", "Mutton Biryani", "Veg Biryani", "Egg Biryani",
                 "Raita", "Chicken 65", "Gulab Jamun"], 2),
    "Indo-Chinese": (["Veg Hakka Noodles", "Chicken Manchurian", "Schezwan Fried Rice",
                      "Chilli Paneer", "Spring Rolls", "Manchow Soup"], 1),
    "Pizza": (["Margherita Pizza", "Farmhouse Pizza", "Pepperoni Pizza", "Garlic Bread",
               "Pasta Arrabbiata", "Choco Lava Cake"], 2),
    "Burgers": (["Classic Veg Burger", "Chicken Burger", "Peri Peri Fries",
                 "Paneer Burger", "Cold Coffee", "Chicken Wings"], 1),
    "Cafe": (["Cappuccino", "Club Sandwich", "Avocado Toast", "Caesar Salad",
              "Brownie", "Iced Latte", "Shakshuka"], 3),
    "Desserts": (["Mango Mastani", "Kulfi", "Rasmalai", "Belgian Waffle",
                  "Cheesecake", "Ice Cream Sundae"], 2),
}  # fmt: skip
NAME_SUFFIX = {
    "North Indian": ["Dhaba", "Punjabi Rasoi", "Tandoor House", "Kitchen"],
    "South Indian": ["Udupi", "Dosa Corner", "Idli House", "Cafe"],
    "Maharashtrian": ["Misal House", "Bhojanalay", "Amruttulya", "Snacks"],
    "Biryani": ["Biryani House", "Dum Biryani", "Biryani Co."],
    "Indo-Chinese": ["Chinese Corner", "Wok Express", "Noodle Bar"],
    "Pizza": ["Pizzeria", "Pizza Co.", "Slice House"],
    "Burgers": ["Burger Joint", "Grill", "Burger Co."],
    "Cafe": ["Cafe", "Coffee House", "Bistro"],
    "Desserts": ["Sweets", "Creamery", "Dessert Bar"],
}
PRICE_RANGE = {1: (60, 220), 2: (140, 380), 3: (220, 650)}
SHIFT_TYPES: list[tuple[str, float, list[tuple[float, float]]]] = [
    # (name, probability, [(start IST hour, end IST hour)]); end > 24 means after midnight
    ("MORNING", 0.08, [(6.0, 12.5)]),
    ("LUNCH", 0.18, [(11.0, 15.5)]),
    ("DAY", 0.14, [(10.0, 19.0)]),
    ("EVENING", 0.08, [(15.0, 22.0)]),
    ("SPLIT", 0.20, [(11.0, 15.0), (18.5, 23.0)]),
    ("DINNER", 0.24, [(18.0, 24.5)]),
    ("NIGHT", 0.08, [(21.0, 30.0)]),
]
SIGNUP_CHANNELS = (["ORGANIC", "REFERRAL", "PAID_AD"], [0.5, 0.2, 0.3])


@dataclass
class MenuItem:
    item_id: str
    name: str
    price_inr: int


@dataclass
class Restaurant:
    restaurant_id: str
    name: str
    zone_id: str
    lat: float
    lng: float
    cell: str
    cuisine: str
    price_band: int
    avg_prep_min: float
    reject_prob: float
    rating: float
    menu: list[MenuItem]
    is_active: bool = True

    def snapshot(self) -> dict:
        return {
            "restaurant_id": self.restaurant_id,
            "name": self.name,
            "zone_id": self.zone_id,
            "h3_r8": self.cell,
            "lat": self.lat,
            "lng": self.lng,
            "cuisine": self.cuisine,
            "price_band": self.price_band,
            "avg_prep_min": round(self.avg_prep_min, 1),
            "rating": round(self.rating, 1),
            "is_active": self.is_active,
        }


@dataclass
class Customer:
    customer_id: str
    zone_id: str
    lat: float
    lng: float
    cell: str
    signup_ts: float
    churn_ts: float  # math.inf = loyal, never churns
    weight: float  # order-frequency weight (heavy-tailed)

    def is_active(self, ts: float) -> bool:
        return self.signup_ts <= ts < self.churn_ts


@dataclass
class Leg:
    """Straight-line movement between two points over [t0, t1]."""

    from_lat: float
    from_lng: float
    to_lat: float
    to_lng: float
    t0: float
    t1: float
    road_km: float


@dataclass
class Driver:
    driver_id: str
    home_zone_id: str
    shifts: list[tuple[float, float]]
    # runtime state
    status: str = "OFFLINE"  # OFFLINE | IDLE | ASSIGNED | AT_RESTAURANT | DELIVERING
    lat: float = 0.0
    lng: float = 0.0
    cell: str = ""
    order_id: str | None = None
    leg: Leg | None = None
    gen: int = 0  # bumped on every go-online; stale scheduled actions compare against it
    online_ts: float = 0.0
    battery_start: float = 100.0
    end_shift_after_delivery: bool = False

    def position_at(self, ts: float) -> tuple[float, float, float]:
        """(lat, lng, speed_kmh) at time ts."""
        leg = self.leg
        if leg is None or ts >= leg.t1 or leg.t1 <= leg.t0:
            return self.lat, self.lng, 0.0
        f = max(0.0, (ts - leg.t0) / (leg.t1 - leg.t0))
        lat = leg.from_lat + (leg.to_lat - leg.from_lat) * f
        lng = leg.from_lng + (leg.to_lng - leg.from_lng) * f
        speed = leg.road_km / ((leg.t1 - leg.t0) / 3600)
        return round(lat, 6), round(lng, 6), speed


@dataclass
class Order:
    order_id: str
    customer_id: str
    restaurant_id: str
    zone_id: str
    placed_ts: float
    delivery_lat: float
    delivery_lng: float
    delivery_cell: str
    road_km: float
    promised_eta_min: int
    status: str = "PLACED"  # PLACED | ACCEPTED | PICKED_UP | DELIVERED | CANCELLED
    last_event: str = "PLACED"
    driver_id: str | None = None
    food_ready: bool = False
    driver_at_restaurant: bool = False
    assign_attempts: int = 0
    dispatch_deadline: float = 0.0


class World:
    """Static, seed-determined population of Pune."""

    def __init__(
        self,
        zone_map: ZoneMap,
        restaurants: list[Restaurant],
        customers: list[Customer],
        drivers: list[Driver],
        world_seed: int,
        first_names: list[str],
        last_names: list[str],
    ) -> None:
        self.zone_map = zone_map
        self.restaurants = {r.restaurant_id: r for r in restaurants}
        self.customers = {c.customer_id: c for c in customers}
        self.drivers = {d.driver_id: d for d in drivers}
        self.world_seed = world_seed
        self._first_names = first_names
        self._last_names = last_names

        self.restaurants_by_zone: dict[str, list[Restaurant]] = {
            z.zone_id: [] for z in zone_map.zones
        }
        for r in restaurants:
            self.restaurants_by_zone[r.zone_id].append(r)
        self.customers_by_zone: dict[str, list[Customer]] = {z.zone_id: [] for z in zone_map.zones}
        for c in customers:
            self.customers_by_zone[c.zone_id].append(c)
        self._cum_weights = {
            zid: _cumsum([c.weight for c in cs]) for zid, cs in self.customers_by_zone.items()
        }
        self._signups = sorted(c.signup_ts for c in customers)
        self._churns = sorted(c.churn_ts for c in customers)
        self.neighbour_zones = {
            z.zone_id: zone_map.nearest_zones(z.zone_id, 2) for z in zone_map.zones
        }

    @classmethod
    def generate(
        cls,
        zone_map: ZoneMap,
        *,
        world_seed: int,
        launch_ts: float,
        n_restaurants: int,
        n_customers: int,
        n_drivers: int,
    ) -> World:
        rng = random.Random(world_seed)
        fake = Faker("en_IN")
        fake.seed_instance(world_seed)
        first_names = sorted({fake.first_name() for _ in range(400)})
        last_names = sorted({fake.last_name() for _ in range(300)})
        zone_ids = [z.zone_id for z in zone_map.zones]
        zone_weights = [z.weight for z in zone_map.zones]

        restaurants = []
        for i in range(1, n_restaurants + 1):
            zid = rng.choices(zone_ids, zone_weights)[0]
            lat, lng, cell = zone_map.random_point(zid, rng)
            cuisine = rng.choice(list(CUISINES))
            dishes, band = CUISINES[cuisine]
            band = min(3, max(1, band + rng.choice([-1, 0, 0, 0, 1])))
            lo, hi = PRICE_RANGE[band]
            rid = f"res_{i:04d}"
            menu = [
                MenuItem(f"itm_{i:04d}_{j:02d}", dish, int(round(rng.uniform(lo, hi), -1)))
                for j, dish in enumerate(
                    rng.sample(dishes, k=min(len(dishes), rng.randint(5, 8))), 1
                )
            ]
            name = f"{rng.choice(last_names)}'s {rng.choice(NAME_SUFFIX[cuisine])}"
            restaurants.append(
                Restaurant(
                    restaurant_id=rid,
                    name=name,
                    zone_id=zid,
                    lat=lat,
                    lng=lng,
                    cell=cell,
                    cuisine=cuisine,
                    price_band=band,
                    avg_prep_min=rng.uniform(10, 25),
                    reject_prob=rng.uniform(0.01, 0.05),
                    rating=round(min(4.9, max(3.0, rng.gauss(4.1, 0.35))), 1),
                    menu=menu,
                )
            )

        customers = []
        for i in range(1, n_customers + 1):
            zid = rng.choices(zone_ids, zone_weights)[0]
            lat, lng, cell = zone_map.random_point(zid, rng)
            # 20% sign up in launch week, the rest spread over the following ~10 months.
            offset_days = rng.uniform(0, 7) if rng.random() < 0.2 else rng.uniform(7, 300)
            signup = launch_ts + offset_days * 86400
            lifetime_days = math.inf if rng.random() < 0.15 else rng.expovariate(1 / 60)
            customers.append(
                Customer(
                    customer_id=f"cus_{i:06d}",
                    zone_id=zid,
                    lat=lat,
                    lng=lng,
                    cell=cell,
                    signup_ts=signup,
                    churn_ts=signup + lifetime_days * 86400,
                    weight=rng.paretovariate(1.5),
                )
            )

        drivers = []
        shift_names = [s[2] for s in SHIFT_TYPES]
        shift_p = [s[1] for s in SHIFT_TYPES]
        for i in range(1, n_drivers + 1):
            drivers.append(
                Driver(
                    driver_id=f"drv_{i:04d}",
                    home_zone_id=rng.choices(zone_ids, zone_weights)[0],
                    shifts=list(rng.choices(shift_names, shift_p)[0]),
                )
            )
        return cls(zone_map, restaurants, customers, drivers, world_seed, first_names, last_names)

    def active_customers(self, ts: float) -> int:
        return bisect.bisect_right(self._signups, ts) - bisect.bisect_right(self._churns, ts)

    def pick_customer(self, zone_id: str, ts: float, rng: random.Random) -> Customer | None:
        """Weighted pick among customers of the zone who are signed up and not churned."""
        pool = self.customers_by_zone[zone_id]
        if not pool:
            return None
        cum = self._cum_weights[zone_id]
        for _ in range(30):
            c = rng.choices(pool, cum_weights=cum)[0]
            if c.is_active(ts):
                return c
        return None

    def customer_signups_between(self, start: float, end: float) -> list[Customer]:
        return sorted(
            (c for c in self.customers.values() if start <= c.signup_ts < end),
            key=lambda c: c.signup_ts,
        )

    def customer_profile(self, c: Customer) -> dict:
        """PII generated lazily but deterministically per customer."""
        rng = random.Random(f"{self.world_seed}:{c.customer_id}")
        first, last = rng.choice(self._first_names), rng.choice(self._last_names)
        domain = rng.choice(["gmail.com", "yahoo.co.in", "outlook.com", "rediffmail.com"])
        return {
            "customer_id": c.customer_id,
            "full_name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}{rng.randint(1, 999)}@{domain}",
            "phone": f"+91{rng.choice('6789')}{rng.randint(0, 999_999_999):09d}",
            "zone_id": c.zone_id,
            "h3_r8": c.cell,
            "home_lat": c.lat,
            "home_lng": c.lng,
            "signup_channel": rng.choices(*SIGNUP_CHANNELS)[0],
        }


def _cumsum(xs: list[float]) -> list[float]:
    out, total = [], 0.0
    for x in xs:
        total += x
        out.append(total)
    return out
