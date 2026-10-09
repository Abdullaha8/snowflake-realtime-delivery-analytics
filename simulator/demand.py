"""Demand, supply, weather, prep-time and travel-time models.

Kept deliberately simple: piecewise-linear curves and a two-state weather Markov chain.
Every number here is a tunable assumption, not a measurement.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass

from simulator.clock import ist_hour, to_ist

# (IST hour, relative order rate). Lunch peak ~13:00, bigger dinner peak ~21:00.
HOUR_CURVE: list[tuple[float, float]] = [
    (0, 0.35), (1, 0.20), (2, 0.10), (3, 0.05), (5, 0.03), (7, 0.10), (8, 0.25),
    (10, 0.35), (11, 0.55), (12, 0.90), (13, 1.00), (14, 0.80), (15, 0.45),
    (16, 0.35), (17, 0.40), (18, 0.55), (19, 0.80), (20, 1.25), (21, 1.35),
    (22, 1.00), (23, 0.60), (24, 0.35),
]  # fmt: skip

# (IST hour, travel-time multiplier). Morning and evening rush, empty roads at night.
TRAFFIC_CURVE: list[tuple[float, float]] = [
    (0, 0.80), (6, 0.80), (8, 1.15), (9.5, 1.35), (11, 1.10), (13, 1.15), (15, 1.05),
    (17, 1.20), (19, 1.45), (21, 1.20), (23, 0.90), (24, 0.80),
]  # fmt: skip

BASE_SPEED_KMH = 22.0
PEAK_THRESHOLD = 0.8  # hour_curve value above which we call it "peak"
MONSOON_RAIN_START_P = {5: 0.005, 6: 0.05, 7: 0.06, 8: 0.05, 9: 0.04, 10: 0.015, 11: 0.005}
DRY_RAIN_START_P = 0.002
RAIN_STOP_P = 0.25  # per 15-minute step -> mean spell ~1 hour
WEATHER_STEP_S = 900


def _interp(curve: list[tuple[float, float]], x: float) -> float:
    for (x0, y0), (x1, y1) in zip(curve, curve[1:], strict=False):
        if x0 <= x <= x1:
            return y0 + (y1 - y0) * (x - x0) / (x1 - x0)
    return curve[-1][1]


def hour_curve(hour: float) -> float:
    return _interp(HOUR_CURVE, hour % 24)


def traffic_factor(hour: float) -> float:
    return _interp(TRAFFIC_CURVE, hour % 24)


def is_peak(hour: float) -> bool:
    return hour_curve(hour) >= PEAK_THRESHOLD


def is_weekend(ts: float) -> bool:
    return to_ist(ts).weekday() >= 5


def profile_factor(profile: str, hour: float, weekend: bool) -> float:
    """Zone-type shape on top of the city curve."""
    lunch = 11 <= hour < 15
    dinner = 19 <= hour < 23
    late_night = hour >= 23 or hour < 2
    if profile == "it_park":  # office lunch orders, quiet weekends
        f = 1.6 if lunch else 0.7 if dinner else 0.9
        return f * (0.5 if weekend else 1.0)
    if profile == "nightlife":  # dinner and late-night heavy, busy weekends
        f = 0.8 if lunch else 1.4 if dinner else 1.8 if late_night else 1.0
        return f * (1.4 if weekend else 1.0)
    return 1.15 if weekend else 1.0  # residential


def poisson(rng: random.Random, lam: float) -> int:
    if lam <= 0:
        return 0
    if lam > 30:  # normal approximation is plenty for large rates
        return max(0, round(rng.gauss(lam, math.sqrt(lam))))
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


@dataclass(frozen=True)
class Weather:
    condition: str  # CLEAR | RAIN
    intensity: str | None  # LIGHT | MODERATE | HEAVY
    demand_multiplier: float
    supply_multiplier: float
    travel_time_multiplier: float

    @property
    def is_raining(self) -> bool:
        return self.condition == "RAIN"


CLEAR = Weather("CLEAR", None, 1.0, 1.0, 1.0)
RAIN = {
    "LIGHT": Weather("RAIN", "LIGHT", 1.15, 0.90, 1.10),
    "MODERATE": Weather("RAIN", "MODERATE", 1.35, 0.75, 1.25),
    "HEAVY": Weather("RAIN", "HEAVY", 1.50, 0.65, 1.40),
}


class WeatherModel:
    """City-wide CLEAR <-> RAIN Markov chain, stepped every 15 minutes."""

    def __init__(self, rng: random.Random, force_rain: bool = False) -> None:
        self.rng = rng
        self.force_rain = force_rain
        self.state = CLEAR

    def _start_p(self, ts: float) -> float:
        return MONSOON_RAIN_START_P.get(to_ist(ts).month, DRY_RAIN_START_P)

    def _random_rain(self) -> Weather:
        name = self.rng.choices(["LIGHT", "MODERATE", "HEAVY"], [0.5, 0.35, 0.15])[0]
        return RAIN[name]

    def initial(self, ts: float) -> Weather:
        if self.force_rain:
            self.state = RAIN["MODERATE"]
        else:
            p = self._start_p(ts)
            stationary_rain = p / (p + RAIN_STOP_P)
            self.state = self._random_rain() if self.rng.random() < stationary_rain else CLEAR
        return self.state

    def step(self, ts: float) -> Weather | None:
        """Advance one step; return the new state only if it changed."""
        if self.force_rain:
            return None
        if self.state.is_raining:
            new = CLEAR if self.rng.random() < RAIN_STOP_P else self.state
        else:
            new = self._random_rain() if self.rng.random() < self._start_p(ts) else CLEAR
        if new == self.state:
            return None
        self.state = new
        return new


def actual_prep_minutes(avg_prep_min: float, ts: float, rng: random.Random) -> float:
    """Kitchens slow down when busy."""
    load = 1 + 0.25 * max(0.0, hour_curve(ist_hour(ts)) - PEAK_THRESHOLD)
    return avg_prep_min * rng.lognormvariate(0, 0.25) * load


def travel_minutes(road_km: float, ts: float, weather: Weather, rng: random.Random) -> float:
    speed = BASE_SPEED_KMH / traffic_factor(ist_hour(ts)) / weather.travel_time_multiplier
    return 1.0 + road_km / speed * 60 * rng.lognormvariate(0, 0.15)


def estimated_travel_minutes(road_km: float, ts: float) -> float:
    """What the platform *thinks* travel takes: knows traffic, ignores weather and noise."""
    return 1.0 + road_km / (BASE_SPEED_KMH / traffic_factor(ist_hour(ts))) * 60
