import random
import statistics
from collections import Counter

from simulator import demand
from simulator.chaos import ChaosConfig
from simulator.clock import parse_ts

from .conftest import simulate


def test_dinner_peak_beats_lunch_beats_early_morning():
    assert demand.hour_curve(21) > demand.hour_curve(13) > demand.hour_curve(4)
    assert demand.is_peak(20.5) and demand.is_peak(13) and not demand.is_peak(4)


def test_zone_profiles():
    assert demand.profile_factor("it_park", 13, weekend=False) > demand.profile_factor(
        "it_park", 13, weekend=True
    )
    assert demand.profile_factor("nightlife", 23.5, False) > demand.profile_factor(
        "residential", 23.5, False
    )
    assert demand.profile_factor("nightlife", 21, True) > demand.profile_factor(
        "nightlife", 21, False
    )


def test_poisson_mean():
    rng = random.Random(0)
    for lam in (0.5, 3.0, 50.0):
        mean = statistics.fmean(demand.poisson(rng, lam) for _ in range(20_000))
        assert abs(mean - lam) < 0.05 * lam + 0.05


def _rain_share(month_ts: float) -> float:
    model = demand.WeatherModel(random.Random(0))
    model.initial(month_ts)
    raining = 0
    for _ in range(20_000):
        model.step(month_ts)
        raining += model.state.is_raining
    return raining / 20_000


def test_monsoon_rains_more_than_winter():
    assert _rain_share(parse_ts("2026-07-15T12:00")) > 3 * _rain_share(parse_ts("2026-01-15T12:00"))


def test_force_rain_never_stops():
    model = demand.WeatherModel(random.Random(0), force_rain=True)
    assert model.initial(0).is_raining
    assert all(model.step(0) is None for _ in range(100))
    assert model.state.is_raining


def test_rain_and_rush_hour_slow_travel():
    ts_night, ts_rush = parse_ts("2026-08-14T03:00"), parse_ts("2026-08-14T19:00")
    rain = demand.RAIN["HEAVY"]

    def t(ts, w):
        return demand.travel_minutes(5.0, ts, w, random.Random(1))

    assert t(ts_rush, demand.CLEAR) > t(ts_night, demand.CLEAR)
    assert t(ts_rush, rain) > t(ts_rush, demand.CLEAR)


def test_rain_raises_order_volume():
    kwargs = {"emit_pings": False, "chaos": ChaosConfig.disabled(), "start": "2026-10-09T18:00"}
    dry = simulate(**kwargs).stats.events_by_type["ORDER_PLACED"]
    wet = simulate(force_rain=True, **kwargs).stats.events_by_type["ORDER_PLACED"]
    assert wet > 1.2 * dry


def test_peak_hours_get_more_orders(clean_run):
    hours = Counter(
        int(v["event_ts"][11:13]) for v in clean_run.values if v["event_type"] == "ORDER_PLACED"
    )
    # UTC hours: 12 = 17:30-18:30 IST (pre-dinner), 15 = 20:30-21:30 IST (dinner peak)
    assert hours[15] > 1.5 * hours[12]
