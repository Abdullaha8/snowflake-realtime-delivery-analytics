"""Summarise a JSONL simulator run: volumes, order KPIs, and observed vs injected chaos."""

from __future__ import annotations

import gzip
import json
import statistics
from collections import Counter
from collections.abc import Iterable, Iterator
from pathlib import Path

from simulator.clock import ist_hour, parse_ts

LATE_THRESHOLD_S = 60.0


def iter_events(path: Path) -> Iterator[dict]:
    path = Path(path)
    files = sorted(p for p in path.rglob("*.jsonl*") if "_manifest" not in p.parts)
    for p in files:
        opener = gzip.open if p.suffix == ".gz" else open
        with opener(p, "rt", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)


def load_manifest(path: Path) -> list[dict]:
    out = []
    for p in sorted((Path(path) / "_manifest").glob("*.jsonl")):
        with open(p, encoding="utf-8") as f:
            out.extend(json.loads(line) for line in f if line.strip())
    return out


def _key(ev: dict) -> str:
    """Same key the producer partitions by (see events.EVENT_SPECS)."""
    d = ev["data"]
    if ev["event_type"] == "DRIVER_LOCATION":
        return f"driver_id:{d['driver_id']}"
    for field in ("order_id", "restaurant_id", "customer_id", "city"):
        if field in d:
            return f"{field}:{d[field]}"
    return ev["event_id"]


def summarize(events: Iterable[dict], manifest: list[dict] | None = None) -> dict:
    """`events` must be in emitted order (as written by the sink)."""
    by_type: Counter = Counter()
    seen_ids: set[str] = set()
    duplicate_copies = 0
    late = very_late = late_eligible = 0
    ooo = 0
    max_ts_by_key: dict[str, float] = {}
    placed: dict[str, dict] = {}
    delivered: dict[str, dict] = {}
    cancelled: Counter = Counter()
    orders_by_hour: Counter = Counter()
    orders_by_zone: Counter = Counter()
    weather: list[str] = []
    total = 0

    for ev in events:
        total += 1
        if ev["event_id"] in seen_ids:
            duplicate_copies += 1
            continue
        seen_ids.add(ev["event_id"])
        et = ev["event_type"]
        by_type[et] += 1
        ts, emitted = parse_ts(ev["event_ts"]), parse_ts(ev["emitted_ts"])
        if ev["source"] == "driver_app":
            late_eligible += 1
            lag = emitted - ts
            late += lag > LATE_THRESHOLD_S
            very_late += lag > 3600
        # out-of-order: event-time older than something already seen for the same key
        key = _key(ev)
        if ts < max_ts_by_key.get(key, float("-inf")):
            ooo += 1
        max_ts_by_key[key] = max(ts, max_ts_by_key.get(key, ts))

        d = ev["data"]
        if et == "ORDER_PLACED":
            placed[d["order_id"]] = d
            orders_by_hour[int(ist_hour(ts))] += 1
            orders_by_zone[d["zone_id"]] += 1
        elif et == "ORDER_DELIVERED":
            delivered[d["order_id"]] = d
        elif et == "ORDER_CANCELLED":
            cancelled[d["reason"]] += 1
        elif et == "WEATHER_UPDATED":
            weather.append(f"{ev['event_ts'][:16]} {d['condition']} {d['intensity'] or ''}".strip())

    unique = len(seen_ids)
    durations = [d["actual_delivery_min"] for d in delivered.values()]
    breaches = sum(
        1
        for oid, d in delivered.items()
        if oid in placed and d["actual_delivery_min"] > placed[oid]["promised_eta_min"]
    )
    gmv = sum(placed[oid]["total_inr"] for oid in delivered if oid in placed)
    n_placed = len(placed)
    return {
        "events_total": total,
        "events_unique": unique,
        "events_by_type": dict(sorted(by_type.items())),
        "orders": {
            "placed": n_placed,
            "delivered": len(delivered),
            "cancelled": dict(cancelled),
            "delivered_pct": _pct(len(delivered), n_placed),
            "cancelled_pct": _pct(sum(cancelled.values()), n_placed),
            "avg_delivery_min": round(statistics.fmean(durations), 1) if durations else None,
            "p90_delivery_min": _p90(durations),
            "sla_breach_pct": _pct(breaches, len(delivered)),
            "gmv_inr": gmv,
            "aov_inr": round(gmv / len(delivered)) if delivered else None,
        },
        "orders_by_ist_hour": dict(sorted(orders_by_hour.items())),
        "orders_by_zone": dict(orders_by_zone.most_common()),
        "weather_changes": weather,
        "chaos_observed": {
            "duplicate_copies": duplicate_copies,
            "duplicate_pct": _pct(duplicate_copies, unique),
            "late_gt_60s": late,
            "late_pct_of_driver_app": _pct(late, late_eligible),
            "very_late_gt_1h": very_late,
            "out_of_order": ooo,
            "out_of_order_pct": _pct(ooo, unique),
        },
        "chaos_injected": dict(Counter(m["anomaly"] for m in manifest)) if manifest else None,
    }


def _pct(n: int, d: int) -> float | None:
    return round(100 * n / d, 2) if d else None


def _p90(xs: list[float]) -> float | None:
    if len(xs) < 10:
        return None
    return round(statistics.quantiles(xs, n=10)[-1], 1)


def format_report(s: dict) -> str:
    lines = [f"Events: {s['events_total']:,} emitted, {s['events_unique']:,} unique"]
    lines += [f"  {k:<22}{v:>10,}" for k, v in s["events_by_type"].items()]
    o = s["orders"]
    lines += [
        "",
        f"Orders: {o['placed']:,} placed | {o['delivered_pct']}% delivered | "
        f"{o['cancelled_pct']}% cancelled {o['cancelled']}",
        f"  delivery time avg {o['avg_delivery_min']} min, p90 {o['p90_delivery_min']} min | "
        f"SLA breach {o['sla_breach_pct']}% | GMV Rs {o['gmv_inr']:,} | AOV Rs {o['aov_inr']}",
        "",
        "Orders by IST hour:",
    ]
    peak = max(s["orders_by_ist_hour"].values(), default=1)
    for h, n in s["orders_by_ist_hour"].items():
        lines.append(f"  {h:02d}:00 {'#' * max(1, round(40 * n / peak)):<40} {n:,}")
    lines += ["", "Orders by zone:"]
    lines += [f"  {z:<16}{n:>8,}" for z, n in s["orders_by_zone"].items()]
    lines += ["", "Weather changes:"] + [f"  {w}" for w in s["weather_changes"][:20]]
    c = s["chaos_observed"]
    lines += [
        "",
        "Chaos observed in the stream:",
        f"  duplicate copies   {c['duplicate_copies']:>8,}  ({c['duplicate_pct']}% of unique events)",
        f"  late > 60s         {c['late_gt_60s']:>8,}  ({c['late_pct_of_driver_app']}% of driver_app events)",
        f"  very late > 1h     {c['very_late_gt_1h']:>8,}",
        f"  out-of-order       {c['out_of_order']:>8,}  ({c['out_of_order_pct']}% of unique events, any cause)",
    ]
    if s["chaos_injected"]:
        lines += ["Chaos injected (manifest):"]
        lines += [f"  {k:<18} {v:>8,}" for k, v in sorted(s["chaos_injected"].items())]
    return "\n".join(lines)


def report_dir(path: Path) -> str:
    """Events within each file are in emit order; merge files by emitted_ts for the report."""
    events = sorted(iter_events(path), key=lambda e: e["emitted_ts"])
    return format_report(summarize(events, load_manifest(path)))
