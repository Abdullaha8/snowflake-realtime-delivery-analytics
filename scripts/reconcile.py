"""Reconcile SILVER/GOLD against simulator ground truth.

One deterministic simulation is produced to Kafka AND kept in memory. Python computes the
expected numbers from the in-memory copy (dedup by event_id, status by event precedence); then,
once the connector has landed every row in BRONZE, the Dynamic Tables are refreshed and the
same numbers are queried from Snowflake. Every metric must match exactly.

Preconditions: `make up`, `make schemas-register`, `make connector-up`, and an empty BRONZE
(`make sf-reset-bronze`) so the run is not mixed with older data.

python scripts/reconcile.py [--seed 77] [--start 2026-10-09T19:00] [--hours 2]
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import Counter

from _env import load_dotenv

STATUS_PRECEDENCE = [
    ("ORDER_DELIVERED", "DELIVERED"),
    ("ORDER_CANCELLED", "CANCELLED"),
    ("ORDER_PICKED_UP", "PICKED_UP"),
    ("DRIVER_ASSIGNED", "DRIVER_ASSIGNED"),
    ("ORDER_ACCEPTED", "ACCEPTED"),
]
TOPIC_TABLES = {
    "fooddel.orders.v1": ("ORDERS_RAW", "ORDER_EVENTS"),
    "fooddel.driver_locations.v1": ("DRIVER_LOCATIONS_RAW", "DRIVER_PINGS"),
    "fooddel.reference.v1": ("REFERENCE_RAW", "REFERENCE_EVENTS"),
}


# -- ground truth (pure, unit-tested) -------------------------------------------------------
def compute_truth(events: list, manifest: list[dict]) -> dict:
    """events: OutEvent list in emit order (duplicates included); manifest: anomaly records."""
    from simulator.report import summarize

    emitted = Counter(e.topic for e in events)
    unique_ids: dict[str, set[str]] = {t: set() for t in TOPIC_TABLES}
    types_by_order: dict[str, set[str]] = {}
    promised: dict[str, int] = {}
    delivered_min: dict[str, float] = {}
    for e in events:
        unique_ids[e.topic].add(e.value["event_id"])
        if e.topic == "fooddel.orders.v1":
            et, d = e.value["event_type"], e.value["data"]
            types_by_order.setdefault(e.key, set()).add(et)
            if et == "ORDER_PLACED":
                promised[e.key] = d["promised_eta_min"]
            elif et == "ORDER_DELIVERED":
                delivered_min[e.key] = d["actual_delivery_min"]

    status = Counter()
    for types in types_by_order.values():
        status[next((s for t, s in STATUS_PRECEDENCE if t in types), "PLACED")] += 1

    s = summarize([e.value for e in events], manifest)
    o = s["orders"]
    dup_by_topic = Counter(m["topic"] for m in manifest if m["anomaly"] == "DUPLICATE")
    breaches = sum(1 for oid, m in delivered_min.items() if oid in promised and m > promised[oid])
    truth = {}
    for topic in TOPIC_TABLES:
        truth[f"bronze rows {topic}"] = emitted[topic]
        truth[f"silver unique events {topic}"] = len(unique_ids[topic])
        truth[f"duplicates removed {topic}"] = dup_by_topic[topic]
    truth["orders placed"] = o["placed"]
    for st in ("DELIVERED", "CANCELLED", "PICKED_UP", "DRIVER_ASSIGNED", "ACCEPTED", "PLACED"):
        truth[f"orders status={st}"] = status[st]
    for reason, n in sorted(o["cancelled"].items()):
        truth[f"cancel reason={reason}"] = n
    truth["sla breaches"] = breaches
    truth["gmv delivered (INR)"] = o["gmv_inr"]
    for zone, n in sorted(s["orders_by_zone"].items()):
        truth[f"orders zone={zone}"] = n
    # GOLD tables must add back up to the SILVER totals
    truth["GOLD zone_demand_supply orders"] = o["placed"]
    truth["GOLD sla_breaches rows"] = breaches
    truth["GOLD restaurant_perf orders"] = o["placed"]
    truth["GOLD hex_demand orders"] = o["placed"]
    return truth


def compare(truth: dict, actual: dict) -> list[tuple[str, object, object, bool]]:
    return [(k, v, actual.get(k), actual.get(k) == v) for k, v in truth.items()]


# -- Snowflake side -------------------------------------------------------------------------
def bronze_counts(cur, db: str) -> dict[str, int]:  # noqa: ANN001
    cur.execute(f"SHOW TABLES IN SCHEMA {db}.BRONZE")  # metadata only: no warehouse
    names = [d[0] for d in cur.description]
    rows = {r[names.index("name")]: r[names.index("rows")] for r in cur.fetchall()}
    return {topic: rows.get(raw, 0) for topic, (raw, _) in TOPIC_TABLES.items()}


def snowflake_actuals(cur, db: str) -> dict:  # noqa: ANN001
    def one(sql: str) -> object:
        cur.execute(sql)
        return cur.fetchone()[0]

    actual = {}
    for topic, (raw, silver) in TOPIC_TABLES.items():
        bronze = one(f"SELECT COUNT(*) FROM {db}.BRONZE.{raw}")
        unique = one(f"SELECT COUNT(*) FROM {db}.SILVER.{silver}")
        actual[f"bronze rows {topic}"] = bronze
        actual[f"silver unique events {topic}"] = unique
        actual[f"duplicates removed {topic}"] = bronze - unique
    orders = f"{db}.SILVER.ORDERS"
    actual["orders placed"] = one(f"SELECT COUNT_IF(HAS_PLACED) FROM {orders}")
    cur.execute(f"SELECT ORDER_STATUS, COUNT(*) FROM {orders} GROUP BY 1")
    for st, n in cur.fetchall():
        actual[f"orders status={st}"] = n
    for st in ("DELIVERED", "CANCELLED", "PICKED_UP", "DRIVER_ASSIGNED", "ACCEPTED", "PLACED"):
        actual.setdefault(f"orders status={st}", 0)
    cur.execute(
        f"SELECT CANCEL_REASON, COUNT(*) FROM {orders} WHERE CANCEL_REASON IS NOT NULL GROUP BY 1"
    )
    for reason, n in cur.fetchall():
        actual[f"cancel reason={reason}"] = n
    actual["sla breaches"] = one(f"SELECT COUNT_IF(SLA_BREACHED AND HAS_PLACED) FROM {orders}")
    actual["gmv delivered (INR)"] = one(
        f"SELECT COALESCE(SUM(TOTAL_INR), 0) FROM {orders} "
        "WHERE ORDER_STATUS = 'DELIVERED' AND HAS_PLACED"
    )
    cur.execute(f"SELECT ZONE_ID, COUNT(*) FROM {orders} WHERE HAS_PLACED GROUP BY 1")
    for zone, n in cur.fetchall():
        actual[f"orders zone={zone}"] = n
    gold = f"{db}.GOLD"
    actual["GOLD zone_demand_supply orders"] = one(
        f"SELECT COALESCE(SUM(ORDERS_PLACED), 0) FROM {gold}.ZONE_DEMAND_SUPPLY_5M"
    )
    actual["GOLD sla_breaches rows"] = one(f"SELECT COUNT(*) FROM {gold}.SLA_BREACHES")
    actual["GOLD restaurant_perf orders"] = one(
        f"SELECT COALESCE(SUM(ORDERS_PLACED), 0) FROM {gold}.RESTAURANT_PERF_HOURLY"
    )
    actual["GOLD hex_demand orders"] = one(
        f"SELECT COALESCE(SUM(ORDERS_PLACED), 0) FROM {gold}.HEX_DEMAND_HOURLY"
    )
    return {k: int(v) if v is not None else None for k, v in actual.items()}


# -- run ------------------------------------------------------------------------------------
class TeeSink:
    """Send every event to Kafka and keep it in memory as ground truth."""

    def __init__(self, kafka_sink) -> None:  # noqa: ANN001
        from simulator.sinks import ListSink

        self.kafka, self.memory = kafka_sink, ListSink()

    def write(self, ev) -> None:  # noqa: ANN001
        self.kafka.write(ev)
        self.memory.write(ev)

    def close(self) -> None:
        self.kafka.close()


def main(argv: list[str]) -> int:
    load_dotenv()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--seed", type=int, default=77)
    p.add_argument("--start", default="2026-10-09T19:00", help="simulated start (IST)")
    p.add_argument("--hours", type=float, default=2.0)
    p.add_argument("--timeout", type=int, default=600, help="seconds to wait for BRONZE")
    p.add_argument(
        "--no-produce",
        action="store_true",
        help="re-check an earlier run: rebuild the same ground truth (deterministic) but do not "
        "produce again",
    )
    args = p.parse_args(argv)

    import connect
    import sf

    from simulator.config import SimConfig
    from simulator.engine import Simulator
    from simulator.registry import DEFAULT_URL
    from simulator.sinks import KafkaSink, ListSink, ManifestWriter, avro_serializer

    code, status = connect.call("GET", f"/connectors/{connect.NAME}/status")
    if code != 200 or status["connector"]["state"] != "RUNNING":
        sys.exit("connector is not RUNNING: run `make up` and `make connector-up` first")
    db = os.environ["SNOWFLAKE_DATABASE"]
    with sf.connect() as conn, conn.cursor() as cur:
        before = bronze_counts(cur, db)
    if any(before.values()) and not args.no_produce:
        sys.exit(f"BRONZE is not empty {before}: run `make sf-reset-bronze` first (DEV only)")

    cfg = SimConfig(
        mode="backfill", start=args.start, days=args.hours / 24, seed=args.seed, emit_pings=True
    )
    manifest = ManifestWriter(None)
    if args.no_produce:
        print(f"1/4 rebuilding ground truth for seed={args.seed} (not producing) ...")
        memory = ListSink()
        Simulator(cfg, memory, run_id=f"reconcile-s{args.seed}", manifest=manifest).run()
        events = memory.events
    else:
        print(f"1/4 producing seed={args.seed}, {args.hours} sim-hours from {args.start} ...")
        kafka = KafkaSink(
            os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            serializer=avro_serializer(os.environ.get("SCHEMA_REGISTRY_URL", DEFAULT_URL)),
        )
        sink = TeeSink(kafka)
        Simulator(cfg, sink, run_id=f"reconcile-s{args.seed}", manifest=manifest).run()
        events = sink.memory.events
    truth = compute_truth(events, manifest.records)
    expected = {t: truth[f"bronze rows {t}"] for t in TOPIC_TABLES}
    print(f"    expecting {sum(expected.values()):,} messages {expected}")

    print("2/4 waiting for the connector to land everything in BRONZE ...")
    deadline = time.time() + args.timeout
    with sf.connect() as conn, conn.cursor() as cur:
        while True:
            got = bronze_counts(cur, db)
            if all(got[t] >= expected[t] for t in TOPIC_TABLES):
                break
            if time.time() > deadline:
                sys.exit(f"timed out: BRONZE {got} vs expected {expected}")
            time.sleep(10)
        print(f"    BRONZE {got}")

        print("3/4 refreshing Dynamic Tables (own-lag tables pull their DOWNSTREAM inputs) ...")
        cur.execute("USE ROLE FOODDEL_TRANSFORM")
        cur.execute("USE WAREHOUSE WH_TRANSFORM")
        for d in sf.refresh_targets(sf.list_dts(cur)):
            cur.execute(f"ALTER DYNAMIC TABLE {db}.{d['schema_name']}.{d['name']} REFRESH")

        print("4/4 comparing ...")
        actual = snowflake_actuals(cur, db)

    rows = compare(truth, actual)
    width = max(len(k) for k, *_ in rows)
    print(f"\n  {'metric':<{width}}  {'truth':>12}  {'snowflake':>12}  result")
    for k, t, a, ok in rows:
        print(f"  {k:<{width}}  {t!s:>12}  {a!s:>12}  {'PASS' if ok else 'FAIL'}")
    failed = sum(1 for *_, ok in rows if not ok)
    print(f"\n{len(rows) - failed}/{len(rows)} metrics match")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
