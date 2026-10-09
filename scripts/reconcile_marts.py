"""Reconcile the dbt MARTS against the backfill's ground truth.

The backfill (make sim-backfill, seed 2026) is the only data in BRONZE after
`make sf-reset-bronze && make load-backfill`. Python recomputes the expected numbers straight
from data/backfill/*.jsonl.gz (dedup by event_id, IST dates); Snowflake answers the same
questions from MARTS. Every metric must match exactly.

python scripts/reconcile_marts.py [--data data/backfill]
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

from _env import ROOT, load_dotenv

IST = timezone(timedelta(hours=5, minutes=30))


def _ist(ts: str) -> datetime:
    return datetime.fromisoformat(ts).astimezone(UTC).astimezone(IST)


def compute_truth(events) -> dict:  # noqa: ANN001 - iterable of event dicts
    seen: set[str] = set()
    placed: dict[str, dict] = {}
    placed_month: dict[str, str] = {}
    delivered: dict[str, float] = {}
    cancelled: dict[str, str] = {}
    signups = 0
    for ev in events:
        if ev["event_id"] in seen:
            continue
        seen.add(ev["event_id"])
        et, d = ev["event_type"], ev["data"]
        if et == "ORDER_PLACED":
            placed[d["order_id"]] = d
            placed_month[d["order_id"]] = _ist(ev["event_ts"]).strftime("%Y-%m")
        elif et == "ORDER_DELIVERED":
            delivered[d["order_id"]] = d["actual_delivery_min"]
        elif et == "ORDER_CANCELLED":
            cancelled[d["order_id"]] = d["reason"]
        elif et == "CUSTOMER_REGISTERED":
            signups += 1

    delivered_placed = [o for o in delivered if o in placed]
    gmv = sum(placed[o]["total_inr"] for o in delivered_placed)
    first_month: dict[str, str] = {}
    active: dict[str, set[str]] = defaultdict(set)
    for oid, d in placed.items():
        month, cust = placed_month[oid], d["customer_id"]
        first_month[cust] = min(first_month.get(cust, month), month)
        active[cust].add(month)

    truth: dict[str, object] = {
        "orders placed": len(placed),
        "orders delivered": len(delivered_placed),
        "orders cancelled": sum(1 for o in cancelled if o in placed),
        "sla breaches": sum(
            1 for o in delivered_placed if delivered[o] > placed[o]["promised_eta_min"]
        ),
        "gmv (INR)": gmv,
        "aov (INR)": round(gmv / len(delivered_placed), 2) if delivered_placed else None,
        "customers registered": signups,
        "customers who ordered": len(first_month),
    }
    for reason, n in sorted(Counter(r for o, r in cancelled.items() if o in placed).items()):
        truth[f"cancel reason={reason}"] = n
    for month, n in sorted(Counter(placed_month.values()).items()):
        truth[f"orders month={month}"] = n
    for zone, n in sorted(Counter(d["zone_id"] for d in placed.values()).items()):
        truth[f"orders zone={zone}"] = n
    for month, n in sorted(Counter(first_month.values()).items()):
        truth[f"cohort size {month}"] = n
    # month-1 retention per cohort: customers active again in the following month
    for cohort in sorted(set(first_month.values())):
        y, m = map(int, cohort.split("-"))
        nxt = f"{y + (m == 12)}-{(m % 12) + 1:02d}"
        members = [c for c, fm in first_month.items() if fm == cohort]
        truth[f"cohort {cohort} active in month+1"] = sum(1 for c in members if nxt in active[c])
    return truth


def snowflake_actuals(cur, db: str) -> dict:  # noqa: ANN001
    m = f"{db}.MARTS"

    def one(sql: str) -> object:
        cur.execute(sql)
        return cur.fetchone()[0]

    a: dict[str, object] = {
        "orders placed": one(f"SELECT COUNT(*) FROM {m}.FCT_ORDERS"),
        "orders delivered": one(f"SELECT COUNT_IF(IS_DELIVERED) FROM {m}.FCT_ORDERS"),
        "orders cancelled": one(f"SELECT COUNT_IF(IS_CANCELLED) FROM {m}.FCT_ORDERS"),
        "sla breaches": one(f"SELECT SUM(SLA_BREACHES) FROM {m}.MART_DAILY_KPIS"),
        "gmv (INR)": one(f"SELECT SUM(GMV_INR) FROM {m}.MART_DAILY_KPIS"),
        "aov (INR)": one(
            f"SELECT ROUND(SUM(GMV_INR) / SUM(ORDERS_DELIVERED), 2) FROM {m}.MART_DAILY_KPIS"
        ),
        "customers registered": one(
            f"SELECT COUNT_IF(SIGNED_UP_AT_IST IS NOT NULL) FROM {m}.DIM_CUSTOMER"
        ),
        "customers who ordered": one(f"SELECT COUNT_IF(HAS_ORDERED) FROM {m}.DIM_CUSTOMER"),
    }
    cur.execute(f"SELECT CANCEL_REASON, COUNT(*) FROM {m}.FCT_ORDERS WHERE IS_CANCELLED GROUP BY 1")
    a.update({f"cancel reason={r}": n for r, n in cur.fetchall()})
    cur.execute(
        f"SELECT TO_CHAR(DATE_DAY, 'YYYY-MM'), SUM(ORDERS_PLACED) FROM {m}.MART_DAILY_KPIS GROUP BY 1"
    )
    a.update({f"orders month={mo}": n for mo, n in cur.fetchall()})
    cur.execute(f"SELECT ZONE_ID, SUM(ORDERS_PLACED) FROM {m}.MART_DAILY_KPIS GROUP BY 1")
    a.update({f"orders zone={z}": n for z, n in cur.fetchall()})
    cur.execute(
        f"SELECT TO_CHAR(COHORT_MONTH, 'YYYY-MM'), MONTHS_SINCE_FIRST_ORDER, COHORT_CUSTOMERS, "
        f"ACTIVE_CUSTOMERS FROM {m}.MART_COHORT_RETENTION WHERE MONTHS_SINCE_FIRST_ORDER <= 1"
    )
    for cohort, months, size, act in cur.fetchall():
        if months == 0:
            a[f"cohort size {cohort}"] = size
        else:
            a[f"cohort {cohort} active in month+1"] = act
    return {k: (float(v) if k == "aov (INR)" and v is not None else int(v) if v is not None else None)
            for k, v in a.items()}  # fmt: skip


def main(argv: list[str]) -> int:
    load_dotenv()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--data", default=str(ROOT / "data" / "backfill"))
    args = p.parse_args(argv)

    import reconcile
    import sf

    from simulator.report import iter_events

    print(f"1/2 computing ground truth from {args.data} ...")
    truth = compute_truth(iter_events(Path(args.data)))
    # months with orders but no later month observed have no month+1 row in the mart
    truth = {k: v for k, v in truth.items() if not (k.endswith("month+1") and v == 0)}
    print("2/2 querying MARTS ...")
    with sf.connect() as conn, conn.cursor() as cur:
        cur.execute("USE ROLE FOODDEL_TRANSFORM")
        cur.execute("USE WAREHOUSE WH_TRANSFORM")
        actual = snowflake_actuals(cur, os.environ["SNOWFLAKE_DATABASE"])

    rows = reconcile.compare(truth, actual)
    width = max(len(k) for k, *_ in rows)
    print(f"\n  {'metric':<{width}}  {'truth':>12}  {'marts':>12}  result")
    for k, t, a, ok in rows:
        print(f"  {k:<{width}}  {t!s:>12}  {a!s:>12}  {'PASS' if ok else 'FAIL'}")
    failed = sum(1 for *_, ok in rows if not ok)
    print(f"\n{len(rows) - failed}/{len(rows)} metrics match")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
