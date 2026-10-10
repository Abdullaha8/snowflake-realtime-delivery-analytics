"""Governance access matrix: what each role actually sees, checked against what it should.

For every role: is PII masked in MARTS_SECURE.V_DIM_CUSTOMER, which zones / how many orders are
visible in V_FCT_ORDERS, and is direct access to the MARTS tables denied? Secondary roles are
switched off so each role is tested on its own.

    python scripts/governance_check.py
"""

from __future__ import annotations

import os
import sys

from _env import load_dotenv

WEST = {"HINJEWADI", "WAKAD", "BANER", "AUNDH", "KOTHRUD", "SHIVAJINAGAR"}
EAST = {"CAMP", "KOREGAON_PARK", "KALYANI_NAGAR", "VIMAN_NAGAR", "MAGARPATTA", "HADAPSAR"}
ALL = WEST | EAST
# role -> (warehouse, PII visible?, zones visible, direct MARTS access?)
EXPECTED = {
    "FOODDEL_TRANSFORM": ("WH_TRANSFORM", True, ALL, True),
    "FOODDEL_BI": ("WH_BI", False, ALL, False),
    "FOODDEL_PII_READER": ("WH_BI", True, ALL, False),
    "FOODDEL_OPS_WEST": ("WH_BI", False, WEST, False),
    "FOODDEL_OPS_EAST": ("WH_BI", False, EAST, False),
}


def observe(cur, db: str, role: str, warehouse: str) -> dict:  # noqa: ANN001
    from snowflake.connector.errors import ProgrammingError

    cur.execute(f"USE ROLE {role}")
    cur.execute("USE SECONDARY ROLES NONE")
    cur.execute(f"USE WAREHOUSE {warehouse}")
    cur.execute(
        f"SELECT EMAIL, PHONE, FULL_NAME FROM {db}.MARTS_SECURE.V_DIM_CUSTOMER"
        " WHERE EMAIL IS NOT NULL ORDER BY CUSTOMER_ID LIMIT 1"
    )
    email, phone, name = cur.fetchone()
    cur.execute(f"SELECT DISTINCT ZONE_ID FROM {db}.MARTS_SECURE.V_FCT_ORDERS")
    zones = {r[0] for r in cur.fetchall()}
    cur.execute(f"SELECT COUNT(*), SUM(GMV_INR) FROM {db}.MARTS_SECURE.V_FCT_ORDERS")
    orders, gmv = cur.fetchone()
    try:
        cur.execute(f"SELECT 1 FROM {db}.MARTS.FCT_ORDERS LIMIT 1")
        direct = True
    except ProgrammingError:
        direct = False
    return {
        "sample": f"{email} | {phone} | {name}",
        "pii_visible": "***" not in email,
        "zones": zones,
        "orders": int(orders),
        "gmv": int(gmv or 0),
        "direct": direct,
    }


def main() -> int:
    load_dotenv()
    import sf

    db = os.environ["SNOWFLAKE_DATABASE"]
    results, failures = {}, 0
    with sf.connect() as conn, conn.cursor() as cur:
        for role, (wh, pii, zones, direct) in EXPECTED.items():
            got = observe(cur, db, role, wh)
            ok = got["pii_visible"] == pii and got["zones"] == zones and got["direct"] == direct
            failures += not ok
            results[role] = got
            print(
                f"{'PASS' if ok else 'FAIL'}  {role:<19} PII {'clear ' if got['pii_visible'] else 'masked'}"
                f"  zones {len(got['zones']):>2}  orders {got['orders']:>7,}"
                f"  direct MARTS {'allowed' if got['direct'] else 'denied '}  e.g. {got['sample']}"
            )
    total = results["FOODDEL_BI"]["orders"]
    split = results["FOODDEL_OPS_WEST"]["orders"] + results["FOODDEL_OPS_EAST"]["orders"]
    ok = split == total
    failures += not ok
    print(f"{'PASS' if ok else 'FAIL'}  west + east orders = {split:,} vs all-zones {total:,}")
    print(f"\n{len(EXPECTED) + 1 - failures}/{len(EXPECTED) + 1} governance checks pass")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
