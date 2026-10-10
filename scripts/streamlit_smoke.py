"""Run every dashboard query as FOODDEL_BI (the app's owner) and report row counts.

Proves the app's data access works under governance before anyone opens it.

    python scripts/streamlit_smoke.py
"""

from __future__ import annotations

import os
import sys

from _env import ROOT, load_dotenv

sys.path.insert(0, str(ROOT / "streamlit"))
# Queries that may legitimately be empty (no open orders past promise, no alerts fired).
MAY_BE_EMPTY = {"at_risk", "alerts"}


def main() -> int:
    load_dotenv()
    import sf
    from queries import QUERIES

    db = os.environ["SNOWFLAKE_DATABASE"]
    failures = 0
    with sf.connect() as conn, conn.cursor() as cur:
        cur.execute("USE ROLE FOODDEL_BI")
        cur.execute("USE SECONDARY ROLES NONE")
        cur.execute("USE WAREHOUSE WH_BI")
        for name, sql in QUERIES.items():
            try:
                cur.execute(sql.format(db=db))
                rows = len(cur.fetchall())
                ok = rows > 0 or name in MAY_BE_EMPTY
                print(f"{'PASS' if ok else 'FAIL'}  {name:<16} {rows:>6} rows")
            except Exception as e:  # noqa: BLE001 - report every query
                ok = False
                print(f"FAIL  {name:<16} {str(e).splitlines()[0][:110]}")
            failures += not ok
    print(f"\n{len(QUERIES) - failures}/{len(QUERIES)} dashboard queries OK as FOODDEL_BI")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
