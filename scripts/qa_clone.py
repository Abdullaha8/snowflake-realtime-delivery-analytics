"""Zero-copy clone FOODDEL_DEV -> FOODDEL_QA, plus a Time Travel restore demo in the clone.

    python scripts/qa_clone.py           clone, suspend the clone's DTs/alerts, run the demo
    python scripts/qa_clone.py --drop    drop FOODDEL_QA

A clone shares DEV's storage until either side changes data, so it is created in seconds
and costs ~nothing to keep for a short time. Cloned Dynamic Tables would start refreshing
on their own schedule (credits), so they are suspended immediately.
"""

from __future__ import annotations

import os
import sys
import time

from _env import load_dotenv

QA = "FOODDEL_QA"


def main(argv: list[str]) -> int:
    load_dotenv()
    import sf

    dev = os.environ["SNOWFLAKE_DATABASE"]
    with sf.connect() as conn, conn.cursor() as cur:
        cur.execute("USE ROLE SYSADMIN")
        if "--drop" in argv:
            cur.execute(f"DROP DATABASE IF EXISTS {QA}")
            print(f"dropped {QA}")
            return 0

        t0 = time.time()
        cur.execute(f"CREATE OR REPLACE DATABASE {QA} CLONE {dev}")
        print(f"1. cloned {dev} -> {QA} in {time.time() - t0:.1f}s (zero-copy: no data copied)")

        suspended = 0
        cur.execute(f"SHOW DYNAMIC TABLES IN DATABASE {QA}")
        names = [d[0] for d in cur.description]
        for row in cur.fetchall():
            d = dict(zip(names, row, strict=True))
            cur.execute(f"ALTER DYNAMIC TABLE {QA}.{d['schema_name']}.{d['name']} SUSPEND")
            suspended += 1
        cur.execute(f"SHOW ALERTS IN DATABASE {QA}")
        names = [d[0] for d in cur.description]
        for row in cur.fetchall():
            d = dict(zip(names, row, strict=True))
            cur.execute(f"ALTER ALERT {QA}.{d['schema_name']}.{d['name']} SUSPEND")
            suspended += 1
        print(
            f"2. suspended {suspended} Dynamic Tables / Alerts in the clone (no surprise credits)"
        )

        cur.execute("USE WAREHOUSE WH_TRANSFORM")
        fct = f"{QA}.MARTS.FCT_ORDERS"
        cur.execute(f"SELECT COUNT(*) FROM {fct}")
        before = cur.fetchone()[0]
        cur.execute(f"DELETE FROM {fct} WHERE ZONE_ID = 'KOTHRUD'")
        deleted = cur.fetchone()[0]
        delete_qid = cur.sfqid
        cur.execute(f"SELECT COUNT(*) FROM {fct}")
        after_delete = cur.fetchone()[0]
        print(
            f"3. 'accident' in the clone: deleted {deleted:,} KOTHRUD orders ({before:,} -> {after_delete:,})"
        )

        # Time Travel: read the table as it was just before that DELETE and put the rows back.
        cur.execute(
            f"INSERT INTO {fct} SELECT * FROM {fct} BEFORE (STATEMENT => '{delete_qid}')"
            " WHERE ZONE_ID = 'KOTHRUD'"
        )
        cur.execute(f"SELECT COUNT(*) FROM {fct}")
        restored = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM {dev}.MARTS.FCT_ORDERS")
        dev_rows = cur.fetchone()[0]
        ok = restored == before == dev_rows
        print(
            f"4. Time Travel restore: {restored:,} rows (before {before:,}; DEV untouched: {dev_rows:,})"
        )
        print("PASS" if ok else "FAIL", f"- run `make qa-drop` when done with {QA}")
        return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
