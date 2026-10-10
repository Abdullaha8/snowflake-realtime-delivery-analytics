"""Cost report from SNOWFLAKE.ACCOUNT_USAGE -> docs/cost_report.md.

Credits by service and by warehouse, compute attributed to each workload (query tag),
Dynamic Table refresh outcomes, storage by schema (incl. Time Travel / Fail-safe), and
recommendations. ACCOUNT_USAGE lags up to ~3 hours, so the last runs may be missing.

    python scripts/cost_report.py [--days 7]
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date

from _env import ROOT, load_dotenv

QUERIES = {
    "by_service": """
        SELECT SERVICE_TYPE, ROUND(SUM(CREDITS_USED), 4) AS CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
        WHERE USAGE_DATE >= DATEADD('day', -{days}, CURRENT_DATE())
        GROUP BY 1 HAVING SUM(CREDITS_USED) > 0 ORDER BY 2 DESC""",
    "by_warehouse": """
        SELECT WAREHOUSE_NAME, ROUND(SUM(CREDITS_USED), 4) AS CREDITS,
               COUNT(DISTINCT DATE_TRUNC('hour', START_TIME)) AS ACTIVE_HOURS
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE START_TIME >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        GROUP BY 1 ORDER BY 2 DESC""",
    "by_workload": """
        SELECT COALESCE(NULLIF(QUERY_TAG, ''), '(untagged)') AS WORKLOAD,
               COUNT(*) AS QUERIES,
               ROUND(SUM(CREDITS_ATTRIBUTED_COMPUTE), 4) AS CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY
        WHERE START_TIME >= DATEADD('day', -{days}, CURRENT_TIMESTAMP())
        GROUP BY 1 ORDER BY 3 DESC""",
    "dt_refreshes": """
        SELECT SCHEMA_NAME || '.' || NAME AS DYNAMIC_TABLE, STATE, COUNT(*) AS REFRESHES
        FROM TABLE({db}.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
            DATA_TIMESTAMP_START => DATEADD('hour', -{dt_hours}, CURRENT_TIMESTAMP())))
        GROUP BY 1, 2 ORDER BY 1, 2""",
    "storage": """
        SELECT TABLE_SCHEMA,
               ROUND(SUM(ACTIVE_BYTES) / POWER(1024, 2), 1) AS ACTIVE_MB,
               ROUND(SUM(TIME_TRAVEL_BYTES) / POWER(1024, 2), 1) AS TIME_TRAVEL_MB,
               ROUND(SUM(FAILSAFE_BYTES) / POWER(1024, 2), 1) AS FAILSAFE_MB
        FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
        WHERE TABLE_CATALOG = '{db}' AND DELETED IS NULL
        GROUP BY 1 ORDER BY 2 DESC""",
    "monitors": """SHOW RESOURCE MONITORS LIKE 'RM_%'""",
}

RECOMMENDATIONS = """\
## Recommendations

**Already applied (and why):**
- **Warehouses:** all XSMALL, `AUTO_SUSPEND = 60`, created suspended, with a resource monitor
  each (5 credits) plus 20 credits account-wide. Nothing in this workload needs more than an
  XSMALL: the largest job, `dbt build` over 230k orders, finishes in ~11 s.
- **Ingestion:** Snowpipe Streaming is serverless and billed per GB, so streaming needs no
  warehouse.
- **Dynamic Tables:**
  - `REFRESH_MODE = INCREMENTAL` is forced, so a FULL refresh can never happen silently.
  - SILVER is `DOWNSTREAM`, so it refreshes only when GOLD needs it.
  - GOLD lag is 10 minutes; refreshes with no new data are SKIPPED (see the table above).
- **Airflow:** the pipeline short-circuits on metadata (`SHOW TABLES`), so an hour with no new
  data costs **0 credits**.
- **Alerts, Dynamic Tables, connector:** Alerts are created suspended; `make stop` suspends
  Alerts and Dynamic Tables and deletes the connector.
- **Storage:**
  - dbt tables are TRANSIENT (dbt-snowflake default), so they have no Fail-safe.
  - BRONZE `DATA_RETENTION_TIME_IN_DAYS = 1` keeps Time Travel short on the largest tables.

**Found in this report:**
- `COMPUTE_WH`, the trial's default warehouse, still shows small usage (Snowsight worksheets).
  Suspend it or set `AUTO_SUSPEND = 60` on it too, so ad-hoc work runs on WH_BI with a monitor.
- `(untagged)` queries were Airflow's and ad-hoc ones. The Airflow connection now sets
  `QUERY_TAG = airflow` (scripts/gen_airflow_env.py), so pipeline compute is attributable.

**Next, if usage grows:**
1. **A Snowflake Budget** for serverless spend (Snowpipe Streaming, serverless alerts).
   Resource monitors only cap warehouses.
2. **Keep demo lag (1 minute) for demos only.** At a 1-minute GOLD lag WH_TRANSFORM stays warm
   (~1 credit/h) instead of ~0.2–0.3 credits/h at 10 minutes. `make dt-normal` restores it.
3. **Partition BRONZE by time for pruning** before it reaches hundreds of millions of rows: a
   clustering key on event date, or a `RECORD_METADATA` push-date column. Not worth it at 1.4 M
   rows.
4. **Move the hourly DAG to "on new data"** (Snowflake streams / Airflow asset triggers) if
   ingestion becomes bursty.
5. **Scale out (multi-cluster), not up**, for many concurrent dashboard users on WH_BI;
   queries are small.
"""


def md_table(cur) -> str:  # noqa: ANN001
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    if not rows:
        return "_no rows yet (ACCOUNT_USAGE lags up to ~3 h)_\n"
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    out += ["| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows]
    return "\n".join(out) + "\n"


def main(argv: list[str]) -> int:
    load_dotenv()
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=7)
    args = p.parse_args(argv)
    import sf

    db = os.environ["SNOWFLAKE_DATABASE"]
    sections = {
        "by_service": "Credits by service",
        "by_warehouse": "Credits by warehouse",
        "by_workload": "Compute credits by workload (query tag)",
        "dt_refreshes": "Dynamic Table refreshes (SKIPPED = no new data, no cost)",
        "storage": f"Storage by schema ({db})",
    }
    parts = [
        f"# Cost report - {db}\n",
        f"_Generated {date.today()} by `make cost-report` from SNOWFLAKE.ACCOUNT_USAGE (last {args.days} "
        "days; this view lags up to ~3 hours)._\n",
    ]
    with sf.connect() as conn, conn.cursor() as cur:
        cur.execute("USE WAREHOUSE WH_INGEST")
        cur.execute(QUERIES["monitors"])
        names = [d[0] for d in cur.description]
        mons = [dict(zip(names, r, strict=True)) for r in cur.fetchall()]
        parts.append("## Resource monitors (credits used this month / quota)\n")
        parts.append("| monitor | used | quota |\n|---|---|---|\n")
        parts += [f"| {m['name']} | {m['used_credits']} | {m['credit_quota']} |\n" for m in mons]
        for key, title in sections.items():
            # the DT refresh-history function only reaches back 7 days
            dt_hours = min(args.days * 24, 167)
            cur.execute(QUERIES[key].format(days=args.days, db=db, dt_hours=dt_hours))
            parts.append(f"\n## {title}\n\n")
            parts.append(md_table(cur))
    parts.append("\n" + RECOMMENDATIONS)
    out = ROOT / "docs" / "cost_report.md"
    out.write_text("".join(parts), encoding="utf-8")
    print(f"wrote {out.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
