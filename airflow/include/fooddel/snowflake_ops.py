"""Snowflake helpers for the fooddel_pipeline DAG (connection = Airflow conn snowflake_default)."""

from __future__ import annotations

import os

DB = os.environ.get("SNOWFLAKE_DATABASE", "FOODDEL_DEV")
BRONZE_TABLES = ("ORDERS_RAW", "DRIVER_LOCATIONS_RAW", "REFERENCE_RAW")
GOLD_TARGET_LAG_S = 600


def _rows(cur) -> list[dict]:  # noqa: ANN001
    names = [d[0] for d in cur.description]
    return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]


def bronze_row_count(cur) -> int:  # noqa: ANN001
    """Total BRONZE rows from metadata (SHOW TABLES): runs on cloud services, no warehouse."""
    cur.execute(f"SHOW TABLES IN SCHEMA {DB}.BRONZE")
    return sum(int(r["rows"] or 0) for r in _rows(cur) if r["name"] in BRONZE_TABLES)


def dynamic_tables(cur) -> list[dict]:  # noqa: ANN001
    cur.execute(f"SHOW DYNAMIC TABLES IN DATABASE {DB}")
    return _rows(cur)


def refresh_dynamic_tables(cur) -> list[str]:  # noqa: ANN001
    """Refresh every Dynamic Table with its own lag; each pulls its DOWNSTREAM inputs along.
    Works whether or not the tables are suspended, so the pipeline - not the DT scheduler -
    decides when SILVER/GOLD move forward."""
    refreshed = []
    for d in dynamic_tables(cur):
        if str(d["target_lag"]).upper() == "DOWNSTREAM":
            continue
        name = f"{DB}.{d['schema_name']}.{d['name']}"
        cur.execute(f"ALTER DYNAMIC TABLE {name} REFRESH")
        refreshed.append(f"{d['schema_name']}.{d['name']}")
    return refreshed


def gold_data_lag_seconds(cur) -> float | None:  # noqa: ANN001
    """now - data timestamp of the stalest GOLD Dynamic Table (worst case)."""
    lags = []
    cur.execute("SELECT CURRENT_TIMESTAMP()")
    now = cur.fetchone()[0]
    for d in dynamic_tables(cur):
        if d["schema_name"] == "GOLD" and d.get("data_timestamp"):
            lags.append((now - d["data_timestamp"]).total_seconds())
    return max(lags) if lags else None
