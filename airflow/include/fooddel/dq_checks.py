"""Data-quality checks run by the fooddel_pipeline DAG after `dbt build`.

Each check is one SQL query returning a single number, a pass condition and a severity.
critical -> the run fails and ALERT_DQ_FAILED fires; warn -> recorded and trended only.
Results for every check, every run, go to OPS.DQ_RESULTS.

Pure Python (no Airflow imports) so it can be unit-tested without an Airflow install.
"""

from __future__ import annotations

import operator
from dataclasses import dataclass

OPERATORS = {"==": operator.eq, "<=": operator.le, ">=": operator.ge}


@dataclass(frozen=True)
class Check:
    name: str
    severity: str  # critical | warn
    sql: str  # {db} is replaced with the database name; must return one number (or NULL)
    operator: str
    threshold: float
    description: str


CHECKS: list[Check] = [
    Check(
        "silver_duplicate_event_ids",
        "critical",
        "SELECT COUNT(*) - COUNT(DISTINCT EVENT_ID) FROM {db}.SILVER.ORDER_EVENTS",
        "==",
        0,
        "Dedup failed: an event_id appears more than once in SILVER",
    ),
    Check(
        "delivered_without_pickup",
        "critical",
        "SELECT COUNT_IF(DELIVERED_TS IS NOT NULL AND PICKED_UP_TS IS NULL) FROM {db}.SILVER.ORDERS",
        "==",
        0,
        "Impossible lifecycle: delivered orders with no pickup event",
    ),
    Check(
        "h3_cell_mismatches",
        "critical",
        "SELECT COUNT_IF(NOT H3_MATCH) FROM {db}.SILVER.DRIVER_PINGS",
        "==",
        0,
        "Snowflake-computed H3 cell disagrees with the producer's cell",
    ),
    Check(
        "marts_vs_silver_order_count",
        "critical",
        "SELECT (SELECT COUNT(*) FROM {db}.MARTS.FCT_ORDERS)"
        " - (SELECT COUNT_IF(HAS_PLACED) FROM {db}.SILVER.ORDERS)",
        "==",
        0,
        "fct_orders lost or duplicated orders compared with SILVER",
    ),
    Check(
        "bronze_duplicate_pct_24h",
        "warn",
        # NULL (= no verdict) below 200 rows: a percentage over a handful of events is noise
        "SELECT IFF(COUNT(*) >= 200,"
        " 100 * (COUNT(*) - COUNT(DISTINCT RECORD_CONTENT:event_id)) / COUNT(*), NULL)"
        " FROM {db}.BRONZE.ORDERS_RAW"
        " WHERE RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER"
        " >= DATE_PART(EPOCH_MILLISECOND, DATEADD('hour', -24, SYSDATE()))",
        "<=",
        5,
        "Duplicate rate of streamed order events, min. 200 rows (2% is injected)",
    ),
    Check(
        "late_event_pct_24h",
        "warn",
        "SELECT IFF(COUNT(*) >= 200, 100 * COUNT_IF(LATENESS_S > 60) / COUNT(*), NULL)"
        " FROM {db}.SILVER.ORDER_EVENTS WHERE EMITTED_TS >= DATEADD('hour', -24, SYSDATE())",
        "<=",
        10,
        "Share of order events arriving more than 60 s after they happened",
    ),
    Check(
        "minutes_since_last_event",
        "warn",
        "SELECT DATEDIFF('minute', MAX(EVENT_TS), SYSDATE()) FROM {db}.SILVER.ORDER_EVENTS",
        "<=",
        60,
        "Freshness: how long ago the newest order event happened",
    ),
]


def evaluate(check: Check, observed: float | None) -> str:
    """PASS/FAIL. No data (NULL) passes: a check over an empty window has nothing to flag."""
    if observed is None:
        return "PASS"
    return "PASS" if OPERATORS[check.operator](float(observed), check.threshold) else "FAIL"


def summarize(results: list[dict]) -> dict:
    critical = [
        r["check_name"] for r in results if r["status"] == "FAIL" and r["severity"] == "critical"
    ]
    warnings = [
        r["check_name"] for r in results if r["status"] == "FAIL" and r["severity"] == "warn"
    ]
    return {"critical_failures": critical, "warnings": warnings}
