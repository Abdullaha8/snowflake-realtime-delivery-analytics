"""
### fooddel_pipeline: hourly batch layer

`check_new_data` → `refresh_dynamic_tables` → `dbt_build` → `data_quality` → `score_eta`
→ `record_latency` → `record_run`

- **check_new_data** short-circuits the run when BRONZE has no new rows since the last
  successful run. It reads table metadata only (`SHOW TABLES`), so an idle hour costs no
  warehouse credits.
- **refresh_dynamic_tables** brings SILVER/GOLD up to date (works while they are suspended).
- **dbt_build** runs the dbt project (models + snapshot + tests) as `SVC_AIRFLOW`.
- **data_quality** runs `include/fooddel/dq_checks.py` and stores every result in
  `OPS.DQ_RESULTS`. A critical failure fails the run without retries (the data won't change
  on a retry); `ALERT_DQ_FAILED` then fires in Snowflake.
- **score_eta** scores newly placed (LIVE) orders with the Model Registry's default ETA
  model, in-warehouse (`MODEL(ML.ETA_XGB, <version>)!PREDICT`), into `ML.ETA_PREDICTIONS`.
- **record_latency** stores producer → Snowflake latency and GOLD freshness in
  `OPS.LATENCY_METRICS`.
- **record_run** always runs and writes `OPS.PIPELINE_RUNS`; the new-data watermark only moves
  forward when the run succeeded.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, Variable, dag, task
from airflow.sdk.exceptions import AirflowFailException
from include.fooddel import snowflake_ops as sfo
from include.fooddel.dq_checks import CHECKS, evaluate, summarize
from include.fooddel.eta_scoring import score_new_orders

CONN_ID = "snowflake_default"
WATERMARK = "fooddel_bronze_rows_at_last_success"
DBT_DIR = "/usr/local/airflow/dbt"
DBT_BIN = "/usr/local/airflow/dbt_venv/bin/dbt"


def _cursor():
    return SnowflakeHook(snowflake_conn_id=CONN_ID).get_conn().cursor()


@dag(
    dag_id="fooddel_pipeline",
    description="Refresh Dynamic Tables, dbt build, data quality, latency - hourly",
    schedule="@hourly",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
    },
    tags=["fooddel", "snowflake", "dbt"],
    # force=True skips the new-data check, e.g. a recovery run after deleting bad rows (a
    # row-count watermark cannot see a fix that removes data).
    params={"force": Param(False, type="boolean", description="run even without new data")},
    doc_md=__doc__,
)
def fooddel_pipeline():
    @task.short_circuit
    def check_new_data(params: dict) -> dict | bool:
        cur = _cursor()
        rows = sfo.bronze_row_count(cur)
        last = int(Variable.get(WATERMARK, default=-1))
        print(f"BRONZE rows now={rows}, at last successful run={last}, force={params['force']}")
        if rows == last and not params["force"]:
            print("No new data since the last successful run: skipping (no warehouse used).")
            return False
        return {"bronze_rows": rows, "new_rows": rows - max(last, 0)}

    @task
    def refresh_dynamic_tables() -> list[str]:
        refreshed = sfo.refresh_dynamic_tables(_cursor())
        print("refreshed:", refreshed)
        return refreshed

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {DBT_DIR} && {DBT_BIN} build --target airflow --profiles-dir . --project-dir ."
        ),
        append_env=True,
        retries=1,
    )

    @task(retries=0)
    def data_quality(**context) -> dict:
        run_id = context["run_id"]
        cur = _cursor()
        results = []
        for check in CHECKS:
            cur.execute(check.sql.format(db=sfo.DB))
            observed = cur.fetchone()[0]
            status = evaluate(check, observed)
            results.append(
                {
                    "check_name": check.name,
                    "severity": check.severity,
                    "observed": None if observed is None else float(observed),
                    "operator": check.operator,
                    "threshold": check.threshold,
                    "status": status,
                    "description": check.description,
                }
            )
            print(f"{status:4} {check.severity:8} {check.name} = {observed}")
        cur.executemany(
            f"INSERT INTO {sfo.DB}.OPS.DQ_RESULTS"
            " (RUN_ID, CHECK_NAME, SEVERITY, OBSERVED, OPERATOR, THRESHOLD, STATUS, DESCRIPTION)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (run_id, r["check_name"], r["severity"], r["observed"], r["operator"],
                 r["threshold"], r["status"], r["description"])
                for r in results
            ],  # fmt: skip
        )
        summary = summarize(results)
        if summary["critical_failures"]:
            # Not retried: re-running the same checks on the same data gives the same answer.
            raise AirflowFailException(f"critical DQ checks failed: {summary['critical_failures']}")
        return summary

    @task
    def score_eta() -> dict:
        result = score_new_orders(_cursor(), sfo.DB)
        print("eta scoring:", result)
        return result

    @task
    def record_latency(**context) -> dict:
        cur = _cursor()
        cur.execute(
            "SELECT SUM(EVENTS), MAX(P50_EMIT_TO_PUSH_S), MAX(P95_EMIT_TO_PUSH_S),"
            " MAX(MAX_EMIT_TO_PUSH_S)"
            f" FROM {sfo.DB}.OPS.V_INGEST_LATENCY WHERE MINUTE_UTC >= DATEADD('minute', -60, SYSDATE())"
        )
        events, p50, p95, worst = cur.fetchone()
        gold_lag = sfo.gold_data_lag_seconds(cur)
        cur.execute(
            f"INSERT INTO {sfo.DB}.OPS.LATENCY_METRICS (RUN_ID, WINDOW_MINUTES, EVENTS,"
            " P50_EMIT_TO_PUSH_S, P95_EMIT_TO_PUSH_S, MAX_EMIT_TO_PUSH_S, GOLD_DATA_LAG_S,"
            " GOLD_TARGET_LAG_S) VALUES (%s, 60, %s, %s, %s, %s, %s, %s)",
            (context["run_id"], events or 0, p50, p95, worst, gold_lag, sfo.GOLD_TARGET_LAG_S),
        )
        metrics = {"events": events or 0, "p50_s": p50, "p95_s": p95, "gold_lag_s": gold_lag}
        print(json.dumps(metrics, default=str))
        return metrics

    @task(trigger_rule="all_done")
    def record_run(new_data: dict, **context) -> str:
        # Airflow 3 tasks cannot query the metadata DB, so the outcome comes from data_quality's
        # XCom (only pushed on success) plus this run's rows in OPS.DQ_RESULTS.
        dq = context["ti"].xcom_pull(task_ids="data_quality")
        cur = _cursor()
        cur.execute(f"SELECT COUNT(*) FROM {sfo.DB}.MARTS.FCT_ORDERS")
        fct_rows = cur.fetchone()[0]
        cur.execute(
            f"SELECT COUNT_IF(SEVERITY = 'critical' AND STATUS = 'FAIL'),"
            f" COUNT_IF(SEVERITY = 'warn' AND STATUS = 'FAIL')"
            f" FROM {sfo.DB}.OPS.DQ_RESULTS WHERE RUN_ID = %s",
            (context["run_id"],),
        )
        critical, warnings = cur.fetchone()
        status = "SUCCESS" if dq is not None else "DQ_FAILED" if critical else "FAILED"
        cur.execute(
            f"INSERT INTO {sfo.DB}.OPS.PIPELINE_RUNS (RUN_ID, DAG_ID, STARTED_AT, STATUS,"
            " BRONZE_ROWS, NEW_BRONZE_ROWS, FCT_ORDERS_ROWS, DQ_CRITICAL_FAILURES, DQ_WARNINGS)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                context["run_id"], context["dag"].dag_id, context["dag_run"].start_date,
                status, new_data["bronze_rows"], new_data["new_rows"], fct_rows, critical, warnings,
            ),
        )  # fmt: skip
        if status == "SUCCESS":
            Variable.set(WATERMARK, str(new_data["bronze_rows"]))
        print(f"run {status}: fct_orders={fct_rows}, dq critical={critical}, warn={warnings}, {dq}")
        if status != "SUCCESS":
            raise AirflowFailException("pipeline run failed data quality")
        return status

    new_data = check_new_data()
    refreshed = refresh_dynamic_tables()
    dq = data_quality()
    scored = score_eta()
    latency = record_latency()
    run = record_run(new_data)
    new_data >> refreshed >> dbt_build >> dq >> scored >> latency >> run


fooddel_pipeline()
