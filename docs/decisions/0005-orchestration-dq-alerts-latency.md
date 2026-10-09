# ADR 0005: Orchestration, data quality, Snowflake Alerts and latency

- **Status:** Accepted (Phase 5, 2026-10-10)
- **Scope:** `airflow/`, `snowflake/07_ops.sql`, `08*_*.sql`, `02_rbac.sql` (SVC_AIRFLOW, alert grants), `scripts/gen_airflow_env.py`

## Context

The batch layer (Dynamic Table refresh, then dbt) needs a scheduler. Data quality has to be
checked every run and *remembered*. Someone must be told when quality, SLA performance or
ingestion latency goes wrong. Ingestion latency needs a number. All of it has to cost nothing
when there's nothing to do.

## Decisions

### 1. Airflow 3 on Astro, all versions pinned
- **Versions:** Astro CLI 1.45.0 (1.46.0 was under two weeks old). Astro Runtime 3.3-8, which is Airflow 3.3.2. `apache-airflow-providers-snowflake` 6.17.0.
- **dbt** runs in its own virtualenv inside the image, so its dependencies can't clash with Airflow's. The dbt project is mounted from `../dbt`, so model changes need no image rebuild.
- **Airflow 3 specifics:**
  - the `airflow.sdk` decorators
  - `Param` for run options
  - task code can't touch the metadata DB, so status is derived from XCom plus Snowflake
  - `DagBag.get_dag()` queries the DB, so the integrity tests use `dagbag.dags`
- **Local ports:** the Airflow UI is on **8082** (`api-server.port`, with `--no-proxy` because Astro's reverse proxy grabbed 8080). kafka-ui keeps 8080, Postgres moves to 5433.

### 2. One DAG, `fooddel_pipeline` (hourly, no catchup, one active run)
`check_new_data → refresh_dynamic_tables → dbt_build → data_quality → record_latency → record_run`

- **`check_new_data` short-circuits** when BRONZE has the same row count as at the last *successful* run. It reads that count with `SHOW TABLES` (metadata, so no warehouse): an idle hour costs **zero credits**. Verified: a run with no new data finished in 2 s with everything downstream skipped.
- **`force` parameter.** A count-based watermark can't see a fix that *removes* rows (count unchanged), so `--conf '{"force": true}'` forces a recovery run.
- **`refresh_dynamic_tables`** refreshes every table that has its own lag, and that refresh also brings along the tables it reads. It works while the tables are suspended, so the pipeline, not the Dynamic Table scheduler, decides when SILVER and GOLD move.
- **`dbt_build`** is a single BashOperator running `dbt build --target airflow`, as `SVC_AIRFLOW`. *Rejected:* Cosmos (one Airflow task per model). It gives a nicer graph, but adds another library, and dbt's own run already reports per-model results.
- **Retries:** 2, with exponential backoff, for transient failures. **`data_quality` has 0 retries**, because re-checking the same data gives the same answer, and it raises `AirflowFailException` to fail fast.
- **`record_run`** uses `trigger_rule=all_done`, so even a failed run gets a row in `OPS.PIPELINE_RUNS`. The watermark only advances on success.

### 3. Data quality: SQL checks with every result persisted
`include/fooddel/dq_checks.py` holds plain data (SQL, operator, threshold, severity) plus a pure `evaluate()` function, so it's unit-tested without Airflow. **Every result of every run** goes to `OPS.DQ_RESULTS`, which makes the history trendable for P8 and gives the alert something to read.

| Check | Severity | Rule |
|---|---|---|
| duplicate `event_id`s in SILVER | critical | = 0 |
| delivered without pickup | critical | = 0 |
| H3 mismatches | critical | = 0 |
| `fct_orders` count vs SILVER | critical | = 0 |
| BRONZE duplicate % (last 24 h) | warn | ≤ 5, needs ≥ 200 rows |
| late-event % (last 24 h) | warn | ≤ 10, needs ≥ 200 rows |
| minutes since last event | warn | ≤ 60 |

The minimum sample was added after a 13-row sample produced a meaningless 7.7% warning. Below 200 rows the check returns NULL (no verdict).

*Rejected:* Snowflake Data Metric Functions. They're the native option, but serverless and Enterprise-only, they run on their own schedule, and they're less visible in the pipeline. They'd suit always-on production tables.

### 4. Snowflake Alerts: serverless, suspended by default, one CALL each
- **Three alerts, all on a 10-minute schedule:**
  - `ALERT_DQ_FAILED`: a critical check failed in the last 10 minutes
  - `ALERT_SLA_SPIKE`: more than 25% breaches over the last 30 minutes of stream time, with at least 20 deliveries
  - `ALERT_INGEST_LATENCY`: p95 above 60 s over the last 15 minutes
- **Suspended by default.** Snowflake creates alerts suspended. `make alerts-resume` is for demos only, and `make stop` suspends them.
- **One action for all.** Each alert's action is a single `CALL OPS.RAISE_ALERT(name, message)`. The procedure writes to `OPS.ALERT_LOG` and, when `SNOWFLAKE_ALERT_EMAIL` is set, emails through the `FOODDEL_EMAIL` notification integration. An email failure is caught, so it can't break the alert.
- **Verified:** after injecting a delivered-without-pickup event, `EXECUTE ALERT` showed TRIGGERED in `ALERT_HISTORY` and logged a row (email off because no address is configured).

### 5. Latency metric
- **Ingest latency:** `OPS.V_INGEST_LATENCY` gives the per-minute p50/p95 of *producer `emitted_ts` → `SnowflakeConnectorPushTime`* for streamed rows.
- **GOLD freshness:** `GOLD_DATA_LAG_S` is now minus the stalest GOLD Dynamic Table's data timestamp.
- Each run stores both in `OPS.LATENCY_METRICS`.
- **Measured at 1× speed:** p50 **0.21 s**, p95 **0.22 s**, max 0.9 s. GOLD trailed real time by **29 s**, against a 10-minute target lag.
- **Excluded rows:** accelerated runs (`--speed > 1`) stamp simulated time, i.e. the future, so their rows would come out negative. They're excluded rather than averaged into a fake number.

### 6. Security
- **Service user.** Airflow connects as `SVC_AIRFLOW` (TYPE=SERVICE, key-pair, role `FOODDEL_TRANSFORM`).
- **Separate env file.** `scripts/gen_airflow_env.py` writes `airflow/.env` (gitignored) containing **only** the Airflow connection and settings. The Kafka connector's key and the personal admin key never enter the Airflow containers. Only `airflow_rsa_key.p8` is mounted, read-only.
- **Alert privileges.** `EXECUTE ALERT` and `EXECUTE MANAGED ALERT` are granted to the transform role by ACCOUNTADMIN in `02_rbac.sql`.

## Verification summary
| Scenario | Result |
|---|---|
| DAG integrity (`astro dev pytest`) | 3/3 |
| Full run | 6/6 tasks success in ~38 s; dbt PASS=58 inside Airflow; OPS tables populated |
| No new data | skipped after `check_new_data`, no warehouse |
| Injected DQ violation | `data_quality` failed (no retry), `record_run` logged DQ_FAILED, run failed, alert TRIGGERED + logged |
| Recovery (`force`) | all critical checks PASS again after deleting the bad row |

## Trade-offs
- **Hourly schedule and laptop Airflow.** Fine for a portfolio; production would run Astro or MWAA, with Airflow alerting (on_failure callbacks) as well as Snowflake Alerts.
- **Row-count watermark.** Cheap and warehouse-free, but blind to updates and deletes, hence `force`.
- **Alert email needs a verified address.** Without one, alerts only log.
- **Latency is measured to the connector's push**, not to the moment a row is queryable. Snowpipe Streaming adds a few seconds of commit time, which `GOLD_DATA_LAG_S` captures at the layer users actually query.
