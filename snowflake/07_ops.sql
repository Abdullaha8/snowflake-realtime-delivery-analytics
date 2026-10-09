-- 07_ops.sql: pipeline-operations tables and latency views (written by the Airflow DAG).
-- Run via `make sf-ops`. Owned by FOODDEL_TRANSFORM; readable by FOODDEL_BI.

USE ROLE FOODDEL_TRANSFORM;
USE SCHEMA ${SNOWFLAKE_DATABASE}.OPS;

-- One row per DAG run that got past the "new data?" check.
CREATE TABLE IF NOT EXISTS PIPELINE_RUNS (
    RUN_ID STRING NOT NULL,
    DAG_ID STRING NOT NULL,
    STARTED_AT TIMESTAMP_TZ,
    FINISHED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    STATUS STRING COMMENT 'SUCCESS | DQ_FAILED',
    BRONZE_ROWS NUMBER COMMENT 'BRONZE row count at the start of the run (metadata)',
    NEW_BRONZE_ROWS NUMBER COMMENT 'rows that arrived since the previous successful run',
    FCT_ORDERS_ROWS NUMBER,
    DQ_CRITICAL_FAILURES NUMBER,
    DQ_WARNINGS NUMBER
)
COMMENT = 'Airflow fooddel_pipeline runs';

-- Every data-quality check result, every run: trendable, and the input to ALERT_DQ_FAILED.
CREATE TABLE IF NOT EXISTS DQ_RESULTS (
    RUN_ID STRING NOT NULL,
    CHECKED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    CHECK_NAME STRING NOT NULL,
    SEVERITY STRING NOT NULL COMMENT 'critical | warn',
    OBSERVED FLOAT,
    OPERATOR STRING COMMENT 'pass condition: observed <operator> threshold',
    THRESHOLD FLOAT,
    STATUS STRING NOT NULL COMMENT 'PASS | FAIL',
    DESCRIPTION STRING
)
COMMENT = 'Data-quality check results per pipeline run';

-- Latency per run: producer emit -> pushed into Snowflake, and how far GOLD trails real time.
CREATE TABLE IF NOT EXISTS LATENCY_METRICS (
    RUN_ID STRING NOT NULL,
    MEASURED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    WINDOW_MINUTES NUMBER,
    EVENTS NUMBER,
    P50_EMIT_TO_PUSH_S FLOAT,
    P95_EMIT_TO_PUSH_S FLOAT,
    MAX_EMIT_TO_PUSH_S FLOAT,
    GOLD_DATA_LAG_S FLOAT COMMENT 'now - newest data timestamp across GOLD Dynamic Tables',
    GOLD_TARGET_LAG_S FLOAT
)
COMMENT = 'Ingestion latency and GOLD freshness per pipeline run';

CREATE TABLE IF NOT EXISTS ALERT_LOG (
    ALERT_NAME STRING NOT NULL,
    FIRED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    MESSAGE STRING,
    EMAILED BOOLEAN
)
COMMENT = 'Every Snowflake Alert that fired (written by OPS.RAISE_ALERT)';

-- Ingestion latency per minute for streamed rows: producer emitted_ts -> connector push.
-- Meaningful for live runs at --speed 1 (event time = wall-clock time). Accelerated runs
-- (--speed > 1) stamp emitted_ts in simulated time, i.e. in the future: those rows come out
-- negative and are excluded rather than averaged into a fake number.
CREATE OR REPLACE VIEW V_INGEST_LATENCY
COMMENT = 'Per-minute p50/p95 producer-emit -> Snowflake-push latency (streamed rows only)'
AS
WITH STREAMED AS (
    SELECT
        TO_TIMESTAMP_NTZ(RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER, 3) AS PUSHED_AT,
        DATEDIFF(
            'millisecond',
            TO_TIMESTAMP_NTZ(RECORD_CONTENT:emitted_ts::STRING, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),
            TO_TIMESTAMP_NTZ(RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER, 3)
        ) / 1000 AS EMIT_TO_PUSH_S
    FROM ${SNOWFLAKE_DATABASE}.BRONZE.ORDERS_RAW
    WHERE RECORD_METADATA:SnowflakeConnectorPushTime IS NOT NULL
    UNION ALL
    SELECT
        TO_TIMESTAMP_NTZ(RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER, 3) AS PUSHED_AT,
        DATEDIFF(
            'millisecond',
            TO_TIMESTAMP_NTZ(RECORD_CONTENT:emitted_ts::STRING, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),
            TO_TIMESTAMP_NTZ(RECORD_METADATA:SnowflakeConnectorPushTime::NUMBER, 3)
        ) / 1000 AS EMIT_TO_PUSH_S
    FROM ${SNOWFLAKE_DATABASE}.BRONZE.DRIVER_LOCATIONS_RAW
    WHERE RECORD_METADATA:SnowflakeConnectorPushTime IS NOT NULL
)

SELECT
    DATE_TRUNC('minute', PUSHED_AT) AS MINUTE_UTC,
    COUNT(*) AS EVENTS,
    APPROX_PERCENTILE(EMIT_TO_PUSH_S, 0.5) AS P50_EMIT_TO_PUSH_S,
    APPROX_PERCENTILE(EMIT_TO_PUSH_S, 0.95) AS P95_EMIT_TO_PUSH_S,
    MAX(EMIT_TO_PUSH_S) AS MAX_EMIT_TO_PUSH_S
FROM STREAMED
WHERE EMIT_TO_PUSH_S >= 0
GROUP BY 1;

-- Health of the pipeline at a glance: last run, its DQ outcome, latest latency.
CREATE OR REPLACE VIEW V_PIPELINE_HEALTH
COMMENT = 'Latest pipeline run with its data-quality and latency results'
AS
WITH LAST_RUN AS (
    SELECT *
    FROM PIPELINE_RUNS
    QUALIFY ROW_NUMBER() OVER (ORDER BY FINISHED_AT DESC) = 1
)

SELECT
    R.RUN_ID,
    R.STATUS,
    R.FINISHED_AT,
    R.NEW_BRONZE_ROWS,
    R.FCT_ORDERS_ROWS,
    R.DQ_CRITICAL_FAILURES,
    R.DQ_WARNINGS,
    L.EVENTS AS LATENCY_SAMPLE_EVENTS,
    L.P50_EMIT_TO_PUSH_S,
    L.P95_EMIT_TO_PUSH_S,
    L.GOLD_DATA_LAG_S
FROM LAST_RUN AS R
LEFT JOIN LATENCY_METRICS AS L ON R.RUN_ID = L.RUN_ID;
