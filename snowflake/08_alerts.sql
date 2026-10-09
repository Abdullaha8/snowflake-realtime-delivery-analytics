-- 08_alerts.sql: Snowflake Alerts (serverless). Run via `make alerts-setup`.
-- Alerts are created SUSPENDED (Snowflake default) and only run during demos:
--   make alerts-resume / make alerts-suspend   (make stop always suspends them)
-- Each alert's action is a single CALL to OPS.RAISE_ALERT, which logs to OPS.ALERT_LOG and
-- emails if SNOWFLAKE_ALERT_EMAIL is configured (see 08b_alert_email.sql, run by alerts-setup).

USE ROLE FOODDEL_TRANSFORM;
USE SCHEMA ${SNOWFLAKE_DATABASE}.OPS;

-- 1. The latest pipeline run had a critical data-quality failure.
CREATE OR REPLACE ALERT ALERT_DQ_FAILED
    SCHEDULE = '10 MINUTE'
    COMMENT = 'Critical data-quality check failed in the last 10 minutes'
IF (EXISTS (
    SELECT 1
    FROM ${SNOWFLAKE_DATABASE}.OPS.DQ_RESULTS
    WHERE
        SEVERITY = 'critical'
        AND STATUS = 'FAIL'
        AND CHECKED_AT > DATEADD('minute', -10, CURRENT_TIMESTAMP())
))
THEN
    CALL ${SNOWFLAKE_DATABASE}.OPS.RAISE_ALERT(
        'ALERT_DQ_FAILED', 'A critical data-quality check failed. See OPS.DQ_RESULTS.'
    );

-- 2. SLA breach rate above 25% over the last 30 minutes of stream time (min. 20 deliveries).
CREATE OR REPLACE ALERT ALERT_SLA_SPIKE
    SCHEDULE = '10 MINUTE'
    COMMENT = 'SLA breach rate > 25% in the last 30 minutes of stream time'
IF (EXISTS (
    WITH CLOCK AS (
        SELECT MAX(DELIVERED_TS) AS STREAM_NOW FROM ${SNOWFLAKE_DATABASE}.SILVER.ORDERS
    )

    SELECT 1
    FROM ${SNOWFLAKE_DATABASE}.SILVER.ORDERS AS O
    CROSS JOIN CLOCK AS C
    WHERE O.DELIVERED_TS > DATEADD('minute', -30, C.STREAM_NOW)
    HAVING COUNT(*) >= 20 AND COUNT_IF(O.SLA_BREACHED) / COUNT(*) > 0.25
))
THEN
    CALL ${SNOWFLAKE_DATABASE}.OPS.RAISE_ALERT(
        'ALERT_SLA_SPIKE', 'SLA breach rate above 25% over the last 30 minutes. See GOLD.SLA_BREACHES.'
    );

-- 3. Ingestion is slow: p95 producer -> Snowflake above 60 s in the last 15 minutes.
CREATE OR REPLACE ALERT ALERT_INGEST_LATENCY
    SCHEDULE = '10 MINUTE'
    COMMENT = 'p95 emit -> push latency > 60 s over the last 15 minutes'
IF (EXISTS (
    SELECT 1
    FROM ${SNOWFLAKE_DATABASE}.OPS.V_INGEST_LATENCY
    WHERE MINUTE_UTC > DATEADD('minute', -15, SYSDATE())
    HAVING SUM(EVENTS) > 0 AND MAX(P95_EMIT_TO_PUSH_S) > 60
))
THEN
    CALL ${SNOWFLAKE_DATABASE}.OPS.RAISE_ALERT(
        'ALERT_INGEST_LATENCY', 'Ingest latency p95 above 60 s. See OPS.V_INGEST_LATENCY.'
    );
