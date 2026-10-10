-- 09_ml.sql: ETA model bookkeeping - version log, predictions, accuracy view, drift alert.
-- Run via `make ml-setup`. The model itself lives in the Model Registry (ML.ETA_XGB), created
-- by ml/train.py.

USE ROLE FOODDEL_TRANSFORM;
USE SCHEMA ${SNOWFLAKE_DATABASE}.ML;

-- One row per trained version: offline metrics, baselines, and whether it was promoted.
CREATE TABLE IF NOT EXISTS MODEL_VERSIONS (
    MODEL_NAME STRING NOT NULL,
    VERSION_NAME STRING NOT NULL,
    TRAINED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    TRAIN_ROWS NUMBER,
    TEST_ROWS NUMBER,
    VALID_MAE FLOAT,
    TEST_MAE FLOAT,
    TEST_P90_ABS_ERR FLOAT,
    TEST_BIAS FLOAT,
    BASELINE_PROMISED_MAE FLOAT COMMENT 'platform promised ETA (padded by design)',
    BASELINE_RULE_MAE FLOAT COMMENT 'promised ETA minus its fixed buffer',
    BASELINE_ZONE_HOUR_MAE FLOAT COMMENT 'mean delivery time per zone x hour (train)',
    PROMOTED BOOLEAN COMMENT 'became the default version',
    FEATURES STRING COMMENT 'comma-separated model inputs, in signature order',
    OUTPUT_COLUMN STRING COMMENT 'field of the PREDICT result holding the prediction',
    PARAMS VARIANT
)
COMMENT = 'ETA model versions trained by ml/train.py (champion/challenger log)';

-- Scores written by the pipeline (and the backtest): one row per order and model version.
CREATE TABLE IF NOT EXISTS ETA_PREDICTIONS (
    ORDER_ID STRING NOT NULL,
    MODEL_NAME STRING NOT NULL,
    MODEL_VERSION STRING NOT NULL,
    PREDICTED_ETA_MIN FLOAT,
    PROMISED_ETA_MIN NUMBER,
    SCORED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'ETA model predictions (in-warehouse inference via MODEL(...)!PREDICT)';

-- Accuracy per IST day and model version, side by side with the platform's promise.
CREATE OR REPLACE VIEW V_ETA_ACCURACY_DAILY
COMMENT = 'Daily ETA accuracy: model MAE vs promised-ETA MAE on delivered orders'
AS
SELECT
    F.PLACED_DATE_IST AS DATE_DAY,
    P.MODEL_NAME,
    P.MODEL_VERSION,
    COUNT(*) AS DELIVERED_ORDERS,
    ROUND(AVG(ABS(P.PREDICTED_ETA_MIN - F.ACTUAL_DELIVERY_MIN)), 3) AS MODEL_MAE,
    ROUND(AVG(P.PREDICTED_ETA_MIN - F.ACTUAL_DELIVERY_MIN), 3) AS MODEL_BIAS,
    ROUND(APPROX_PERCENTILE(ABS(P.PREDICTED_ETA_MIN - F.ACTUAL_DELIVERY_MIN), 0.9), 3)
        AS MODEL_P90_ABS_ERR,
    ROUND(AVG(ABS(P.PROMISED_ETA_MIN - F.ACTUAL_DELIVERY_MIN)), 3) AS PROMISED_MAE,
    ROUND(AVG(ABS(P.PROMISED_ETA_MIN - 12 - F.ACTUAL_DELIVERY_MIN)), 3) AS RULE_MAE
FROM ETA_PREDICTIONS AS P
INNER JOIN ${SNOWFLAKE_DATABASE}.MARTS.FCT_ORDERS AS F ON P.ORDER_ID = F.ORDER_ID
WHERE F.IS_DELIVERED
GROUP BY 1, 2, 3;

-- Drift: the newest day with enough deliveries is 30% worse than the version's test MAE.
CREATE OR REPLACE ALERT ALERT_ETA_DRIFT
    SCHEDULE = 'USING CRON 0 6 * * * Asia/Kolkata'
    COMMENT = 'ETA model MAE > 1.3 x its offline test MAE on the latest day (>= 50 deliveries)'
IF (EXISTS (
    WITH LATEST AS (
        SELECT
            A.MODEL_VERSION,
            A.MODEL_MAE
        FROM ${SNOWFLAKE_DATABASE}.ML.V_ETA_ACCURACY_DAILY AS A
        WHERE A.DELIVERED_ORDERS >= 50
        QUALIFY ROW_NUMBER() OVER (PARTITION BY A.MODEL_VERSION ORDER BY A.DATE_DAY DESC) = 1
    )

    SELECT 1
    FROM LATEST AS L
    INNER JOIN ${SNOWFLAKE_DATABASE}.ML.MODEL_VERSIONS AS V
        ON L.MODEL_VERSION = V.VERSION_NAME
    WHERE V.PROMOTED AND L.MODEL_MAE > 1.3 * V.TEST_MAE
))
THEN
    CALL ${SNOWFLAKE_DATABASE}.OPS.RAISE_ALERT(
        'ALERT_ETA_DRIFT', 'ETA model MAE drifted above 1.3x its test MAE. See ML.V_ETA_ACCURACY_DAILY.'
    );
