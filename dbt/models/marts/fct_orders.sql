{{
    config(
        materialized='incremental',
        unique_key='ORDER_ID',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

-- One row per order. Incremental: each run re-merges every order whose newest event was emitted
-- within `incremental_lookback_hours` of the latest one already loaded, so late and
-- out-of-order events still update the right row. A one-off bulk load of OLDER history
-- (e.g. a new backfill) needs `dbt build --full-refresh`.
WITH ORDERS AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
        WHERE LAST_EMITTED_TS >= (
            SELECT DATEADD('hour', -{{ var('incremental_lookback_hours') }}, MAX(LAST_EMITTED_TS))
            FROM {{ this }}
        )
    {% endif %}
)

SELECT
    O.ORDER_ID,
    O.CUSTOMER_ID,
    O.RESTAURANT_ID,
    R.RESTAURANT_SK,
    O.ZONE_ID,
    O.DRIVER_ID,
    O.H3_R8,
    O.PLACED_DATE_IST,
    O.PLACED_TS,
    O.PLACED_TS_IST,
    HOUR(O.PLACED_TS_IST) AS PLACED_HOUR_IST,
    O.ACCEPTED_TS,
    O.DRIVER_ASSIGNED_TS,
    O.FOOD_READY_TS,
    O.PICKED_UP_TS,
    O.DELIVERED_TS,
    O.CANCELLED_TS,
    O.ORDER_STATUS,
    O.ORDER_STATUS = 'DELIVERED' AS IS_DELIVERED,
    O.ORDER_STATUS = 'CANCELLED' AS IS_CANCELLED,
    O.CANCEL_REASON,
    O.CANCELLED_BY,
    O.PAYMENT_METHOD,
    O.IS_RAINING,
    O.ITEM_LINES,
    O.SUBTOTAL_INR,
    O.DELIVERY_FEE_INR,
    O.SURGE_FEE_INR,
    O.DISCOUNT_INR,
    O.TOTAL_INR,
    IFF(O.ORDER_STATUS = 'DELIVERED', O.TOTAL_INR, 0) AS GMV_INR,
    O.PROMISED_ETA_MIN,
    O.ACTUAL_DELIVERY_MIN,
    O.SLA_BREACHED,
    O.MINUTES_VS_PROMISE,
    O.ESTIMATED_PREP_MIN,
    O.ACTUAL_PREP_MIN,
    O.ACTUAL_PREP_MIN - O.ESTIMATED_PREP_MIN AS PREP_DELAY_MIN,
    O.DISTANCE_KM,
    O.ASSIGN_ATTEMPTS,
    R.RATING AS RESTAURANT_RATING_AT_ORDER,
    R.AVG_PREP_MIN AS RESTAURANT_AVG_PREP_AT_ORDER,
    O.EVENT_COUNT,
    O.LATE_EVENT_COUNT,
    O.LAST_EMITTED_TS,
    CURRENT_TIMESTAMP() AS DBT_LOADED_AT
FROM ORDERS AS O
-- Point-in-time: the restaurant version that was valid when the order was placed.
LEFT JOIN {{ ref('dim_restaurant') }} AS R
    ON
        O.RESTAURANT_ID = R.RESTAURANT_ID
        AND O.PLACED_TS >= R.VALID_FROM
        AND (O.PLACED_TS < R.VALID_TO OR R.VALID_TO IS NULL)
