-- ETA model features: one row per order, using ONLY what is known when the order is placed.
-- No pickup/delivery/actual-prep information (that would be leakage). The target
-- (ACTUAL_DELIVERY_MIN) is filled for delivered orders only.
--
-- Time-based split by IST placement date (dbt vars): a random split would let the model learn
-- from orders placed after the ones it is evaluated on.
WITH ORDERS AS (
    SELECT
        F.ORDER_ID,
        F.PLACED_TS,
        F.PLACED_DATE_IST,
        F.PLACED_HOUR_IST,
        F.PLACED_TS_IST,
        F.ZONE_ID,
        F.RESTAURANT_ID,
        F.IS_RAINING,
        F.SURGE_FEE_INR,
        F.ITEM_LINES,
        F.SUBTOTAL_INR,
        F.PROMISED_ETA_MIN,
        F.RESTAURANT_AVG_PREP_AT_ORDER,
        F.RESTAURANT_RATING_AT_ORDER,
        F.IS_DELIVERED,
        F.ACTUAL_DELIVERY_MIN,
        R.CUISINE,
        R.PRICE_BAND,
        R.LAT AS RESTAURANT_LAT,
        R.LNG AS RESTAURANT_LNG,
        S.DELIVERY_LAT,
        S.DELIVERY_LNG
    FROM {{ ref('fct_orders') }} AS F
    INNER JOIN {{ ref('dim_restaurant') }} AS R ON F.RESTAURANT_SK = R.RESTAURANT_SK
    INNER JOIN {{ ref('stg_orders') }} AS S ON F.ORDER_ID = S.ORDER_ID
)

SELECT
    ORDER_ID,
    PLACED_TS,
    PLACED_DATE_IST,
    CASE
        WHEN PLACED_DATE_IST <= '{{ var("eta_train_end") }}' THEN 'TRAIN'
        WHEN PLACED_DATE_IST <= '{{ var("eta_valid_end") }}' THEN 'VALID'
        WHEN PLACED_DATE_IST <= '{{ var("eta_test_end") }}' THEN 'TEST'
        ELSE 'LIVE'
    END AS SPLIT,

    HOUR(PLACED_TS_IST) AS PLACED_HOUR_IST,  -- for the zone x hour baseline, not a model feature

    -- time (cyclical hour so 23:00 and 00:00 are close)
    SIN(2 * PI() * (HOUR(PLACED_TS_IST) + MINUTE(PLACED_TS_IST) / 60) / 24) AS HOUR_SIN,
    COS(2 * PI() * (HOUR(PLACED_TS_IST) + MINUTE(PLACED_TS_IST) / 60) / 24) AS HOUR_COS,
    DAYOFWEEKISO(PLACED_DATE_IST) AS DAY_OF_WEEK,
    IFF(DAYOFWEEKISO(PLACED_DATE_IST) >= 6, 1, 0) AS IS_WEEKEND,
    IFF(MONTH(PLACED_DATE_IST) BETWEEN 6 AND 9, 1, 0) AS IS_MONSOON,

    -- context at placement
    IFF(IS_RAINING, 1, 0) AS IS_RAINING,
    SURGE_FEE_INR,
    ZONE_ID,

    -- restaurant as of order time (SCD2 point-in-time)
    RESTAURANT_AVG_PREP_AT_ORDER AS RESTAURANT_AVG_PREP_MIN,
    RESTAURANT_RATING_AT_ORDER AS RESTAURANT_RATING,
    PRICE_BAND,
    CUISINE,

    -- order
    ITEM_LINES,
    SUBTOTAL_INR,

    -- geography: straight-line distance from coordinates both known at placement
    ST_DISTANCE(
        ST_MAKEPOINT(RESTAURANT_LNG, RESTAURANT_LAT), ST_MAKEPOINT(DELIVERY_LNG, DELIVERY_LAT)
    ) / 1000 AS DISTANCE_KM,

    -- real-time load: orders placed EARLIER in the last 30 minutes (current row excluded)
    COUNT(*) OVER (
        PARTITION BY ZONE_ID ORDER BY PLACED_TS
        RANGE BETWEEN INTERVAL '30 MINUTES' PRECEDING AND CURRENT ROW
    ) - 1 AS ZONE_ORDERS_PREV_30M,
    COUNT(*) OVER (
        PARTITION BY RESTAURANT_ID ORDER BY PLACED_TS
        RANGE BETWEEN INTERVAL '30 MINUTES' PRECEDING AND CURRENT ROW
    ) - 1 AS RESTAURANT_ORDERS_PREV_30M,

    -- baseline (the platform's rule-based promise) and target
    PROMISED_ETA_MIN,
    IFF(IS_DELIVERED, ACTUAL_DELIVERY_MIN, NULL) AS ACTUAL_DELIVERY_MIN
FROM ORDERS
