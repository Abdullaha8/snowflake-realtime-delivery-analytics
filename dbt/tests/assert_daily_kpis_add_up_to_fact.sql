-- The daily mart must add back up to the fact table (no orders lost or double counted).
WITH MART AS (
    SELECT SUM(ORDERS_PLACED) AS ORDERS, SUM(GMV_INR) AS GMV FROM {{ ref('mart_daily_kpis') }}
),

FACT AS (
    SELECT COUNT(*) AS ORDERS, SUM(GMV_INR) AS GMV FROM {{ ref('fct_orders') }}
)

SELECT MART.ORDERS, FACT.ORDERS AS FACT_ORDERS, MART.GMV, FACT.GMV AS FACT_GMV
FROM MART CROSS JOIN FACT
WHERE MART.ORDERS != FACT.ORDERS OR MART.GMV != FACT.GMV
