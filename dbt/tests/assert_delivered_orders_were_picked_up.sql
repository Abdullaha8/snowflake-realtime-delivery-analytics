-- A delivered order must have been picked up (and therefore assigned and food-ready).
SELECT ORDER_ID
FROM {{ ref('fct_orders') }}
WHERE IS_DELIVERED AND (PICKED_UP_TS IS NULL OR DRIVER_ASSIGNED_TS IS NULL OR FOOD_READY_TS IS NULL)
