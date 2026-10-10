-- Leakage guard: recompute the 30-minute zone load feature with an explicit self-join that
-- counts only orders placed STRICTLY EARLIER, for a deterministic sample of 500 orders. Any
-- difference means the window feature saw the current or a future order.
WITH SAMPLE_ORDERS AS (
    SELECT ORDER_ID, ZONE_ID, PLACED_TS, ZONE_ORDERS_PREV_30M
    FROM {{ ref('ml_eta_features') }}
    QUALIFY ROW_NUMBER() OVER (ORDER BY HASH(ORDER_ID)) <= 500
),

RECOMPUTED AS (
    SELECT
        S.ORDER_ID,
        S.ZONE_ORDERS_PREV_30M,
        COUNT(E.ORDER_ID) AS EXPECTED
    FROM SAMPLE_ORDERS AS S
    LEFT JOIN {{ ref('ml_eta_features') }} AS E
        ON
            S.ZONE_ID = E.ZONE_ID
            AND E.PLACED_TS < S.PLACED_TS
            AND E.PLACED_TS >= DATEADD('minute', -30, S.PLACED_TS)
    GROUP BY 1, 2
)

SELECT *
FROM RECOMPUTED
WHERE ZONE_ORDERS_PREV_30M != EXPECTED
