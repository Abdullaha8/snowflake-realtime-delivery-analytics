-- Customers: everyone who signed up, plus anyone who ordered without a signup event in the
-- data (e.g. a live run whose customers registered before BRONZE history starts).
-- PII columns (FULL_NAME, EMAIL, PHONE) are masked by policy in P7.
WITH CUSTOMER_IDS AS (
    SELECT CUSTOMER_ID FROM {{ ref('stg_customers') }}
    UNION
    SELECT CUSTOMER_ID FROM {{ ref('int_customer_first_order') }}
)

SELECT
    I.CUSTOMER_ID,
    C.FULL_NAME,
    C.EMAIL,
    C.PHONE,
    C.HOME_ZONE_ID,
    C.HOME_H3_R8,
    COALESCE(C.SIGNUP_CHANNEL, 'UNKNOWN') AS SIGNUP_CHANNEL,
    C.SIGNED_UP_AT_IST,
    F.FIRST_ORDER_AT_IST,
    F.LAST_ORDER_AT_IST,
    F.COHORT_MONTH,
    COALESCE(F.LIFETIME_ORDERS, 0) AS LIFETIME_ORDERS,
    COALESCE(F.LIFETIME_DELIVERED_ORDERS, 0) AS LIFETIME_DELIVERED_ORDERS,
    COALESCE(F.LIFETIME_GMV_INR, 0) AS LIFETIME_GMV_INR,
    F.CUSTOMER_ID IS NOT NULL AS HAS_ORDERED
FROM CUSTOMER_IDS AS I
LEFT JOIN {{ ref('stg_customers') }} AS C ON I.CUSTOMER_ID = C.CUSTOMER_ID
LEFT JOIN {{ ref('int_customer_first_order') }} AS F ON I.CUSTOMER_ID = F.CUSTOMER_ID
