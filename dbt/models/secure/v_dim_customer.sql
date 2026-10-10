-- Customers with PII masked unless the reader may see it (macros/governance.sql). Zone
-- managers see customers whose home zone they manage.
SELECT
    CUSTOMER_ID,
    {{ mask_name('FULL_NAME') }} AS FULL_NAME,
    {{ mask_email('EMAIL') }} AS EMAIL,
    {{ mask_phone('PHONE') }} AS PHONE,
    HOME_ZONE_ID,
    SIGNUP_CHANNEL,
    SIGNED_UP_AT_IST,
    FIRST_ORDER_AT_IST,
    LAST_ORDER_AT_IST,
    COHORT_MONTH,
    LIFETIME_ORDERS,
    LIFETIME_DELIVERED_ORDERS,
    LIFETIME_GMV_INR,
    HAS_ORDERED
FROM {{ ref('dim_customer') }}
WHERE {{ zone_filter('HOME_ZONE_ID') }}
