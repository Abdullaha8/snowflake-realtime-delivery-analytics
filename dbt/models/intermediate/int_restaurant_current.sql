-- Current state per restaurant: the input to the dbt snapshot (snap_restaurants).
SELECT
    RESTAURANT_ID,
    RESTAURANT_NAME,
    ZONE_ID,
    CUISINE,
    PRICE_BAND,
    AVG_PREP_MIN,
    RATING,
    IS_ACTIVE
FROM {{ ref('int_restaurant_versions') }}
WHERE IS_CURRENT
