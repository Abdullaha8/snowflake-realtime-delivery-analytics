-- SCD2 restaurant dimension. Join facts point-in-time:
--   order.PLACED_TS >= VALID_FROM AND (order.PLACED_TS < VALID_TO OR VALID_TO IS NULL)
SELECT
    RESTAURANT_SK,
    RESTAURANT_ID,
    VERSION_NUMBER,
    RESTAURANT_NAME,
    ZONE_ID,
    H3_R8,
    LAT,
    LNG,
    CUISINE,
    PRICE_BAND,
    AVG_PREP_MIN,
    RATING,
    IS_ACTIVE,
    CHANGED_FIELDS,
    VALID_FROM,
    VALID_TO,
    IS_CURRENT
FROM {{ ref('int_restaurant_versions') }}
