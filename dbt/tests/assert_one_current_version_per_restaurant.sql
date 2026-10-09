-- SCD2 integrity: exactly one current version per restaurant.
SELECT RESTAURANT_ID, COUNT_IF(IS_CURRENT) AS CURRENT_VERSIONS
FROM {{ ref('dim_restaurant') }}
GROUP BY RESTAURANT_ID
HAVING COUNT_IF(IS_CURRENT) != 1
