-- By definition every customer is active in their cohort month.
SELECT COHORT_MONTH, RETENTION_PCT
FROM {{ ref('mart_cohort_retention') }}
WHERE MONTHS_SINCE_FIRST_ORDER = 0 AND RETENTION_PCT != 100
