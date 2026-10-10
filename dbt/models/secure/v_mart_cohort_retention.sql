-- No PII and no zone-level restriction: exposed as-is so consumers never need MARTS directly.
SELECT *
FROM {{ ref('mart_cohort_retention') }}
