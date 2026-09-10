{{
  config(
    materialized='table',
    properties={
      'format': "'PARQUET'"
    }
  )
}}

SELECT
    city,
    COUNT(user_id) AS total_users,
    COUNT(DISTINCT company_name) AS unique_companies,
    MIN(ingested_at) AS first_ingested_at,
    MAX(ingested_at) AS last_ingested_at,
    CURRENT_TIMESTAMP AS calculated_at
FROM {{ ref('stg_users') }}
GROUP BY city

