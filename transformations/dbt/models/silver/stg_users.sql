{{
  config(
    materialized='table',
    properties={
      'format': "'PARQUET'"
    }
  )
}}

SELECT
    CAST(id AS INTEGER) AS user_id,
    TRIM(name) AS full_name,
    LOWER(TRIM(username)) AS username,
    LOWER(TRIM(email)) AS email_address,
    TRIM(city) AS city,
    TRIM(company_name) AS company_name,
    CAST(ingested_at AS TIMESTAMP(6)) AS ingested_at,
    CURRENT_TIMESTAMP AS transformed_at
FROM {{ source('bronze', 'raw_users') }}
WHERE id IS NOT NULL

