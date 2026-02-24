{{
  config(
      materialized='table'
  )
}}

WITH
  final AS (
    SELECT
      MIN(qty),
      MIN(price)
    FROM
      {{ ref('t0') }}

  )
SELECT * FROM final
