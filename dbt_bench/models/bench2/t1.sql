{{
  config(
      materialized='table'
  )
}}

WITH
  final AS (
    SELECT
      AVG(qty),
      AVG(price)
    FROM
      {{ ref('t0') }}

  )
SELECT * FROM final
