{{
  config(
      materialized='table'
  )
}}

WITH
  final AS (
    SELECT
      MAX(qty),
      MAX(price)
    FROM
      {{ ref('t0') }}

  )
SELECT * FROM final
