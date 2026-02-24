{{
  config(
      materialized='table'
  )
}}

WITH
  final AS (
    SELECT
    qty + 10,
      price + 10
    FROM
      {{ ref('s1') }}

  )
SELECT * FROM final
