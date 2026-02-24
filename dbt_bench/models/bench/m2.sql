{{
  config(
      materialized='table'
  )
}}

WITH
  final AS (
    SELECT
      qty + 11,
      price + 11
    FROM
      {{ ref('s1') }}

  )
SELECT * FROM final
