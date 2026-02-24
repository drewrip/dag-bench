{{
  config(
      materialized='table'
  )
}}

WITH
  final AS (
    SELECT
      qty + 12,
      price + 12
    FROM
      {{ ref('s1') }}

  )
SELECT * FROM final
