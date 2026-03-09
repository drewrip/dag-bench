WITH
  final AS (
    SELECT
      *
    FROM
      {{ ref("large_stg_customer") }},
      {{ ref("large_stg_orders") }}
    WHERE
      c_custkey = o_custkey
  )
SELECT
  *
FROM
  final
