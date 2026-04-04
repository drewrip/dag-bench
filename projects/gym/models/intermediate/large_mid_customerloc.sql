WITH
  final AS (
    SELECT
      *
    FROM
      {{ ref("large_stg_customer") }} c
      JOIN {{ ref("large_stg_nation") }} n ON c.c_nationkey = n.n_nationkey
      JOIN {{ ref("large_stg_region") }} r ON n.n_regionkey = r.r_regionkey
      JOIN {{ ref("large_stg_orders") }} o ON c.c_custkey = o.o_custkey
  )
SELECT
  *
FROM
  final
