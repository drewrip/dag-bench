WITH
  final AS (
    SELECT
      *
    FROM
      {{ ref("large_stg_customer") }} c
      JOIN {{ ref("large_stg_supplier") }} s ON c.c_nationkey = s.s_nationkey
      JOIN {{ ref("large_stg_nation") }} n ON c.c_nationkey = n.n_nationkey
      JOIN {{ ref("large_stg_region") }} r ON n.n_regionkey = r.r_regionkey
  )
SELECT
  * EXCLUDE (s_phone)
FROM
  final
