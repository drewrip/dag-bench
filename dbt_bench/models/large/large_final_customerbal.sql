WITH
  final AS (
    SELECT
      r_regionkey,
      ANY_VALUE (r_name),
      SUM(c_acctbal)
    FROM
      {{ ref("large_mid_customerloc") }}
    GROUP BY
      r_regionkey
  )
SELECT
  *
FROM
  final
