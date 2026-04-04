SELECT
  ps_partkey,
  sum(ps_supplycost * ps_availqty) AS value
FROM
  {{ ref("large_stg_partsupp") }},
  {{ ref("large_stg_supplier") }},
  {{ ref("large_stg_nation") }}
WHERE
  ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
GROUP BY
  ps_partkey
HAVING
  sum(ps_supplycost * ps_availqty) > ( SELECT total_cost FROM {{ ref("large_mid_importantstock") }} ) * 0.0001000000
ORDER BY
  value DESC
