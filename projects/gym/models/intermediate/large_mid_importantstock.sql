SELECT
  sum(ps_supplycost * ps_availqty) total_cost
FROM
  {{ ref("large_stg_partsupp") }},
  {{ ref("large_stg_supplier") }},
  {{ ref("large_stg_nation") }}
WHERE
  ps_suppkey = s_suppkey
  AND s_nationkey = n_nationkey
  AND n_name = 'GERMANY'
