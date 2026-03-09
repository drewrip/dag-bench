SELECT
  *
FROM
  {{ ref("large_stg_lineitem") }} l,
  {{ ref("large_stg_partsupp") }} ps
WHERE
  l.l_partkey = ps.ps_partkey
  AND l.l_suppkey = ps.ps_suppkey
  AND ps.ps_availqty > 100
