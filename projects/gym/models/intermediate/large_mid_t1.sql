SELECT
  *
FROM
  {{ ref("large_stg_lineitem") }} l
  JOIN {{ ref("large_stg_orders") }} o ON l.l_orderkey = o.o_orderkey
  JOIN {{ ref("large_stg_customer") }} c ON o.o_custkey = c.c_custkey
