WITH
  final AS (
    SELECT
      c_nationkey,
      sum(l_extendedprice) as le,
      sum(l_quantity) as lq
    FROM
      {{ ref("large_stg_customer") }},
      {{ ref("large_stg_orders") }},
      {{ ref("large_stg_lineitem") }}
    WHERE
      c_custkey = o_custkey
      and o_orderkey = l_orderkey
      and o_orderdate < '1996-07-01'
      and c_nationkey > 5
      and c_nationkey < 25
    GROUP BY
      c_nationkey
  )
SELECT
  *
FROM
  final
