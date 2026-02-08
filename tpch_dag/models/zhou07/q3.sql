WITH
  final AS (
    select
      n_regionkey,
      sum(l_extendedprice) as le,
      sum(l_quantity) as lq
    from
      customer,
      orders,
      lineitem,
      nation
    where
      c_custkey = o_custkey
      and o_orderkey = l_orderkey
      and c_nationkey = n_nationkey
      and o_orderdate < '1996-07-01'
      and c_nationkey > 2
      and c_nationkey < 24
    GROUP BY
      n_regionkey
  )
SELECT
  *
FROM
  final
