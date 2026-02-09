WITH
  final AS (
    SELECT
      o_orderkey,
      l_extendedprice
    FROM
      orders,
      lineitem
    WHERE
      o_orderkey = l_orderkey
      and o_orderdate = '1995-01-01'
  )
SELECT
  *
FROM
  final
