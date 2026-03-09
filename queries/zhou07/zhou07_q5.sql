WITH
  final AS (
    SELECT
      c_name,
      c_nationkey,
      o_totalprice
    FROM
      customer,
      orders
    WHERE
      c_custkey = o_custkey
  )
SELECT
  *
FROM
  final
