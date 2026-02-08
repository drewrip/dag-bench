WITH
  final AS (
    SELECT
      *
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
