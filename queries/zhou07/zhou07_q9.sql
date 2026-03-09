/*
* This query is also slightly modified from Q9 in Zhou07.
* There is no column named 'totaldisc' in the original query.
* Here we choose that to be the name for the 'sum(l_discount)'.*/
WITH
  final AS (
    SELECT
      c_nationkey,
      n_name,
      sum(l_discount) AS totaldisc
    FROM
      customer,
      orders,
      lineitem,
      nation
    WHERE
      c_custkey = o_custkey
      and o_orderkey = l_orderkey
      and c_nationkey = n_nationkey
    GROUP BY
      c_nationkey,
      n_name
    HAVING
      sum(l_discount) > (
        SELECT
          sum(l_discount) / 25
        FROM
          customer,
          orders,
          lineitem
        WHERE
          c_custkey = o_custkey
          and o_orderkey = l_orderkey
      )
    ORDER BY
      totaldisc desc
  )
SELECT
  *
FROM
  final
