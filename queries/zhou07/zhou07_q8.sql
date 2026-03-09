/*
* This is a modified version of Q8 from Zhou07. The original appears to reference a column 'p_availqty'
* which doesn't exist in the TPC-H spec (at least in 2026). To fix this, we use the quantity from lineitem.
* In the paper Q8 is also written with an 'ORDER BY p_type', this also seems like an error.
* What seems to be meant is 'GROUP BY p_type'.*/
WITH
  final AS (
    SELECT
      p_type,
      sum(l_quantity) as qty
    FROM
      part,
      orders,
      lineitem
    WHERE
      p_partkey = l_partkey
      and o_orderkey = l_orderkey
      and o_orderdate < '1996-07-01'
    GROUP BY
      p_type
  )
SELECT
  *
FROM
  final
