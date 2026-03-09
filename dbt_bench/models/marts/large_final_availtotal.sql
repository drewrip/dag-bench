SELECT
  ps_suppkey,
  AVG(l_extendedprice)
FROM
  {{ ref("large_mid_availqty") }} aq,
  {{ ref("large_mid_customerloc") }} cl
WHERE
  aq.l_orderkey = cl.o_orderkey
GROUP BY
  ps_suppkey
