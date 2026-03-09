SELECT
  s_suppkey,
  ANY_VALUE (s_name),
  AVG(l_extendedprice)
FROM
  {{ ref("large_mid_availqty") }} aq,
  {{ ref("large_mid_customerloc") }} cl
WHERE
  aq.ps_suppkey = cl.s_suppkey
GROUP BY
  s_suppkey
