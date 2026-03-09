SELECT
  c_custkey,
  c_mktsegment,
  ANY_VALUE (c_name) AS c_name,
  SUM(o_totalprice) AS sum_totalprice,
  AVG(o_totalprice) AS avg_totalprice,
  MIN(o_totalprice) AS min_totalprice,
  MAX(o_totalprice) AS max_totalprice
FROM
  {{ ref("large_mid_t2") }}
GROUP BY
  c_custkey, c_mktsegment
ORDER BY
  sum_totalprice DESC
