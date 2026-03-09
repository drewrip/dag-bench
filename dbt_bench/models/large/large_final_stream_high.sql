SELECT
  c_custkey,
  ANY_VALUE (c_name),
  AVG(c_acctbal),
  SUM(c_acctbal)
FROM
  {{ ref("large_mid_stream") }} s
WHERE
  s.l_linenumber <> 5
  AND o_totalprice > 300045.0
GROUP BY
  c_custkey
