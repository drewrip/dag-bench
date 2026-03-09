SELECT
  *
FROM
  {{ ref("large_mid_agg") }} t
WHERE
  t.c_mktsegment = 'FURNITURE'
