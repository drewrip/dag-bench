SELECT * FROM {{ ref("large_mid_t1") }}
WHERE
  l_discount > 0.02
