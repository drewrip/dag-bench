SELECT
  *
FROM
  {{ ref("large_mid_partinfo") }}
WHERE
  p_type LIKE '%COPPER%'
