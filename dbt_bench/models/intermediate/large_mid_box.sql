SELECT
  *
FROM
  {{ ref("large_mid_encoding") }}
WHERE
  p_container LIKE '%BOX%'
