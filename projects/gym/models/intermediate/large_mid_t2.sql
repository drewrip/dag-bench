SELECT
  {{ dbt_utils.star(from=ref("large_mid_t1"), except=[]) }}
FROM {{ ref("large_mid_t1") }}
WHERE
  l_discount > 0.02
