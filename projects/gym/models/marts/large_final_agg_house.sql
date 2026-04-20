SELECT
  {{ dbt_utils.star(from=ref("large_mid_agg"), except=[]) }},
  avg_totalprice / max_totalprice AS norm,
  0.345 * avg_totalprice * (avg_totalprice / min_totalprice) AS expected_norm
FROM
  {{ ref("large_mid_agg") }} t
