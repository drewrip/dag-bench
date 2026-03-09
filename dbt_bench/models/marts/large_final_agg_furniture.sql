SELECT
  *,
  avg_totalprice / max_totalprice,
  0.1 * avg_totalprice * (avg_totalprice / min_totalprice)
FROM
  {{ ref("large_mid_agg") }} t
