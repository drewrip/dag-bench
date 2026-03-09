SELECT
  *
FROM
  {{ ref("large_stg_partsupp") }} ps
  JOIN {{ ref("large_stg_part") }} p ON ps.ps_partkey = p.p_partkey
  JOIN {{ ref("large_stg_supplier") }} s ON ps.ps_suppkey = s.s_suppkey
