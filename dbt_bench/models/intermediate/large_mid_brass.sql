WITH
  partinfo AS (
    SELECT
      *
    FROM
      {{ ref("large_mid_partinfo") }}
  ),
  partinfo_spec AS (
    SELECT
      *
    FROM
      {{ ref("large_mid_partinfo") }}
    WHERE
      p_type LIKE '%BRASS%'
  )
SELECT
  *,
  (
    (
      SELECT
        AVG(p_retailprice) AS avg_price
      FROM
        partinfo_spec
    ) > p_retailprice
  ) AS is_brass
FROM
  partinfo
