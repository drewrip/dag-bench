WITH
  partinfo AS (
    SELECT
      *
    FROM
      {{ ref("large_mid_encoding") }}
  ),
  partinfo_spec AS (
    SELECT
      *
    FROM
      {{ ref("large_mid_encoding") }}
    WHERE
      p_container LIKE '%BOX%'
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
  ) AS is_box
FROM
  partinfo
