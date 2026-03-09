WITH
  box AS (
    SELECT
      *
    FROM
      {{ ref("large_mid_box") }}
  ),
  drum AS (
    SELECT
      *
    FROM
      {{ ref("large_mid_drum") }}
  ),
  case_con AS (
    SELECT
      *
    FROM
      {{ ref("large_mid_case") }}
  )
SELECT
  *
FROM
  (
    SELECT
      *
    FROM drum UNION ALL
    SELECT
      *
    FROM
      case_con
    UNION ALL
    SELECT
      *
    FROM
      box
  ) kt,
  (
    SELECT
      MIN(p_retailprice) AS min_price
    FROM
      drum
  ) drum_price,
  (
    SELECT
      MIN(p_retailprice) AS min_price
    FROM
      case_con
  ) case_price,
  (
    SELECT
      MIN(p_retailprice) AS min_price
    FROM
      box
  ) box_price
WHERE
  (drum_price.min_price + case_price.min_price + box_price.min_price) < p_retailprice
