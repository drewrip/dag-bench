WITH
  steel AS (
    SELECT
      *
    FROM
      {{ ref("large_mid_steel") }}
  ),
  copper AS (
    SELECT
      *
    FROM
      {{ ref("large_mid_copper") }}
  ),
  brass AS (
    SELECT
      *
    FROM
      {{ ref("large_mid_brass") }}
  )
SELECT
  *,
  (
    CASE
      WHEN steel_price.avg_price < p_retailprice THEN 1
      ELSE 0
    END
  ) AS steel_status,
  (
    CASE
      WHEN copper_price.avg_price < p_retailprice THEN 1
      ELSE 0
    END
  ) AS copper_status,
  (
    CASE
      WHEN brass_price.avg_price < p_retailprice THEN 1
      ELSE 0
    END
  ) AS brass_status
FROM
  (
    SELECT
      *
    FROM
      steel
    UNION ALL
    SELECT
      *
    FROM
      copper
    UNION ALL
    SELECT
      *
    FROM
      brass
  ) kt,
  (
    SELECT
      AVG(p_retailprice) AS avg_price
    FROM
      steel
  ) steel_price,
  (
    SELECT
      AVG(p_retailprice) AS avg_price
    FROM
      copper
  ) copper_price,
  (
    SELECT
      AVG(p_retailprice) AS avg_price
    FROM
      brass
  ) brass_price
