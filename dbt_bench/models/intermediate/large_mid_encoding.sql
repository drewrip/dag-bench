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
  steel.ps_partkey AS ps_partkey,
  steel.ps_suppkey AS ps_suppkey,
  steel.p_retailprice AS p_retailprice,
  steel.p_container AS p_container,
  steel.is_steel AS is_steel,
  copper.is_copper AS is_copper,
  brass.is_brass AS is_brass
FROM
  steel
  JOIN copper ON steel.ps_partkey = copper.ps_partkey
  AND steel.ps_suppkey = copper.ps_suppkey
  JOIN brass ON steel.ps_partkey = brass.ps_partkey
  AND steel.ps_suppkey = brass.ps_suppkey
