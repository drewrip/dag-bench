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
  SUM(box.is_steel) AS steel_cnt,
  SUM(box.is_copper) AS copper_cnt,
  SUM(box.is_brass) AS brass_cnt,
  SUM(is_box) AS box_cnt,
  SUM(is_drum) AS drum_cnt,
  SUM(is_case) AS case_cnt
FROM
  box
  JOIN drum ON box.ps_partkey = drum.ps_partkey
  AND box.ps_suppkey = drum.ps_suppkey
  JOIN case_con ON box.ps_partkey = case_con.ps_partkey
  AND box.ps_suppkey = case_con.ps_suppkey
