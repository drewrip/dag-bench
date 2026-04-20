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
  SUM(CAST(box.is_steel AS INT)) AS steel_cnt,
  SUM(CAST(box.is_copper AS INT)) AS copper_cnt,
  SUM(CAST(box.is_brass AS INT)) AS brass_cnt,
  SUM(CAST(is_box AS INT)) AS box_cnt,
  SUM(CAST(is_drum AS INT)) AS drum_cnt,
  SUM(CAST(is_case AS INT)) AS case_cnt
FROM
  box
  JOIN drum ON box.ps_partkey = drum.ps_partkey
  AND box.ps_suppkey = drum.ps_suppkey
  JOIN case_con ON box.ps_partkey = case_con.ps_partkey
  AND box.ps_suppkey = case_con.ps_suppkey
