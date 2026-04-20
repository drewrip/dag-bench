SELECT
  {{ dbt_utils.star(from=source('tpch', 'part'), except=["p_comment"]) }}
FROM
   {{ source('tpch', 'part') }}
