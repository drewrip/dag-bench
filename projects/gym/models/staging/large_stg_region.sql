SELECT
  {{ dbt_utils.star(from=source('tpch', 'region'), except=["r_comment"]) }}
FROM
   {{ source('tpch', 'region') }}
