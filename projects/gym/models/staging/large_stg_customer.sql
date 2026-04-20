SELECT
  {{ dbt_utils.star(from=source('tpch', 'customer'), except=["c_comment"]) }}
FROM
  {{ source('tpch', 'customer') }}
