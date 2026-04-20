SELECT
  {{ dbt_utils.star(from=source('tpch', 'supplier'), except=["s_comment"]) }}
FROM
   {{ source('tpch', 'supplier') }}
