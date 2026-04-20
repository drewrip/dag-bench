SELECT
  {{ dbt_utils.star(from=source('tpch', 'partsupp'), except=["ps_comment"]) }}
FROM
   {{ source('tpch', 'partsupp') }}
