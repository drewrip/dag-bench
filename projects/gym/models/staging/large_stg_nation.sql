SELECT
  {{ dbt_utils.star(from=source('tpch', 'nation'), except=["n_comment"]) }}
FROM
   {{ source('tpch', 'nation') }}
