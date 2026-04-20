SELECT
  {{ dbt_utils.star(from=source('tpch', 'orders'), except=["o_shippriority"]) }}
FROM
   {{ source('tpch', 'orders') }}
