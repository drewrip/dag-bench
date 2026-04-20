SELECT
  {{ dbt_utils.star(from=source('tpch', 'lineitem'), except=["l_shipmode"]) }}
FROM
    {{ source('tpch', 'lineitem') }}
