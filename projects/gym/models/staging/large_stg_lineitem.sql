SELECT
  * EXCLUDE (l_shipmode)
FROM
    {{ source('tpch', 'lineitem') }}
