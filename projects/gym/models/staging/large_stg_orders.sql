SELECT
  * EXCLUDE (o_shippriority)
FROM
   {{ source('tpch', 'orders') }}
