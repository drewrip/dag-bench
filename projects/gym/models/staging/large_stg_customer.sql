SELECT
  * EXCLUDE (c_comment)
FROM
  {{ source('tpch', 'customer') }}
