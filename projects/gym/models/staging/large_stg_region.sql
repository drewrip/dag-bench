SELECT
  * EXCLUDE (r_comment)
FROM
   {{ source('tpch', 'region') }}
