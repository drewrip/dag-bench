SELECT
  * EXCLUDE (s_comment)
FROM
   {{ source('tpch', 'supplier') }}
