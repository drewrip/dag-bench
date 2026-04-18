SELECT
  * EXCLUDE (ps_comment)
FROM
   {{ source('tpch', 'partsupp') }}
