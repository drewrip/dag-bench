SELECT
  * EXCLUDE (p_comment)
FROM
   {{ source('tpch', 'part') }}
