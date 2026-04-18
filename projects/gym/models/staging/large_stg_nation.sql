SELECT
  * EXCLUDE (n_comment)
FROM
   {{ source('tpch', 'nation') }}
