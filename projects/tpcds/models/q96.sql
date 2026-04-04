SELECT count(*)
FROM {{ source('tpcds', 'store_sales') }} ,
     {{ source('tpcds', 'household_demographics') }},
     {{ source('tpcds', 'time_dim') }},
     {{ source('tpcds', 'store') }}
WHERE ss_sold_time_sk = {{ source('tpcds', 'time_dim') }}.t_time_sk
  AND ss_hdemo_sk = {{ source('tpcds', 'household_demographics') }}.hd_demo_sk
  AND ss_store_sk = s_store_sk
  AND {{ source('tpcds', 'time_dim') }}.t_hour = 20
  AND {{ source('tpcds', 'time_dim') }}.t_minute >= 30
  AND {{ source('tpcds', 'household_demographics') }}.hd_dep_count = 7
  AND {{ source('tpcds', 'store') }}.s_store_name = 'ese'
ORDER BY count(*)
LIMIT 100