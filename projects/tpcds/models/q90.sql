SELECT case when pmc=0 then null else cast(amc AS decimal(15,4))/cast(pmc AS decimal(15,4)) end am_pm_ratio
FROM
  (SELECT count(*) amc
   FROM {{ source('tpcds', 'web_sales') }},
        {{ source('tpcds', 'household_demographics') }},
        {{ source('tpcds', 'time_dim') }},
        {{ source('tpcds', 'web_page') }}
   WHERE ws_sold_time_sk = {{ source('tpcds', 'time_dim') }}.t_time_sk
     AND ws_ship_hdemo_sk = {{ source('tpcds', 'household_demographics') }}.hd_demo_sk
     AND ws_web_page_sk = {{ source('tpcds', 'web_page') }}.wp_web_page_sk
     AND {{ source('tpcds', 'time_dim') }}.t_hour BETWEEN 8 AND 8+1
     AND {{ source('tpcds', 'household_demographics') }}.hd_dep_count = 6
     AND {{ source('tpcds', 'web_page') }}.wp_char_count BETWEEN 5000 AND 5200) at_,
  (SELECT count(*) pmc
   FROM {{ source('tpcds', 'web_sales') }},
        {{ source('tpcds', 'household_demographics') }},
        {{ source('tpcds', 'time_dim') }},
        {{ source('tpcds', 'web_page') }}
   WHERE ws_sold_time_sk = {{ source('tpcds', 'time_dim') }}.t_time_sk
     AND ws_ship_hdemo_sk = {{ source('tpcds', 'household_demographics') }}.hd_demo_sk
     AND ws_web_page_sk = {{ source('tpcds', 'web_page') }}.wp_web_page_sk
     AND {{ source('tpcds', 'time_dim') }}.t_hour BETWEEN 19 AND 19+1
     AND {{ source('tpcds', 'household_demographics') }}.hd_dep_count = 6
     AND {{ source('tpcds', 'web_page') }}.wp_char_count BETWEEN 5000 AND 5200) pt
ORDER BY am_pm_ratio
LIMIT 100