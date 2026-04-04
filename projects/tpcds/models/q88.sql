SELECT *
FROM
  (SELECT count(*) h8_30_to_9
   FROM {{ source('tpcds', 'store_sales') }},
        {{ source('tpcds', 'household_demographics') }},
        {{ source('tpcds', 'time_dim') }},
        {{ source('tpcds', 'store') }}
   WHERE ss_sold_time_sk = {{ source('tpcds', 'time_dim') }}.t_time_sk
     AND ss_hdemo_sk = {{ source('tpcds', 'household_demographics') }}.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND {{ source('tpcds', 'time_dim') }}.t_hour = 8
     AND {{ source('tpcds', 'time_dim') }}.t_minute >= 30
     AND (({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 4
           AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=4+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 2
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=2+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 0
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=0+2))
     AND {{ source('tpcds', 'store') }}.s_store_name = 'ese') s1,
  (SELECT count(*) h9_to_9_30
   FROM {{ source('tpcds', 'store_sales') }},
        {{ source('tpcds', 'household_demographics') }},
        {{ source('tpcds', 'time_dim') }},
        {{ source('tpcds', 'store') }}
   WHERE ss_sold_time_sk = {{ source('tpcds', 'time_dim') }}.t_time_sk
     AND ss_hdemo_sk = {{ source('tpcds', 'household_demographics') }}.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND {{ source('tpcds', 'time_dim') }}.t_hour = 9
     AND {{ source('tpcds', 'time_dim') }}.t_minute < 30
     AND (({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 4
           AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=4+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 2
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=2+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 0
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=0+2))
     AND {{ source('tpcds', 'store') }}.s_store_name = 'ese') s2,
  (SELECT count(*) h9_30_to_10
   FROM {{ source('tpcds', 'store_sales') }},
        {{ source('tpcds', 'household_demographics') }},
        {{ source('tpcds', 'time_dim') }},
        {{ source('tpcds', 'store') }}
   WHERE ss_sold_time_sk = {{ source('tpcds', 'time_dim') }}.t_time_sk
     AND ss_hdemo_sk = {{ source('tpcds', 'household_demographics') }}.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND {{ source('tpcds', 'time_dim') }}.t_hour = 9
     AND {{ source('tpcds', 'time_dim') }}.t_minute >= 30
     AND (({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 4
           AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=4+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 2
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=2+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 0
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=0+2))
     AND {{ source('tpcds', 'store') }}.s_store_name = 'ese') s3,
  (SELECT count(*) h10_to_10_30
   FROM {{ source('tpcds', 'store_sales') }},
        {{ source('tpcds', 'household_demographics') }},
        {{ source('tpcds', 'time_dim') }},
        {{ source('tpcds', 'store') }}
   WHERE ss_sold_time_sk = {{ source('tpcds', 'time_dim') }}.t_time_sk
     AND ss_hdemo_sk = {{ source('tpcds', 'household_demographics') }}.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND {{ source('tpcds', 'time_dim') }}.t_hour = 10
     AND {{ source('tpcds', 'time_dim') }}.t_minute < 30
     AND (({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 4
           AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=4+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 2
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=2+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 0
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=0+2))
     AND {{ source('tpcds', 'store') }}.s_store_name = 'ese') s4,
  (SELECT count(*) h10_30_to_11
   FROM {{ source('tpcds', 'store_sales') }},
        {{ source('tpcds', 'household_demographics') }},
        {{ source('tpcds', 'time_dim') }},
        {{ source('tpcds', 'store') }}
   WHERE ss_sold_time_sk = {{ source('tpcds', 'time_dim') }}.t_time_sk
     AND ss_hdemo_sk = {{ source('tpcds', 'household_demographics') }}.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND {{ source('tpcds', 'time_dim') }}.t_hour = 10
     AND {{ source('tpcds', 'time_dim') }}.t_minute >= 30
     AND (({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 4
           AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=4+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 2
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=2+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 0
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=0+2))
     AND {{ source('tpcds', 'store') }}.s_store_name = 'ese') s5,
  (SELECT count(*) h11_to_11_30
   FROM {{ source('tpcds', 'store_sales') }},
        {{ source('tpcds', 'household_demographics') }},
        {{ source('tpcds', 'time_dim') }},
        {{ source('tpcds', 'store') }}
   WHERE ss_sold_time_sk = {{ source('tpcds', 'time_dim') }}.t_time_sk
     AND ss_hdemo_sk = {{ source('tpcds', 'household_demographics') }}.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND {{ source('tpcds', 'time_dim') }}.t_hour = 11
     AND {{ source('tpcds', 'time_dim') }}.t_minute < 30
     AND (({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 4
           AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=4+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 2
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=2+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 0
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=0+2))
     AND {{ source('tpcds', 'store') }}.s_store_name = 'ese') s6,
  (SELECT count(*) h11_30_to_12
   FROM {{ source('tpcds', 'store_sales') }},
        {{ source('tpcds', 'household_demographics') }},
        {{ source('tpcds', 'time_dim') }},
        {{ source('tpcds', 'store') }}
   WHERE ss_sold_time_sk = {{ source('tpcds', 'time_dim') }}.t_time_sk
     AND ss_hdemo_sk = {{ source('tpcds', 'household_demographics') }}.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND {{ source('tpcds', 'time_dim') }}.t_hour = 11
     AND {{ source('tpcds', 'time_dim') }}.t_minute >= 30
     AND (({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 4
           AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=4+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 2
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=2+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 0
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=0+2))
     AND {{ source('tpcds', 'store') }}.s_store_name = 'ese') s7,
  (SELECT count(*) h12_to_12_30
   FROM {{ source('tpcds', 'store_sales') }},
        {{ source('tpcds', 'household_demographics') }},
        {{ source('tpcds', 'time_dim') }},
        {{ source('tpcds', 'store') }}
   WHERE ss_sold_time_sk = {{ source('tpcds', 'time_dim') }}.t_time_sk
     AND ss_hdemo_sk = {{ source('tpcds', 'household_demographics') }}.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND {{ source('tpcds', 'time_dim') }}.t_hour = 12
     AND {{ source('tpcds', 'time_dim') }}.t_minute < 30
     AND (({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 4
           AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=4+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 2
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=2+2)
          OR ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 0
              AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count<=0+2))
     AND {{ source('tpcds', 'store') }}.s_store_name = 'ese') s8 ;

