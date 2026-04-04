SELECT c_last_name,
       c_first_name,
       SUBSTRING(s_city,1,30),
       ss_ticket_number,
       amt,
       profit
FROM
  (SELECT ss_ticket_number ,
          ss_customer_sk ,
          {{ source('tpcds', 'store') }}.s_city ,
          sum(ss_coupon_amt) amt ,
          sum(ss_net_profit) profit
   FROM {{ source('tpcds', 'store_sales') }},
        {{ source('tpcds', 'date_dim') }},
        {{ source('tpcds', 'store') }},
        {{ source('tpcds', 'household_demographics') }}
   WHERE {{ source('tpcds', 'store_sales') }}.ss_sold_date_sk = {{ source('tpcds', 'date_dim') }}.d_date_sk
     AND {{ source('tpcds', 'store_sales') }}.ss_store_sk = {{ source('tpcds', 'store') }}.s_store_sk
     AND {{ source('tpcds', 'store_sales') }}.ss_hdemo_sk = {{ source('tpcds', 'household_demographics') }}.hd_demo_sk
     AND ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 6
          OR {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count > 2)
     AND {{ source('tpcds', 'date_dim') }}.d_dow = 1
     AND {{ source('tpcds', 'date_dim') }}.d_year IN (1999,
                             1999+1,
                             1999+2)
     AND {{ source('tpcds', 'store') }}.s_number_employees BETWEEN 200 AND 295
   GROUP BY ss_ticket_number,
            ss_customer_sk,
            ss_addr_sk,
            {{ source('tpcds', 'store') }}.s_city) ms,
     {{ source('tpcds', 'customer') }}
WHERE ss_customer_sk = c_customer_sk
ORDER BY c_last_name  NULLS FIRST,
         c_first_name  NULLS FIRST,
         SUBSTRING(s_city,1,30)  NULLS FIRST,
         profit NULLS FIRST,
         ss_ticket_number
LIMIT 100;
