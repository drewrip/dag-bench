
SELECT c_last_name,
       c_first_name,
       ca_city,
       bought_city,
       ss_ticket_number,
       amt,
       profit
FROM
  (SELECT ss_ticket_number,
          ss_customer_sk,
          ca_city bought_city,
          sum(ss_coupon_amt) amt,
          sum(ss_net_profit) profit
   FROM {{ source('tpcds', 'store_sales') }},
        {{ source('tpcds', 'date_dim') }},
        {{ source('tpcds', 'store') }},
        {{ source('tpcds', 'household_demographics') }},
        {{ source('tpcds', 'customer_address') }}
   WHERE {{ source('tpcds', 'store_sales') }}.ss_sold_date_sk = {{ source('tpcds', 'date_dim') }}.d_date_sk
     AND {{ source('tpcds', 'store_sales') }}.ss_store_sk = {{ source('tpcds', 'store') }}.s_store_sk
     AND {{ source('tpcds', 'store_sales') }}.ss_hdemo_sk = {{ source('tpcds', 'household_demographics') }}.hd_demo_sk
     AND {{ source('tpcds', 'store_sales') }}.ss_addr_sk = {{ source('tpcds', 'customer_address') }}.ca_address_sk
     AND ({{ source('tpcds', 'household_demographics') }}.hd_dep_count = 4
          OR {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count= 3)
     AND {{ source('tpcds', 'date_dim') }}.d_dow IN (6,
                            0)
     AND {{ source('tpcds', 'date_dim') }}.d_year IN (1999,
                             1999+1,
                             1999+2)
     AND {{ source('tpcds', 'store') }}.s_city IN ('Fairview',
                          'Midway')
   GROUP BY ss_ticket_number,
            ss_customer_sk,
            ss_addr_sk,
            ca_city) dn,
     {{ source('tpcds', 'customer') }},
     {{ source('tpcds', 'customer_address') }} current_addr
WHERE ss_customer_sk = c_customer_sk
  AND {{ source('tpcds', 'customer') }}.c_current_addr_sk = current_addr.ca_address_sk
  AND current_addr.ca_city <> bought_city
ORDER BY c_last_name NULLS FIRST,
         c_first_name NULLS FIRST,
         ca_city NULLS FIRST,
         bought_city NULLS FIRST,
         ss_ticket_number NULLS FIRST
LIMIT 100;

