
SELECT c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
FROM
  (SELECT ss_ticket_number,
          ss_customer_sk,
          count(*) cnt
   FROM {{ source('tpcds', 'store_sales') }},
        {{ source('tpcds', 'date_dim') }},
        {{ source('tpcds', 'store') }},
        {{ source('tpcds', 'household_demographics') }}
   WHERE {{ source('tpcds', 'store_sales') }}.ss_sold_date_sk = {{ source('tpcds', 'date_dim') }}.d_date_sk
     AND {{ source('tpcds', 'store_sales') }}.ss_store_sk = {{ source('tpcds', 'store') }}.s_store_sk
     AND {{ source('tpcds', 'store_sales') }}.ss_hdemo_sk = {{ source('tpcds', 'household_demographics') }}.hd_demo_sk
     AND {{ source('tpcds', 'date_dim') }}.d_dom BETWEEN 1 AND 2
     AND ({{ source('tpcds', 'household_demographics') }}.hd_buy_potential = 'Unknown'
          OR {{ source('tpcds', 'household_demographics') }}.hd_buy_potential = '>10000')
     AND {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count > 0
     AND CASE
             WHEN {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count > 0 THEN ({{ source('tpcds', 'household_demographics') }}.hd_dep_count*1.000)/ {{ source('tpcds', 'household_demographics') }}.hd_vehicle_count
             ELSE NULL
         END > 1
     AND {{ source('tpcds', 'date_dim') }}.d_year IN (1999,
                             1999+1,
                             1999+2)
     AND {{ source('tpcds', 'store') }}.s_county IN ('Orange County',
                            'Bronx County',
                            'Franklin Parish',
                            'Williamson County')
   GROUP BY ss_ticket_number,
            ss_customer_sk) dj,
     {{ source('tpcds', 'customer') }}
WHERE ss_customer_sk = c_customer_sk
  AND cnt BETWEEN 1 AND 5
ORDER BY cnt DESC,
         c_last_name ASC;

