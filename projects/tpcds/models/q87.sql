SELECT count(*)
FROM ((SELECT DISTINCT c_last_name,
                         c_first_name,
                         d_date
         FROM {{ source('tpcds', 'store_sales') }},
              {{ source('tpcds', 'date_dim') }},
              {{ source('tpcds', 'customer') }}
         WHERE {{ source('tpcds', 'store_sales') }}.ss_sold_date_sk = {{ source('tpcds', 'date_dim') }}.d_date_sk
           AND {{ source('tpcds', 'store_sales') }}.ss_customer_sk = {{ source('tpcds', 'customer') }}.c_customer_sk
           AND d_month_seq BETWEEN 1200 AND 1200+11)
      EXCEPT
        (SELECT DISTINCT c_last_name,
                         c_first_name,
                         d_date
         FROM {{ source('tpcds', 'catalog_sales') }},
              {{ source('tpcds', 'date_dim') }},
              {{ source('tpcds', 'customer') }}
         WHERE {{ source('tpcds', 'catalog_sales') }}.cs_sold_date_sk = {{ source('tpcds', 'date_dim') }}.d_date_sk
           AND {{ source('tpcds', 'catalog_sales') }}.cs_bill_customer_sk = {{ source('tpcds', 'customer') }}.c_customer_sk
           AND d_month_seq BETWEEN 1200 AND 1200+11)
      EXCEPT
        (SELECT DISTINCT c_last_name,
                         c_first_name,
                         d_date
         FROM {{ source('tpcds', 'web_sales') }},
              {{ source('tpcds', 'date_dim') }},
              {{ source('tpcds', 'customer') }}
         WHERE {{ source('tpcds', 'web_sales') }}.ws_sold_date_sk = {{ source('tpcds', 'date_dim') }}.d_date_sk
           AND {{ source('tpcds', 'web_sales') }}.ws_bill_customer_sk = {{ source('tpcds', 'customer') }}.c_customer_sk
           AND d_month_seq BETWEEN 1200 AND 1200+11)) cool_cust