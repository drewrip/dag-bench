SELECT dt.d_year,
       {{ source('tpcds', 'item') }}.i_brand_id brand_id,
       {{ source('tpcds', 'item') }}.i_brand brand,
       sum(ss_ext_sales_price) sum_agg
FROM {{ source('tpcds', 'date_dim') }} dt,
     {{ source('tpcds', 'store_sales') }},
     {{ source('tpcds', 'item') }}
WHERE dt.d_date_sk = {{ source('tpcds', 'store_sales') }}.ss_sold_date_sk
  AND {{ source('tpcds', 'store_sales') }}.ss_item_sk = {{ source('tpcds', 'item') }}.i_item_sk
  AND {{ source('tpcds', 'item') }}.i_manufact_id = 128
  AND dt.d_moy=11
GROUP BY dt.d_year,
         {{ source('tpcds', 'item') }}.i_brand,
         {{ source('tpcds', 'item') }}.i_brand_id
ORDER BY dt.d_year,
         sum_agg DESC,
         brand_id
LIMIT 100