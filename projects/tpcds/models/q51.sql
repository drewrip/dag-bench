WITH web_v1 AS
  (SELECT ws_item_sk item_sk,
          d_date,
          sum(sum(ws_sales_price)) OVER (PARTITION BY ws_item_sk
                                         ORDER BY d_date ROWS BETWEEN unbounded preceding AND CURRENT ROW) cume_sales
   FROM {{ source('tpcds', 'web_sales') }},
        {{ source('tpcds', 'date_dim') }}
   WHERE ws_sold_date_sk=d_date_sk
     AND d_month_seq BETWEEN 1200 AND 1200+11
     AND ws_item_sk IS NOT NULL
   GROUP BY ws_item_sk,
            d_date),
     store_v1 AS
  (SELECT ss_item_sk item_sk,
          d_date,
          sum(sum(ss_sales_price)) OVER (PARTITION BY ss_item_sk
                                         ORDER BY d_date ROWS BETWEEN unbounded preceding AND CURRENT ROW) cume_sales
   FROM {{ source('tpcds', 'store_sales') }},
        {{ source('tpcds', 'date_dim') }}
   WHERE ss_sold_date_sk=d_date_sk
     AND d_month_seq BETWEEN 1200 AND 1200+11
     AND ss_item_sk IS NOT NULL
   GROUP BY ss_item_sk,
            d_date)
SELECT *
FROM
  (SELECT item_sk,
          d_date,
          {{ source('tpcds', 'web_sales') }},
          {{ source('tpcds', 'store_sales') }},
          max({{ source('tpcds', 'web_sales') }}) OVER (PARTITION BY item_sk
                               ORDER BY d_date ROWS BETWEEN unbounded preceding AND CURRENT ROW) web_cumulative,
                              max({{ source('tpcds', 'store_sales') }}) OVER (PARTITION BY item_sk
                                                     ORDER BY d_date ROWS BETWEEN unbounded preceding AND CURRENT ROW) store_cumulative
   FROM
     (SELECT CASE
                 WHEN web.item_sk IS NOT NULL THEN web.item_sk
                 ELSE {{ source('tpcds', 'store') }}.item_sk
             END item_sk,
             CASE
                 WHEN web.d_date IS NOT NULL THEN web.d_date
                 ELSE {{ source('tpcds', 'store') }}.d_date
             END d_date,
             web.cume_sales {{ source('tpcds', 'web_sales') }},
             {{ source('tpcds', 'store') }}.cume_sales {{ source('tpcds', 'store_sales') }}
      FROM web_v1 web
      FULL OUTER JOIN store_v1 {{ source('tpcds', 'store') }} ON (web.item_sk = {{ source('tpcds', 'store') }}.item_sk
                                         AND web.d_date = {{ source('tpcds', 'store') }}.d_date))x)y
WHERE web_cumulative > store_cumulative
ORDER BY item_sk NULLS FIRST,
         d_date NULLS FIRST
LIMIT 100;

