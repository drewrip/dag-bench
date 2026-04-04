WITH ssr AS
  (SELECT s_store_id AS store_id,
          sum(ss_ext_sales_price) AS sales,
          sum(coalesce(sr_return_amt, 0)) AS returns_,
          sum(ss_net_profit - coalesce(sr_net_loss, 0)) AS profit
   FROM {{ source('tpcds', 'store_sales') }}
   LEFT OUTER JOIN {{ source('tpcds', 'store_returns') }} ON (ss_item_sk = sr_item_sk
                                     AND ss_ticket_number = sr_ticket_number), {{ source('tpcds', 'date_dim') }},
                                                                               {{ source('tpcds', 'store') }},
                                                                               {{ source('tpcds', 'item') }},
                                                                               {{ source('tpcds', 'promotion') }}
   WHERE ss_sold_date_sk = d_date_sk
     AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
     AND ss_store_sk = s_store_sk
     AND ss_item_sk = i_item_sk
     AND i_current_price > 50
     AND ss_promo_sk = p_promo_sk
     AND p_channel_tv = 'N'
   GROUP BY s_store_id) ,
     csr AS
  (SELECT cp_catalog_page_id AS catalog_page_id,
          sum(cs_ext_sales_price) AS sales,
          sum(coalesce(cr_return_amount, 0)) AS returns_,
          sum(cs_net_profit - coalesce(cr_net_loss, 0)) AS profit
   FROM {{ source('tpcds', 'catalog_sales') }}
   LEFT OUTER JOIN {{ source('tpcds', 'catalog_returns') }} ON (cs_item_sk = cr_item_sk
                                       AND cs_order_number = cr_order_number), {{ source('tpcds', 'date_dim') }},
                                                                               {{ source('tpcds', 'catalog_page') }},
                                                                               {{ source('tpcds', 'item') }},
                                                                               {{ source('tpcds', 'promotion') }}
   WHERE cs_sold_date_sk = d_date_sk
     AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
     AND cs_catalog_page_sk = cp_catalog_page_sk
     AND cs_item_sk = i_item_sk
     AND i_current_price > 50
     AND cs_promo_sk = p_promo_sk
     AND p_channel_tv = 'N'
   GROUP BY cp_catalog_page_id) ,
     wsr AS
  (SELECT web_site_id,
          sum(ws_ext_sales_price) AS sales,
          sum(coalesce(wr_return_amt, 0)) AS returns_,
          sum(ws_net_profit - coalesce(wr_net_loss, 0)) AS profit
   FROM {{ source('tpcds', 'web_sales') }}
   LEFT OUTER JOIN {{ source('tpcds', 'web_returns') }} ON (ws_item_sk = wr_item_sk
                                   AND ws_order_number = wr_order_number), {{ source('tpcds', 'date_dim') }},
                                                                           {{ source('tpcds', 'web_site') }},
                                                                           {{ source('tpcds', 'item') }},
                                                                           {{ source('tpcds', 'promotion') }}
   WHERE ws_sold_date_sk = d_date_sk
     AND d_date BETWEEN cast('2000-08-23' AS date) AND cast('2000-09-22' AS date)
     AND ws_web_site_sk = web_site_sk
     AND ws_item_sk = i_item_sk
     AND i_current_price > 50
     AND ws_promo_sk = p_promo_sk
     AND p_channel_tv = 'N'
   GROUP BY web_site_id)
SELECT channel ,
       id ,
       sum(sales) AS sales ,
       sum(returns_) AS returns_ ,
       sum(profit) AS profit
FROM
  (SELECT '{{ source('tpcds', 'store') }} channel' AS channel ,
          concat('{{ source('tpcds', 'store') }}', store_id) AS id ,
          sales ,
          returns_ ,
          profit
   FROM ssr
   UNION ALL SELECT 'catalog channel' AS channel ,
                    concat('{{ source('tpcds', 'catalog_page') }}', catalog_page_id) AS id ,
                    sales ,
                    returns_ ,
                    profit
   FROM csr
   UNION ALL SELECT 'web channel' AS channel ,
                    concat('{{ source('tpcds', 'web_site') }}', web_site_id) AS id ,
                    sales ,
                    returns_ ,
                    profit
   FROM wsr ) x
GROUP BY ROLLUP (channel,
                 id)
ORDER BY channel NULLS FIRST,
         id NULLS FIRST
LIMIT 100