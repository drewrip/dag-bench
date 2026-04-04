
SELECT channel,
       {{ source('tpcds', 'item') }},
       return_ratio,
       return_rank,
       currency_rank
FROM
  (SELECT 'web' AS channel,
          web.{{ source('tpcds', 'item') }},
          web.return_ratio,
          web.return_rank,
          web.currency_rank
   FROM
     (SELECT {{ source('tpcds', 'item') }},
             return_ratio,
             currency_ratio,
             rank() OVER (
                          ORDER BY return_ratio) AS return_rank,
                         rank() OVER (
                                      ORDER BY currency_ratio) AS currency_rank
      FROM
        (SELECT ws.ws_item_sk AS {{ source('tpcds', 'item') }},
                (cast(sum(coalesce(wr.wr_return_quantity,0)) AS decimal(15,4))/ cast(sum(coalesce(ws.ws_quantity,0)) AS decimal(15,4))) AS return_ratio,
                (cast(sum(coalesce(wr.wr_return_amt,0)) AS decimal(15,4))/ cast(sum(coalesce(ws.ws_net_paid,0)) AS decimal(15,4))) AS currency_ratio
         FROM {{ source('tpcds', 'web_sales') }} ws
         LEFT OUTER JOIN {{ source('tpcds', 'web_returns') }} wr ON (ws.ws_order_number = wr.wr_order_number
                                            AND ws.ws_item_sk = wr.wr_item_sk) ,{{ source('tpcds', 'date_dim') }}
         WHERE wr.wr_return_amt > 10000
           AND ws.ws_net_profit > 1
           AND ws.ws_net_paid > 0
           AND ws.ws_quantity > 0
           AND ws_sold_date_sk = d_date_sk
           AND d_year = 2001
           AND d_moy = 12
         GROUP BY ws.ws_item_sk) in_web) web
   WHERE (web.return_rank <= 10
          OR web.currency_rank <= 10)
   UNION SELECT 'catalog' AS channel,
                catalog.{{ source('tpcds', 'item') }},
                catalog.return_ratio,
                catalog.return_rank,
                catalog.currency_rank
   FROM
     (SELECT {{ source('tpcds', 'item') }},
             return_ratio,
             currency_ratio,
             rank() OVER (
                          ORDER BY return_ratio) AS return_rank,
                         rank() OVER (
                                      ORDER BY currency_ratio) AS currency_rank
      FROM
        (SELECT cs.cs_item_sk AS {{ source('tpcds', 'item') }},
                (cast(sum(coalesce(cr.cr_return_quantity,0)) AS decimal(15,4))/ cast(sum(coalesce(cs.cs_quantity,0)) AS decimal(15,4))) AS return_ratio,
                (cast(sum(coalesce(cr.cr_return_amount,0)) AS decimal(15,4))/ cast(sum(coalesce(cs.cs_net_paid,0)) AS decimal(15,4))) AS currency_ratio
         FROM {{ source('tpcds', 'catalog_sales') }} cs
         LEFT OUTER JOIN {{ source('tpcds', 'catalog_returns') }} cr ON (cs.cs_order_number = cr.cr_order_number
                                                AND cs.cs_item_sk = cr.cr_item_sk) ,{{ source('tpcds', 'date_dim') }}
         WHERE cr.cr_return_amount > 10000
           AND cs.cs_net_profit > 1
           AND cs.cs_net_paid > 0
           AND cs.cs_quantity > 0
           AND cs_sold_date_sk = d_date_sk
           AND d_year = 2001
           AND d_moy = 12
         GROUP BY cs.cs_item_sk) in_cat) CATALOG
   WHERE (catalog.return_rank <= 10
          OR catalog.currency_rank <=10)
   UNION SELECT '{{ source('tpcds', 'store') }}' AS channel,
                {{ source('tpcds', 'store') }}.{{ source('tpcds', 'item') }},
                {{ source('tpcds', 'store') }}.return_ratio,
                {{ source('tpcds', 'store') }}.return_rank,
                {{ source('tpcds', 'store') }}.currency_rank
   FROM
     (SELECT {{ source('tpcds', 'item') }},
             return_ratio,
             currency_ratio,
             rank() OVER (
                          ORDER BY return_ratio) AS return_rank,
                         rank() OVER (
                                      ORDER BY currency_ratio) AS currency_rank
      FROM
        (SELECT sts.ss_item_sk AS {{ source('tpcds', 'item') }},
                (cast(sum(coalesce(sr.sr_return_quantity,0)) AS decimal(15,4))/cast(sum(coalesce(sts.ss_quantity,0)) AS decimal(15,4))) AS return_ratio,
                (cast(sum(coalesce(sr.sr_return_amt,0)) AS decimal(15,4))/cast(sum(coalesce(sts.ss_net_paid,0)) AS decimal(15,4))) AS currency_ratio
         FROM {{ source('tpcds', 'store_sales') }} sts
         LEFT OUTER JOIN {{ source('tpcds', 'store_returns') }} sr ON (sts.ss_ticket_number = sr.sr_ticket_number
                                              AND sts.ss_item_sk = sr.sr_item_sk) ,{{ source('tpcds', 'date_dim') }}
         WHERE sr.sr_return_amt > 10000
           AND sts.ss_net_profit > 1
           AND sts.ss_net_paid > 0
           AND sts.ss_quantity > 0
           AND ss_sold_date_sk = d_date_sk
           AND d_year = 2001
           AND d_moy = 12
         GROUP BY sts.ss_item_sk) in_store) {{ source('tpcds', 'store') }}
   WHERE ({{ source('tpcds', 'store') }}.return_rank <= 10
          OR {{ source('tpcds', 'store') }}.currency_rank <= 10) ) sq1
ORDER BY 1 NULLS FIRST,
         4 NULLS FIRST,
         5 NULLS FIRST,
         2 NULLS FIRST
LIMIT 100;

