{{
    config(
        materialized = 'table'
    )
}}
WITH Watches AS (
  SELECT 
    customerid,
    symbol,        
    MIN(w_dts)::DATE AS dateplaced,
    MAX(CASE WHEN w_action = 'CNCL' THEN w_dts ELSE NULL END)::DATE AS dateremoved,
    MIN(batchid) AS batchid
  FROM {{ source('tpcdi', 'raw_watchhistory') }}
  GROUP BY customerid, symbol
)

SELECT
  c.sk_customerid,
  s.sk_securityid,
  CAST(strftime(wh.dateplaced, '%Y%m%d') AS BIGINT) AS sk_dateid_dateplaced,
  CAST(strftime(wh.dateremoved, '%Y%m%d') AS BIGINT) AS sk_dateid_dateremoved,
  wh.batchid
FROM Watches wh
JOIN {{ ref('DimSecurity') }} s
  ON s.symbol = wh.symbol
  AND wh.dateplaced >= s.effectivedate 
  AND wh.dateplaced < s.enddate
JOIN {{ ref('DimCustomer') }} c
  ON wh.customerid = c.customerid
  AND wh.dateplaced >= c.effectivedate 
  AND wh.dateplaced < c.enddate
