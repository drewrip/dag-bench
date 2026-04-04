{{
    config(
        materialized = 'table'
    )
}}
with dailytotals as (
    SELECT
        accountid,
        DATE(ct_dts) datevalue,
        sum(ct_amt) account_daily_total,
        batchid
    FROM {{ source('tpcdi', 'raw_cashtransaction') }}
    GROUP BY
        accountid,
        datevalue,
        batchid
)
SELECT
  a.sk_customerid, 
  a.sk_accountid, 
  CAST(strftime(datevalue, '%Y%m%d') AS BIGINT) sk_dateid,
  sum(account_daily_total) OVER (partition by c.accountid order by c.datevalue) cash,
  c.batchid
FROM dailytotals c
JOIN {{ ref( 'DimAccount') }} a
  ON 
    c.accountid = a.accountid
    AND c.datevalue >= a.effectivedate 
    AND c.datevalue < a.enddate
