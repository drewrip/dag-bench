WITH Holdings as (
    SELECT
        *
    FROM
        {{ source('tpcdi', 'raw_holdinghistory') }}
)
SELECT 
  hh_h_t_id tradeid,
  hh_t_id currenttradeid,
  sk_customerid,
  sk_accountid,
  sk_securityid,
  sk_companyid,
  sk_closedateid sk_dateid,
  sk_closetimeid sk_timeid,
  tradeprice currentprice,
  hh_after_qty currentholding,
  hh.batchid
FROM  Holdings hh
JOIN {{ ref('DimTrade') }} dt
  ON hh.hh_t_id = dt.tradeid
