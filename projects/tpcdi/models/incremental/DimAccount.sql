WITH AccountIncremental AS (
  select
    *
  from
    {{ source('tpcdi', 'raw_account') }}
),
Account AS (
  SELECT
    accountid,
    customerid,
    accountdesc,
    taxstatus,
    brokerid,
    status,
    update_ts,
    1 AS batchid
  FROM {{ source('tpcdi', 'customermgmt_clean') }} c
  WHERE ActionType NOT IN ('UPDCUST', 'INACT')
  UNION ALL
  SELECT
    accountid,
    customerid,
    accountdesc,
    taxstatus,
    brokerid,
    CASE
      WHEN a.status = 'ACTV' THEN 'Active'
      WHEN a.status = 'CMPT' THEN 'Completed'
      WHEN a.status = 'CNCL' THEN 'Canceled'
      WHEN a.status = 'PNDG' THEN 'Pending'
      WHEN a.status = 'SBMT' THEN 'Submitted'
      WHEN a.status = 'INAC' THEN 'Inactive'
      ELSE NULL
    END AS status,
    CAST(bd.batchdate AS TIMESTAMP) AS update_ts,
    a.batchid
  FROM AccountIncremental a
  JOIN {{ ref('BatchDate') }} bd
    ON a.batchid = bd.batchid
),
AccountFinal AS (
  SELECT
    accountid,
    customerid,
    COALESCE(accountdesc, LAST_VALUE(accountdesc IGNORE NULLS) OVER (PARTITION BY accountid ORDER BY update_ts)) AS accountdesc,
    COALESCE(taxstatus, LAST_VALUE(taxstatus IGNORE NULLS) OVER (PARTITION BY accountid ORDER BY update_ts )) AS taxstatus,
    COALESCE(brokerid, LAST_VALUE(brokerid IGNORE NULLS) OVER (PARTITION BY accountid ORDER BY update_ts )) AS brokerid,
    COALESCE(status, LAST_VALUE(status IGNORE NULLS) OVER (PARTITION BY accountid ORDER BY update_ts )) AS status,
    DATE(update_ts) AS effectivedate,
    COALESCE(
      LEAD(DATE(update_ts)) OVER (PARTITION BY accountid ORDER BY update_ts),
      DATE('9999-12-31')
    ) AS enddate,
    batchid
  FROM Account
),
AccountCustomerUpdates AS (
  SELECT
    a.accountid,
    a.accountdesc,
    a.taxstatus,
    a.brokerid,
    a.status,
    c.sk_customerid,
    CASE
      WHEN a.effectivedate < c.effectivedate THEN c.effectivedate
      ELSE a.effectivedate
    END AS effectivedate,
    
    CASE
      WHEN a.enddate > c.enddate THEN c.enddate
      ELSE a.enddate
    END AS enddate,
    a.batchid
  FROM AccountFinal a
  FULL OUTER JOIN {{ ref('DimCustomer') }} c
     ON a.customerid = c.customerid
     AND c.enddate > a.effectivedate
     AND c.effectivedate < a.enddate
  WHERE a.effectivedate < a.enddate 
)
SELECT
  CAST(strftime(a.effectivedate, '%Y%m%d') || a.accountid::VARCHAR AS BIGINT) AS sk_accountid,
  a.accountid,
  b.sk_brokerid,
  a.sk_customerid,
  a.accountdesc,
  a.taxstatus,
  a.status,
  (a.enddate = DATE('9999-12-31')) AS iscurrent,
  a.batchid,
  a.effectivedate,
  a.enddate
FROM AccountCustomerUpdates a
JOIN {{ ref('DimBroker') }} b 
   ON a.brokerid = b.brokerid
