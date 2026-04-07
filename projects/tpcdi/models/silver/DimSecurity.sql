WITH SEC AS (
  SELECT
    recdate AS effectivedate,
    TRIM(SUBSTRING(value, 1, 15)) AS Symbol,
    TRIM(SUBSTRING(value, 16, 6)) AS issue,
    TRIM(SUBSTRING(value, 22, 4)) AS Status,
    TRIM(SUBSTRING(value, 26, 70)) AS Name,
    TRIM(SUBSTRING(value, 96, 6)) AS exchangeid,
    CAST(SUBSTRING(value, 102, 13) AS BIGINT) AS sharesoutstanding,
    strptime(SUBSTRING(value, 115, 8), '%Y%m%d')::DATE AS firsttrade,
    strptime(SUBSTRING(value, 123, 8), '%Y%m%d')::DATE AS firsttradeonexchange,
    CAST(SUBSTRING(value, 131, 12) AS FLOAT) AS Dividend,
    TRIM(SUBSTRING(value, 143, 60)) AS conameorcik
  FROM {{ ref('FinWire') }}
  WHERE rectype = 'SEC'
),
dc AS (
  SELECT 
    sk_companyid,
    name AS conameorcik,
    EffectiveDate,
    EndDate
  FROM {{ ref('DimCompany') }}
  UNION ALL
  SELECT 
    sk_companyid,
    CAST(companyid AS VARCHAR) AS conameorcik,
    EffectiveDate,
    EndDate
  FROM {{ ref('DimCompany') }}
),
SEC_prep AS (
  SELECT 
    SEC.* EXCLUDE (Status, conameorcik),
    COALESCE(CAST(TRY_CAST(conameorcik AS BIGINT) AS VARCHAR), conameorcik) AS conameorcik,
    CASE
      WHEN Status = 'ACTV' THEN 'Active'
      WHEN Status = 'CMPT' THEN 'Completed'
      WHEN Status = 'CNCL' THEN 'Canceled'
      WHEN Status = 'PNDG' THEN 'Pending'
      WHEN Status = 'SBMT' THEN 'Submitted'
      WHEN Status = 'INAC' THEN 'Inactive'
      ELSE NULL
    END AS status,
    coalesce(
        lead(effectivedate) OVER (
          PARTITION BY symbol
          ORDER BY effectivedate),
          CAST('9999-12-31' AS DATE)
    ) AS enddate
  FROM SEC
),
SEC_final AS (
  SELECT
    SEC.Symbol,
    SEC.issue,
    SEC.status,
    SEC.Name,
    SEC.exchangeid,
    SEC.sharesoutstanding,
    SEC.firsttrade,
    SEC.firsttradeonexchange,
    SEC.Dividend,
    IF(SEC.effectivedate < dc.effectivedate, dc.effectivedate, SEC.effectivedate) AS effectivedate,
    IF(SEC.enddate > dc.enddate, dc.enddate, SEC.enddate) AS enddate,
    dc.sk_companyid
  FROM SEC_prep SEC
  JOIN dc 
    ON SEC.conameorcik = dc.conameorcik
    AND SEC.effectivedate < dc.EndDate
    AND SEC.enddate > dc.EffectiveDate
)
SELECT 
  ROW_NUMBER() OVER(ORDER BY effectivedate) AS sk_securityid,
  Symbol,
  issue,
  status,
  Name,
  exchangeid,
  sk_companyid,
  sharesoutstanding,
  firsttrade,
  firsttradeonexchange,
  Dividend,
  (enddate = CAST('9999-12-31' AS DATE)) AS iscurrent,
  1 AS batchid,
  effectivedate,
  enddate
FROM SEC_final
WHERE effectivedate < enddate
