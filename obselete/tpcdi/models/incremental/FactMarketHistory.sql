WITH companyfinancials AS (
  SELECT
    f.sk_companyid,
    fi_qtr_start_date,
    SUM(fi_basic_eps) OVER (PARTITION BY f.sk_companyid ORDER BY fi_qtr_start_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - fi_basic_eps AS sum_fi_basic_eps
  FROM {{ ref('Financial') }} f
  JOIN {{ ref('DimCompany') }} d
    ON f.sk_companyid = d.sk_companyid
),
markethistory AS (
  SELECT
    dm.*,
    FIRST_VALUE({'dm_low': dm_low, 'dm_date': dm_date}) OVER (
      PARTITION BY dm_s_symb
      ORDER BY dm_low ASC, dm_date ASC
      ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS fiftytwoweeklow_struct,
    FIRST_VALUE({'dm_high': dm_high, 'dm_date': dm_date}) OVER (
      PARTITION BY dm_s_symb
      ORDER BY dm_high DESC, dm_date ASC
      ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS fiftytwoweekhigh_struct
  FROM {{ source('tpcdi', 'raw_dailymarket') }} dm
)
SELECT
  s.sk_securityid,
  s.sk_companyid,
  CAST(strftime(dm_date, '%Y%m%d') AS BIGINT) AS sk_dateid,
  CASE WHEN sum_fi_basic_eps = 0 THEN 0 ELSE mh.dm_close / sum_fi_basic_eps END AS peratio,
  CASE WHEN mh.dm_close = 0 THEN 0 ELSE (s.dividend / mh.dm_close) / 100 END AS yield,
  fiftytwoweekhigh_struct.dm_high AS fiftytwoweekhigh,
  CAST(strftime(fiftytwoweekhigh_struct.dm_date, '%Y%m%d') AS BIGINT) AS sk_fiftytwoweekhighdate,
  fiftytwoweeklow_struct.dm_low AS fiftytwoweeklow,
  CAST(strftime(fiftytwoweeklow_struct.dm_date, '%Y%m%d') AS BIGINT) AS sk_fiftytwoweeklowdate,
  dm_close AS closeprice,
  dm_high AS dayhigh,
  dm_low AS daylow,
  dm_vol AS volume,
  mh.batchid
FROM markethistory mh
JOIN {{ ref('DimSecurity') }} s 
  ON s.symbol = mh.dm_s_symb
  AND mh.dm_date >= s.effectivedate 
  AND mh.dm_date < s.enddate
LEFT JOIN companyfinancials f 
  ON f.sk_companyid = s.sk_companyid
  AND EXTRACT(QUARTER FROM mh.dm_date) = EXTRACT(QUARTER FROM fi_qtr_start_date)
  AND EXTRACT(YEAR FROM mh.dm_date) = EXTRACT(YEAR FROM fi_qtr_start_date)
