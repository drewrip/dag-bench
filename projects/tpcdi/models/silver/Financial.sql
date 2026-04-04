{{
    config(
        materialized = 'table',
    )
}}
SELECT 
    dc.sk_companyid, 
    CAST(SUBSTR(value, 1, 4) AS INT) AS fi_year,
    CAST(SUBSTR(value, 5, 1) AS INT) AS fi_qtr,
    strptime(SUBSTR(value, 6, 8), '%Y%m%d')::DATE AS fi_qtr_start_date,
    CAST(SUBSTR(value, 22, 17) AS FLOAT) AS fi_revenue,
    CAST(SUBSTR(value, 39, 17) AS FLOAT) AS fi_net_earn,
    CAST(SUBSTR(value, 56, 12) AS FLOAT) AS fi_basic_eps,
    CAST(SUBSTR(value, 68, 12) AS FLOAT) AS fi_dilut_eps,
    CAST(SUBSTR(value, 80, 12) AS FLOAT) AS fi_margin,
    CAST(SUBSTR(value, 92, 17) AS FLOAT) AS fi_inventory,
    CAST(SUBSTR(value, 109, 17) AS FLOAT) AS fi_assets,
    CAST(SUBSTR(value, 126, 17) AS FLOAT) AS fi_liability,
    CAST(SUBSTR(value, 143, 13) AS BIGINT) AS fi_out_basic,
    CAST(SUBSTR(value, 156, 13) AS BIGINT) AS fi_out_dilut
FROM {{ ref('FinWire') }} f
JOIN {{ ref('DimCompany') }} dc
ON rectype = 'FIN_NAME'
  AND TRIM(SUBSTR(value, 169, 60)) = dc.name
  AND f.recdate >= dc.effectivedate
  AND f.recdate < dc.enddate
UNION ALL
SELECT 
    dc.sk_companyid,
    CAST(SUBSTR(value, 1, 4) AS INT) AS fi_year,
    CAST(SUBSTR(value, 5, 1) AS INT) AS fi_qtr,
    strptime(SUBSTR(value, 6, 8), '%Y%m%d')::DATE AS fi_qtr_start_date,
    CAST(SUBSTR(value, 22, 17) AS FLOAT) AS fi_revenue,
    CAST(SUBSTR(value, 39, 17) AS FLOAT) AS fi_net_earn,
    CAST(SUBSTR(value, 56, 12) AS FLOAT) AS fi_basic_eps,
    CAST(SUBSTR(value, 68, 12) AS FLOAT) AS fi_dilut_eps,
    CAST(SUBSTR(value, 80, 12) AS FLOAT) AS fi_margin,
    CAST(SUBSTR(value, 92, 17) AS FLOAT) AS fi_inventory,
    CAST(SUBSTR(value, 109, 17) AS FLOAT) AS fi_assets,
    CAST(SUBSTR(value, 126, 17) AS FLOAT) AS fi_liability,
    CAST(SUBSTR(value, 143, 13) AS BIGINT) AS fi_out_basic,
    CAST(SUBSTR(value, 156, 13) AS BIGINT) AS fi_out_dilut
FROM {{ ref('FinWire') }} f
JOIN {{ ref('DimCompany') }} dc
ON rectype = 'FIN_COMPANYID'
  AND TRY_CAST(TRIM(SUBSTR(value, 169, 60)) AS BIGINT) = dc.companyid
  AND f.recdate >= dc.effectivedate
  AND f.recdate < dc.enddate
