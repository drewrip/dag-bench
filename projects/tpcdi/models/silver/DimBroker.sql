{{
    config(
        materialized = 'table'
    )
}}
SELECT
  sk_brokerid,
  sk_brokerid as brokerid,
  managerid,
  firstname,
  lastname,
  middleinitial,
  branch,
  office,
  phone,
  true iscurrent,
  1 batchid,
  (SELECT min(datevalue) FROM {{ ref('DimDate') }}) effectivedate,
  CAST('9999-12-31' AS DATE) enddate
FROM
  {{ source('tpcdi', 'raw_hr') }}
WHERE jobcode = 314
