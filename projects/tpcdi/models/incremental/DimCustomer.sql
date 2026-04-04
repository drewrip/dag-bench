{{
    config(
        materialized = 'table'
    )
}}
WITH customer_incremental as (
  select
    *
  from
    {{ source('tpcdi', 'raw_customer') }}
),
Customers AS (
    SELECT
      customerid,
      taxid,
      status,
      lastname,
      firstname,
      middleinitial,
      gender,
      tier,
      dob,
      addressline1,
      addressline2,
      postalcode,
      city,
      stateprov,
      country,
      phone1,
      phone2,
      phone3,
      email1,
      email2,
      lcl_tx_id,
      nat_tx_id,
      1 AS batchid,
      update_ts
    FROM {{ source('tpcdi', 'customermgmt_clean') }}
    WHERE ActionType IN ('NEW', 'INACT', 'UPDCUST')
    UNION ALL
    SELECT
        customerid,
        NULLIF(taxid, '') AS taxid,
        CASE
            WHEN status = 'ACTV' THEN 'Active'
            WHEN status = 'CMPT' THEN 'Completed'
            WHEN status = 'CNCL' THEN 'Canceled'
            WHEN status = 'PNDG' THEN 'Pending'
            WHEN status = 'SBMT' THEN 'Submitted'
            WHEN status = 'INAC' THEN 'Inactive'
            ELSE NULL
        END AS status,
        NULLIF(lastname, '') AS lastname,
        NULLIF(firstname, '') AS firstname,
        NULLIF(middleinitial, '') AS middleinitial,
        NULLIF(gender, '') AS gender,
        tier,
        dob,
        NULLIF(addressline1, '') AS addressline1,
        NULLIF(addressline2, '') AS addressline2,
        NULLIF(postalcode, '') AS postalcode,
        NULLIF(city, '') AS city,
        NULLIF(stateprov, '') AS stateprov,
        country,
        CASE
            WHEN NULLIF(c_local_1, '') IS NOT NULL THEN
                (CASE WHEN NULLIF(c_ctry_1, '') IS NOT NULL THEN '+' || c_ctry_1 || ' ' ELSE '' END) ||
                (CASE WHEN NULLIF(c_area_1, '') IS NOT NULL THEN '(' || c_area_1 || ') ' ELSE '' END) ||
                c_local_1 ||
                COALESCE(c_ext_1, '')
            ELSE NULL
        END AS phone1,
        CASE
            WHEN NULLIF(c_local_2, '') IS NOT NULL THEN
                (CASE WHEN NULLIF(c_ctry_2, '') IS NOT NULL THEN '+' || c_ctry_2 || ' ' ELSE '' END) ||
                (CASE WHEN NULLIF(c_area_2, '') IS NOT NULL THEN '(' || c_area_2 || ') ' ELSE '' END) ||
                c_local_2 ||
                COALESCE(c_ext_2, '')
            ELSE NULL
        END AS phone2,
        CASE
            WHEN NULLIF(c_local_3, '') IS NOT NULL THEN
                (CASE WHEN NULLIF(c_ctry_3, '') IS NOT NULL THEN '+' || c_ctry_3 || ' ' ELSE '' END) ||
                (CASE WHEN NULLIF(c_area_3, '') IS NOT NULL THEN '(' || c_area_3 || ') ' ELSE '' END) ||
                c_local_3 ||
                COALESCE(c_ext_3, '')
            ELSE NULL
        END AS phone3,
        NULLIF(email1, '') AS email1,
        NULLIF(email2, '') AS email2,
        NULLIF(lcl_tx_id, '') AS lcl_tx_id,
        NULLIF(nat_tx_id, '') AS nat_tx_id,
        c.batchid,
        CAST(bd.batchdate AS TIMESTAMP) AS update_ts
    FROM customer_incremental c
    JOIN {{ ref('BatchDate') }} bd
      ON c.batchid = bd.batchid
),
CustomerFinal AS (
    SELECT
      customerid,
      COALESCE(
        taxid,
        LAST_VALUE(taxid IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS taxid,
      status,
      COALESCE(
        lastname,
        LAST_VALUE(lastname IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS lastname,
      COALESCE(
        firstname,
        LAST_VALUE(firstname IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS firstname,
      COALESCE(
        middleinitial,
        LAST_VALUE(middleinitial IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS middleinitial,
      COALESCE(
        gender,
        LAST_VALUE(gender IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS gender,
      COALESCE(
        tier,
        LAST_VALUE(tier IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS tier,
      COALESCE(
        dob,
        LAST_VALUE(dob IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS dob,
      COALESCE(
        addressline1,
        LAST_VALUE(addressline1 IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS addressline1,
      COALESCE(
        addressline2,
        LAST_VALUE(addressline2 IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS addressline2,
      COALESCE(
        postalcode,
        LAST_VALUE(postalcode IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS postalcode,
      COALESCE(
        city,
        LAST_VALUE(city IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS city,
      COALESCE(
        stateprov,
        LAST_VALUE(stateprov IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS stateprov,
      COALESCE(
        country,
        LAST_VALUE(country IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS country,
      COALESCE(
        phone1,
        LAST_VALUE(phone1 IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS phone1,
      COALESCE(
        phone2,
        LAST_VALUE(phone2 IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS phone2,
      COALESCE(
        phone3,
        LAST_VALUE(phone3 IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS phone3,
      COALESCE(
        email1,
        LAST_VALUE(email1 IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS email1,
      COALESCE(
        email2,
        LAST_VALUE(email2 IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS email2,
      COALESCE(
        lcl_tx_id,
        LAST_VALUE(lcl_tx_id IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS lcl_tx_id,
      COALESCE(
        nat_tx_id,
        LAST_VALUE(nat_tx_id IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS nat_tx_id,
      LEAD(update_ts) OVER (PARTITION BY customerid ORDER BY update_ts) IS NULL AS iscurrent,
      DATE(update_ts) AS effectivedate,
      COALESCE(
        LEAD(DATE(update_ts)) OVER (PARTITION BY customerid ORDER BY update_ts),
        DATE('9999-12-31')
      ) AS enddate,
      batchid
    FROM Customers
)
SELECT
  strftime(c.effectivedate, '%Y%m%d') || c.customerid::VARCHAR AS sk_customerid,
  c.customerid,
  c.taxid,
  c.status,
  c.lastname,
  c.firstname,
  c.middleinitial,
  CASE WHEN c.gender IN ('M', 'F') THEN c.gender ELSE 'U' END AS gender,
  c.tier,
  c.dob,
  c.addressline1,
  c.addressline2,
  c.postalcode,
  c.city,
  c.stateprov,
  c.country,
  c.phone1,
  c.phone2,
  c.phone3,
  c.email1,
  c.email2,
  r_nat.tx_name AS nationaltaxratedesc,
  r_nat.tx_rate AS nationaltaxrate,
  r_lcl.tx_name AS localtaxratedesc,
  r_lcl.tx_rate AS localtaxrate,
  p.agencyid,
  p.creditrating,
  p.networth,
  p.marketingnameplate,
  c.iscurrent,
  c.batchid,
  c.effectivedate,
  c.enddate
FROM CustomerFinal c
JOIN {{ ref('TaxRate') }} r_lcl
  ON c.lcl_tx_id = r_lcl.tx_id
JOIN {{ ref('TaxRate') }} r_nat
  ON c.nat_tx_id = r_nat.tx_id
LEFT JOIN {{ ref('ProspectIncremental') }} p
  ON UPPER(p.lastname) = UPPER(c.lastname)
  AND UPPER(p.firstname) = UPPER(c.firstname)
  AND UPPER(p.addressline1) = UPPER(c.addressline1)
  AND UPPER(COALESCE(p.addressline2, '')) = UPPER(COALESCE(c.addressline2, ''))
  AND UPPER(p.postalcode) = UPPER(c.postalcode)
WHERE c.effectivedate < c.enddate
