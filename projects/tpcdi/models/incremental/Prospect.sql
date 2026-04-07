WITH cust AS (
  SELECT 
    lastname,
    firstname,
    addressline1,
    addressline2,
    postalcode
  FROM {{ref("DimCustomer")}}
  WHERE iscurrent
)
SELECT 
  p.agencyid,
  CAST(strftime(recdate.batchdate, '%Y%m%d') AS BIGINT) AS sk_recorddateid,
  CAST(strftime(origdate.batchdate, '%Y%m%d') AS BIGINT) AS sk_updatedateid,
  p.batchid,
  CASE WHEN c.lastname IS NOT NULL THEN TRUE ELSE FALSE END AS iscustomer,
  p.lastname,
  p.firstname,
  p.middleinitial,
  p.gender,
  p.addressline1,
  p.addressline2,
  p.postalcode,
  p.city,
  p.state,
  p.country,
  p.phone,
  p.income,
  p.numbercars,
  p.numberchildren,
  p.maritalstatus,
  p.age,
  p.creditrating,
  p.ownorrentflag,
  p.employer,
  p.numbercreditcards,
  p.networth,
  p.marketingnameplate
FROM {{ref("ProspectIncremental")}} p
JOIN {{ref("BatchDate")}} recdate
  ON p.recordbatchid = recdate.batchid
JOIN  {{ref("BatchDate")}} origdate
  ON p.batchid = origdate.batchid
LEFT JOIN cust c
  ON UPPER(p.lastname) = UPPER(c.lastname)
  AND UPPER(p.firstname) = UPPER(c.firstname)
  AND UPPER(p.addressline1) = UPPER(c.addressline1)
  AND UPPER(COALESCE(p.addressline2, '')) = UPPER(COALESCE(c.addressline2, ''))
  AND UPPER(p.postalcode) = UPPER(c.postalcode)
