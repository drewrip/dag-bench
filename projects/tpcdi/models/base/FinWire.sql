select
    CASE 
      WHEN SUBSTRING(line, 16, 3) = 'FIN' THEN
        CASE 
          WHEN TRY_CAST(TRIM(SUBSTRING(line, 187, 60)) AS INTEGER) IS NOT NULL THEN 'FIN_COMPANYID'
          ELSE 'FIN_NAME'
        END
      ELSE SUBSTRING(line, 16, 3)
    END AS rectype,
    strptime(SUBSTRING(line, 1, 8), '%Y%m%d')::DATE AS recdate,
    SUBSTRING(line, 19) AS value
from
    {{ source('tpcdi', 'raw_finwire') }}
