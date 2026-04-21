select
    CASE 
      WHEN SUBSTRING(line, 16, 3) = 'FIN' THEN
        CASE 
          WHEN {{ dbt.safe_cast("TRIM(SUBSTRING(line, 187, 60))", api.Column.translate_type("INTEGER")) }} IS NOT NULL THEN 'FIN_COMPANYID'
          ELSE 'FIN_NAME'
        END
      ELSE SUBSTRING(line, 16, 3)
    END AS rectype,
    {% if target.type == "duckdb" %}
    strptime(SUBSTRING(line, 1, 8), '%Y%m%d')::DATE AS recdate,
    {% else %}
    TO_DATE(SUBSTRING(line, 1, 8), 'YYYYMMDD')::DATE AS recdate,
    {% endif %}
    SUBSTRING(line, 19) AS value
from
    {{ source('tpcdi', 'raw_finwire') }}
