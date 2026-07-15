select region, cause, severity, outages, total_min, total_cml, avg_hrs, major_outages,
    round(CAST((total_cml*100.0/nullif(sum(total_cml) over(),0)) AS NUMERIC),2) as cml_share_pct,
    current_timestamp as report_ts
from {{ ref('outage_by_region') }}
order by total_cml desc
