select region, cause, severity, outages, total_min, total_cml, avg_hrs, major_outages,
    round(cast(total_cml*100.0/nullif(sum(total_cml) over(),0) as numeric(18,4)), 2) as cml_share_pct,
    current_timestamp as report_ts
from {{ ref('outage_by_region') }}
order by total_cml desc nulls last
