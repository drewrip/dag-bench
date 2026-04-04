select region, cause, severity,
    count(distinct outage_id) as outages, sum(duration_min) as total_min,
    sum(cml) as total_cml, avg(duration_hrs) as avg_hrs,
    count(*) filter (where is_major) as major_outages
from {{ ref('outage_enriched') }}
group by region, cause, severity
