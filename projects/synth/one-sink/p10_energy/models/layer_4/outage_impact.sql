select region, cause, severity,
    count(distinct outage_id)            as outage_count,
    sum(duration_min)                    as total_duration_min,
    sum(customer_minutes_lost)           as total_cml,
    avg(duration_hours)                  as avg_duration_hours,
    count(*) filter (where is_major)     as major_outages
from {{ ref('outage_enriched') }}
group by region, cause, severity
