select region, cause, severity,
    count(distinct outage_id) as outages, sum(duration_min) as total_min,
    sum(cml) as total_cml, avg(duration_hrs) as avg_hrs,
    {{ dagbench_agg_filter('count', '*', 'is_major') }} as major_outages
from {{ ref('outage_enriched') }}
group by region, cause, severity
having sum(cml) > 100
