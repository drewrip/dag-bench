select m.region, va.read_hour::date as anomaly_date,
    {{ dagbench_agg_filter('count', '*', 'va.is_anomaly') }} as anomalies,
    {{ dagbench_agg_filter('count', 'distinct va.meter_id', 'va.is_anomaly') }} as affected_meters,
    cast(round({{ dagbench_agg_filter('avg', 'va.z_score', 'va.is_anomaly') }}, 3) as numeric(38,3)) as avg_z
from {{ ref('voltage_anomalies') }} va
join {{ ref('stg_meters') }} m using (meter_id)
group by m.region, va.read_hour::date
