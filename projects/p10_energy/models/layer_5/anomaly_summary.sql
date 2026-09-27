select m.region, va.read_hour::date as anomaly_date,
    count(*) filter (where va.is_anomaly) as anomalies,
    count(distinct va.meter_id) filter (where va.is_anomaly) as affected_meters,
    cast(round(avg(va.z_score) filter (where va.is_anomaly), 3) as numeric(38,3)) as avg_z
from {{ ref('voltage_anomalies') }} va
join {{ ref('stg_meters') }} m using (meter_id)
group by m.region, va.read_hour::date
