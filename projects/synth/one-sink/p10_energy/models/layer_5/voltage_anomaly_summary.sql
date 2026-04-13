select m.region, av.read_hour::date as anomaly_date,
    count(*) filter (where is_voltage_anomaly) as anomaly_count,
    count(distinct av.meter_id) filter (where is_voltage_anomaly) as affected_meters,
    round(avg(av.z_score) filter (where is_voltage_anomaly),3) as avg_z_score
from {{ ref('anomaly_voltage') }} av
join {{ ref('stg_meters') }} m using (meter_id)
group by m.region, av.read_hour::date
