select h.device_id, d.site_id, d.region, d.device_type,
    avg(h.min_battery) as avg_min_battery,
    avg(h.errors*100.0/nullif(h.reading_count,0)) as avg_error_pct,
    sum(h.valid_count) as total_valid,
    m.total_actions, m.last_maintenance,
    least(100,greatest(0,round(100
        - avg(h.errors*100.0/nullif(h.reading_count,0))*5
        - case when avg(h.min_battery)<20 then 20 else 0 end
        + coalesce(m.total_actions,0)*2,2))) as health_score
from {{ ref('hourly_stats') }} h
join {{ ref('stg_devices') }} d using (device_id)
left join {{ ref('device_maintenance_agg') }} m using (device_id)
group by h.device_id,d.site_id,d.region,d.device_type,m.total_actions,m.last_maintenance
