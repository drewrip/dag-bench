select h.device_id, d.site_id, d.region, d.device_type,
    avg(h.min_battery)                    as avg_min_battery,
    avg(h.error_rate_pct)                 as avg_error_rate,
    sum(h.valid_readings)                 as total_valid,
    sum(h.reading_count)                  as total_readings,
    m.total_actions                       as maintenance_actions,
    m.last_maintenance_ts,
    -- health 0-100: higher is better
    round(
        least(100, greatest(0,
          100
          - avg(h.error_rate_pct)*5
          - case when avg(h.min_battery)<20 then 20 else 0 end
          + coalesce(m.total_actions,0)*2
        ))
    ,2) as health_score
from {{ ref('hourly_device_stats') }} h
join {{ ref('stg_devices') }} d using (device_id)
left join {{ ref('device_maintenance_summary') }} m using (device_id)
group by h.device_id, d.site_id, d.region, d.device_type,
         m.total_actions, m.last_maintenance_ts
