with hourly_device_stats as (
    select * from {{ ref('hourly_device_stats') }}
),

stg_devices as (
    select * from {{ ref('stg_devices') }}
),

device_maintenance_summary as (
    select * from {{ ref('device_maintenance_summary') }}
),

final as (
    select h.device_id, d.site_id, d.region, d.device_type,
        cast(avg(h.min_battery) as numeric(38,6))    as avg_min_battery,
        cast(avg(h.error_rate_pct) as numeric(38,6)) as avg_error_rate,
        cast(sum(h.valid_readings) as bigint) as total_valid,
        cast(sum(h.reading_count) as bigint)  as total_readings,
        m.total_actions       as maintenance_actions,
        m.last_maintenance_ts,
        -- health 0-100: higher is better
        cast(round(least(100, greatest(0,
              100
              - avg(h.error_rate_pct)*5
              - case when avg(h.min_battery)<20 then 20 else 0 end
              + coalesce(m.total_actions,0)*2
            )), 2) as numeric(10,2)) as health_score
    from hourly_device_stats h
    join stg_devices d using (device_id)
    left join device_maintenance_summary m using (device_id)
    group by h.device_id, d.site_id, d.region, d.device_type,
             m.total_actions, m.last_maintenance_ts
)

select * from final
