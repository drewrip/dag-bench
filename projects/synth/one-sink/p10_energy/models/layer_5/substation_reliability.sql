select s.sub_id, s.sub_name, s.region, s.capacity_mw,
    sum(sdl.total_kwh)                        as period_kwh,
    avg(sdl.avg_power_factor)                 as avg_pf,
    coalesce(oi.outage_count,0)               as outage_count,
    coalesce(oi.total_cml,0)                  as total_cml,
    round(100 - coalesce(oi.total_cml,0)
          /(nullif(sdl_days.days,0)*60*24) * 100, 4) as availability_pct
from {{ ref('stg_substations') }} s
left join {{ ref('substation_daily_load') }} sdl using (sub_id)
left join (
    select region, sum(outage_count) as outage_count,
           sum(total_cml) as total_cml
    from {{ ref('outage_impact') }}
    group by region
) oi on oi.region = s.region
left join (
    select sub_id, count(distinct read_day) as days
    from {{ ref('substation_daily_load') }}
    group by sub_id
) sdl_days using (sub_id)
group by s.sub_id, s.sub_name, s.region, s.capacity_mw,
         oi.outage_count, oi.total_cml, sdl_days.days
