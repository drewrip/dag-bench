select meter_type, tariff_class,
    count(distinct meter_id)    as meter_count,
    round(avg(daily_kwh),3)     as avg_daily_kwh,
    round(sum(daily_kwh),0)     as total_kwh,
    round(avg(avg_power_factor),4) as avg_pf
from {{ ref('meter_daily_stats') }}
group by meter_type, tariff_class
