select meter_type, tariff_class,
    count(distinct meter_id)    as meter_count,
    round(CAST(avg(daily_kwh) AS NUMERIC),3)     as avg_daily_kwh,
    round(CAST(sum(daily_kwh) AS NUMERIC),0)     as total_kwh,
    round(CAST(avg(avg_power_factor) AS NUMERIC),4) as avg_pf
from {{ ref('meter_daily_stats') }}
group by meter_type, tariff_class
