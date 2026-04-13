select meter_type, tariff_class,
    count(distinct meter_id) as meters,
    round(avg(daily_kwh),3) as avg_daily_kwh,
    round(sum(daily_kwh),0) as total_kwh,
    round(avg(avg_pf),4) as avg_pf
from {{ ref('meter_daily') }}
group by meter_type, tariff_class
