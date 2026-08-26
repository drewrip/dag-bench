select region, meter_type, tariff_class,
    count(distinct meter_id) as meters,
    round(avg(daily_kwh), 3) as avg_daily_kwh,
    round(sum(daily_kwh), 0) as total_kwh,
    round(cast(avg(avg_pf) as numeric(18,6)), 4) as avg_pf
from {{ ref('meter_daily') }}
group by region, meter_type, tariff_class
