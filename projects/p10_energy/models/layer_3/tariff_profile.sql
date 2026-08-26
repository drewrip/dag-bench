select region, meter_type, tariff_class,
    count(distinct meter_id) as meters,
    round(cast(avg(daily_kwh) as numeric(38,10)), 3) as avg_daily_kwh,
    round(cast(sum(daily_kwh) as numeric(38,10)), 0) as total_kwh,
    round(cast(avg(avg_pf) as numeric(38,10)), 4) as avg_pf
from {{ ref('meter_daily') }}
group by region, meter_type, tariff_class
