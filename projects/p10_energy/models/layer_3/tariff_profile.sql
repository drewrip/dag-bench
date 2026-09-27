select region, meter_type, tariff_class,
    count(distinct meter_id) as meters,
    cast(round(avg(daily_kwh), 3) as numeric(18,3)) as avg_daily_kwh,
    cast(round(sum(daily_kwh), 0) as numeric(18,0)) as total_kwh,
    cast(round(cast(avg(avg_pf) as numeric(18,6)), 4) as numeric(10,4)) as avg_pf
from {{ ref('meter_daily') }}
group by region, meter_type, tariff_class
