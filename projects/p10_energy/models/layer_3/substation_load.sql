select sub_id, region, read_day, count(distinct meter_id) as active_meters,
    cast(sum(daily_kwh) as numeric(18,4)) as total_kwh, cast(max(daily_kwh) as numeric(18,4)) as peak_meter_kwh,
    avg(avg_voltage) as avg_voltage, avg(avg_pf) as avg_pf
from {{ ref('meter_daily') }}
group by sub_id, region, read_day
