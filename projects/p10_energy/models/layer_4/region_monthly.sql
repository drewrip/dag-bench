select region, cast(date_trunc('month',read_day) as date) as month,
    cast(sum(total_kwh) as numeric(18,4)) as monthly_kwh, avg(avg_voltage) as avg_voltage,
    cast(avg(active_meters) as numeric(38,6)) as avg_meters,
    cast(sum(total_kwh)/nullif(avg(active_meters),0) as numeric(38,6)) as kwh_per_meter
from {{ ref('substation_load') }}
group by region, cast(date_trunc('month',read_day) as date)
