select region, date_trunc('month',read_day) as month,
    sum(total_kwh) as monthly_kwh, avg(avg_voltage) as avg_voltage,
    avg(active_meters) as avg_meters,
    sum(total_kwh)/nullif(avg(active_meters),0) as kwh_per_meter
from {{ ref('substation_load') }}
group by region, date_trunc('month',read_day)
