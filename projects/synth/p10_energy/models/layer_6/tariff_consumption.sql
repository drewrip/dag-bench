select tariff_class, meter_type,
    round(avg(avg_daily_kwh),3) as avg_daily_kwh,
    round(sum(total_kwh),0)     as total_kwh,
    meter_count
from {{ ref('meter_type_profile') }}
group by tariff_class, meter_type, meter_count
