select tariff_class, meter_type, meters, avg_daily_kwh, total_kwh, avg_pf
from {{ ref('tariff_profile') }}
