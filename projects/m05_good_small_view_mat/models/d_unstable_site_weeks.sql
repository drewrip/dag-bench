select site_id, ts_week, reading_count, active_devices, active_hours, error_count, min_battery,
    avg_temp, median_temp, p95_temp, stddev_temp, stddev_humidity, stddev_pressure
from {{ ref('a_site_weekly_profile') }}
where active_devices >= 3
  and stddev_pressure > 20
