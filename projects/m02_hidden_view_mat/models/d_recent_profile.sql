select device_id, firmware, installed_date, reading_count, active_hours,
    avg_humidity, stddev_humidity, avg_pressure, stddev_pressure, max_gap_seconds
from {{ ref('a_device_profile') }}
where installed_date >= date '2020-03-01'
