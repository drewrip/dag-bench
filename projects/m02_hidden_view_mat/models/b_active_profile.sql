select device_id, model, reading_count, active_hours,
    avg_temp, stddev_temp, cast(max_temp - min_temp as numeric(10,3)) as temp_range,
    max_gap_seconds
from {{ ref('a_device_profile') }}
where is_active
