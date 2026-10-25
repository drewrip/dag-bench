select device_id, device_type, active_hours, error_count, avg_gap_seconds,
    cast(round(error_count * 100.0 / nullif(reading_count, 0), 3) as numeric(10,3)) as error_rate_pct
from {{ ref('a_device_profile') }}
where firmware not like 'v1.%'
