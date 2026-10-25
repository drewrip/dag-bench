select device_id, ts_hour,
    max(device_label)                                              as device_label,
    count(*)                                                       as reading_count,
    min(battery_pct)                                               as min_battery,
    cast(round(cast(avg(temp_f) as numeric(18,5)), 2) as numeric(10,2)) as avg_temp_f
from {{ ref('a_readings_enriched') }}
where is_low_battery
  and ts >= timestamp '2023-06-01 00:00:00'
group by device_id, ts_hour
