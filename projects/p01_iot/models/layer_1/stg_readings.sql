select reading_id, device_id, ts, temperature_c, humidity_pct,
    pressure_hpa, battery_pct, rssi_dbm, error_flag,
    date_trunc('hour', ts) as ts_hour,
    date_trunc('day',  ts) as ts_day,
    extract('hour' from ts) as hour_of_day,
    not error_flag         as is_valid
from {{ source('iot','readings') }}
where humidity_pct between 0 and 100
  and temperature_c between -20 and 60
