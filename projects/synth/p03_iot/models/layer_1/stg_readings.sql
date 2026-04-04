select reading_id, device_id, ts, temperature_c, humidity_pct,
    pressure_hpa, battery_pct, rssi_dbm, error_flag,
    date_trunc('hour', ts) as ts_hour,
    date_trunc('day',  ts) as ts_day,
    extract('hour' from ts) as hour_of_day,
    not error_flag         as is_valid
from {{ source('iot','readings') }}
