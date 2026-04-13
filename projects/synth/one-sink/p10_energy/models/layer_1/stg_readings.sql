select reading_id, meter_id, read_ts, kwh, voltage_v, power_factor,
    is_estimated,
    date_trunc('hour', read_ts) as read_hour,
    date_trunc('day',  read_ts) as read_day,
    extract('hour' from read_ts) as hour_of_day,
    extract('month' from read_ts) as read_month,
    kwh > 0 and not is_estimated as is_valid_actual
from {{ source('grid','consumption_readings') }}
