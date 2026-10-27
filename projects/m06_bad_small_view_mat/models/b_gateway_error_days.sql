select device_id, ts_day,
    max(firmware)                                                  as firmware,
    count(*)                                                       as error_count,
    cast(round(cast(avg(rssi_dbm) as numeric(18,5)), 2) as numeric(10,2)) as avg_rssi_dbm,
    min(battery_pct)                                               as min_battery
from {{ ref('a_legacy_gateway_readings') }}
where error_flag
  and ts_day between date '2023-03-01' and date '2023-03-07'
group by device_id, ts_day
