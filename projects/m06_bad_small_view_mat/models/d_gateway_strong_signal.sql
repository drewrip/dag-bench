select device_id,
    max(site_id)                                                        as site_id,
    max(model)                                                          as model,
    count(*)                                                            as reading_count,
    min(ts)                                                             as first_ts,
    max(ts)                                                             as last_ts,
    max(rssi_dbm)                                                       as max_rssi_dbm,
    cast(round(cast(avg(humidity_pct) as numeric(18,5)), 3) as numeric(10,3)) as avg_humidity,
    cast(round(cast(avg(pressure_hpa) as numeric(18,5)), 3) as numeric(10,3)) as avg_pressure
from {{ ref('a_legacy_gateway_readings') }}
where rssi_dbm >= -35
group by device_id
