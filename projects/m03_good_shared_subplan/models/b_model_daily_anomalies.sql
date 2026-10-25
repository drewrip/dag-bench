with daily as (
    select d.model, cast(r.ts as date) as reading_date,
        count(*)                       as reading_count,
        sum(r.pressure_hpa) / count(*) as mean_pressure,
        sum(r.humidity_pct) / count(*) as mean_humidity
    from {{ source('iot', 'devices') }} d, {{ source('iot', 'readings') }} r
    where d.device_id = r.device_id
      and d.is_active = true
      and r.error_flag = false
    group by d.model, cast(r.ts as date)
),
baselines as (
    select model, reading_date, reading_count, mean_pressure, mean_humidity,
        avg(mean_pressure)         over w as base_pressure,
        stddev_samp(mean_pressure) over w as sd_pressure,
        avg(mean_humidity)         over w as base_humidity,
        stddev_samp(mean_humidity) over w as sd_humidity
    from daily
    window w as (partition by model order by reading_date rows between 7 preceding and 1 preceding)
)
select model, reading_date, reading_count,
    cast(round(cast(mean_pressure as numeric(18,5)), 3) as numeric(10,3)) as mean_pressure,
    cast(round(cast(mean_humidity as numeric(18,5)), 3) as numeric(10,3)) as mean_humidity,
    coalesce(abs(mean_pressure - base_pressure) > 2 * sd_pressure, false) as pressure_anomaly,
    coalesce(abs(mean_humidity - base_humidity) > 2 * sd_humidity, false) as humidity_anomaly
from baselines
