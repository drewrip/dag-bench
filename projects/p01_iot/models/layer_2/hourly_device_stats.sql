with hourly as (
    select device_id, ts_hour,
        count(*)                         as reading_count,
        {{ dagbench_agg_filter('count', '*', 'is_valid') }} as valid_readings,
        cast(round(cast({{ dagbench_agg_filter('avg', 'temperature_c', 'is_valid') }} as numeric(18,5)), 3) as numeric(10,3)) as avg_temp,
        cast(round(cast({{ dagbench_agg_filter('min', 'temperature_c', 'is_valid') }} as numeric(18,5)), 3) as numeric(10,3)) as min_temp,
        cast(round(cast({{ dagbench_agg_filter('max', 'temperature_c', 'is_valid') }} as numeric(18,5)), 3) as numeric(10,3)) as max_temp,
        cast(round(cast({{ dagbench_agg_filter('avg', 'humidity_pct', 'is_valid', gap='  ') }} as numeric(18,5)), 3) as numeric(10,3)) as avg_humidity,
        cast(round(cast({{ dagbench_agg_filter('avg', 'pressure_hpa', 'is_valid', gap='  ') }} as numeric(18,5)), 3) as numeric(10,3)) as avg_pressure,
        min(battery_pct)                   as min_battery,
        {{ dagbench_agg_filter('count', '*', 'error_flag') }} as error_count,
        cast(round({{ dagbench_agg_filter('count', '*', 'error_flag') }}*100.0
              /nullif(count(*),0), 2) as numeric(10,2)) as error_rate_pct
    from {{ ref('stg_readings') }}
    group by device_id, ts_hour
)
select *,
    cast(lag(avg_temp) over (partition by device_id order by ts_hour) as numeric(10,3)) as prev_hour_avg_temp
from hourly
