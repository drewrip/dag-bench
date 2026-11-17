select meter_id, read_hour,
    cast({{ dagbench_agg_filter('sum', 'kwh', 'is_valid') }} as numeric(18,4)) as valid_kwh,
    {{ dagbench_agg_filter('count', '*', 'is_valid') }} as valid_readings,
    {{ dagbench_agg_filter('count', '*', 'is_estimated') }} as estimated,
    {{ dagbench_agg_filter('avg', 'voltage_v', 'is_valid') }} as avg_voltage,
    {{ dagbench_agg_filter('avg', 'power_factor', 'is_valid') }} as avg_pf
from (
    select *, max(read_hour) over () as max_read_hour
    from {{ ref('stg_readings') }}
) r
where read_hour >= max_read_hour - interval '30 days'
group by meter_id, read_hour
