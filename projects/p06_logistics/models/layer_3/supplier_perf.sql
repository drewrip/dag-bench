select supplier_id, supplier_name, supplier_country, category, is_preferred,
    {{ dagbench_agg_filter('count', '*', 'is_delivered') }} as on_time,
    {{ dagbench_agg_filter('count', '*', 'is_problematic') }} as problems,
    count(*) as total,
    cast(round({{ dagbench_agg_filter('avg', 'transit_days', 'is_delivered') }}, 1) as numeric(10,1)) as avg_transit,
    cast(sum(cargo_value) as numeric(18,2)) as total_cargo_value,
    cast(round({{ dagbench_agg_filter('count', '*', 'is_delivered') }}*100.0/nullif(count(*),0), 2) as numeric(10,2)) as delivery_rate
from {{ ref('shipment_enriched') }}
group by supplier_id, supplier_name, supplier_country, category, is_preferred
