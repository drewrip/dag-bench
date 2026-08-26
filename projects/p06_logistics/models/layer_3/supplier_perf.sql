select supplier_id, supplier_name, supplier_country, category, is_preferred,
    count(*) filter (where is_delivered) as on_time,
    count(*) filter (where is_problematic) as problems,
    count(*) as total,
    round(cast(avg(transit_days) filter (where is_delivered) as numeric(38,10)), 1) as avg_transit,
    sum(cargo_value) as total_cargo_value,
    round(cast(count(*) filter (where is_delivered)*100.0/nullif(count(*),0) as numeric(38,10)), 2) as delivery_rate
from {{ ref('shipment_enriched') }}
group by supplier_id, supplier_name, supplier_country, category, is_preferred
