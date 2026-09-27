select supplier_id, supplier_name, supplier_country, category, is_preferred,
    count(*) filter (where is_delivered) as on_time,
    count(*) filter (where is_problematic) as problems,
    count(*) as total,
    cast(round(avg(transit_days) filter (where is_delivered), 1) as numeric(10,1)) as avg_transit,
    cast(sum(cargo_value) as numeric(18,2)) as total_cargo_value,
    cast(round(count(*) filter (where is_delivered)*100.0/nullif(count(*),0), 2) as numeric(10,2)) as delivery_rate
from {{ ref('shipment_enriched') }}
group by supplier_id, supplier_name, supplier_country, category, is_preferred
