select supplier_id, supplier_name, supplier_country, category, is_preferred,
    count(*) filter (where is_delivered)   as on_time_shipments,
    count(*) filter (where is_problematic) as problem_shipments,
    count(*)                               as total_shipments,
    round(avg(transit_days) filter (where is_delivered),1) as avg_transit_days,
    round(avg(cargo_value),2)              as avg_cargo_value,
    sum(cargo_value)                       as total_cargo_value,
    round(count(*) filter (where is_delivered)*100.0
          /nullif(count(*),0),2)           as delivery_rate_pct
from {{ ref('shipment_enriched') }}
group by supplier_id, supplier_name, supplier_country, category, is_preferred
