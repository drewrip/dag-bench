select s.*, sup.supplier_name, sup.country as supplier_country,
    sup.reliability_score, sup.lead_time_days, sup.category, sup.is_preferred,
    w.wh_name, w.region as wh_region,
    s.transit_days>sup.lead_time_days*1.5 as is_late
from {{ ref('stg_shipments') }} s
join {{ ref('stg_suppliers') }} sup using (supplier_id)
join {{ ref('stg_warehouses') }} w using (wh_id)
