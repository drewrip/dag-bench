select supplier_id, supplier_name, count(*) as total_pos,
    count(*) filter (where status='complete') as completed,
    cast(round(avg(fill_rate_pct), 2) as numeric(10,2)) as avg_fill_rate,
    cast(round(avg(promised_lead), 1) as numeric(10,1)) as avg_lead,
    cast(sum(ordered_value) as numeric(18,2)) as total_ordered, cast(sum(received_value) as numeric(18,2)) as total_received
from {{ ref('po_enriched') }}
group by supplier_id, supplier_name
having count(*) >= 3
