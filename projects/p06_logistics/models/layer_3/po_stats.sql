select supplier_id, supplier_name, count(*) as total_pos,
    count(*) filter (where status='complete') as completed,
    round(cast(avg(fill_rate_pct) as numeric(38,10)), 2) as avg_fill_rate,
    round(cast(avg(promised_lead) as numeric(38,10)), 1) as avg_lead,
    sum(ordered_value) as total_ordered, sum(received_value) as total_received
from {{ ref('po_enriched') }}
group by supplier_id, supplier_name
having count(*) >= 3
