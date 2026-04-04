select supplier_id, supplier_name,
    count(*)                                  as total_pos,
    count(*) filter (where status='complete') as completed_pos,
    round(avg(fill_rate_pct),2)               as avg_fill_rate,
    round(avg(promised_lead_days),1)          as avg_promised_lead,
    sum(ordered_value)                        as total_ordered_value,
    sum(received_value)                       as total_received_value
from {{ ref('po_enriched') }}
group by supplier_id, supplier_name
