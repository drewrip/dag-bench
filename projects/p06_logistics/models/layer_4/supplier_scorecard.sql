select sp.supplier_id, sp.supplier_name, sp.category, sp.is_preferred,
    sp.delivery_rate, sp.avg_transit, sp.total_cargo_value,
    ps.avg_fill_rate, ps.avg_lead,
    round(cast(sp.delivery_rate*0.4+ps.avg_fill_rate*0.3+(100-least(sp.avg_transit,100))*0.3 as numeric(38,10)), 2) as score,
    rank() over (order by sp.delivery_rate*0.4+ps.avg_fill_rate*0.3
                          +(100-least(sp.avg_transit,100))*0.3 desc nulls last) as overall_rank
from {{ ref('supplier_perf') }} sp
left join {{ ref('po_stats') }} ps using (supplier_id)
