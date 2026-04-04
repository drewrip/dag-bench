select sp.supplier_id, sp.supplier_name, sp.category,
    sp.delivery_rate_pct, sp.avg_transit_days, sp.total_cargo_value,
    pf.avg_fill_rate, pf.avg_promised_lead,
    round(
        sp.delivery_rate_pct * 0.4
        + pf.avg_fill_rate * 0.3
        + (100 - least(sp.avg_transit_days, 100)) * 0.3
    ,2) as composite_score,
    rank() over (order by
        sp.delivery_rate_pct*0.4 + pf.avg_fill_rate*0.3
        + (100-least(sp.avg_transit_days,100))*0.3
        desc)                                          as overall_rank
from {{ ref('supplier_performance') }} sp
left join {{ ref('po_fulfillment_stats') }} pf using (supplier_id)
