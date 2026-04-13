select sc.category,
    csr.avg_composite_score, csr.supplier_count,
    rsh.total_inventory, rsh.reorder_alerts,
    case
        when csr.avg_composite_score < 50 then 'HIGH_RISK'
        when rsh.reorder_alerts > 10      then 'INVENTORY_RISK'
        else 'NORMAL'
    end as risk_status
from {{ ref('category_supplier_rank') }} csr
join {{ ref('supplier_scorecard') }} sc using (category)
left join {{ ref('region_supply_health') }} rsh on true
group by sc.category, csr.avg_composite_score, csr.supplier_count,
         rsh.total_inventory, rsh.reorder_alerts
