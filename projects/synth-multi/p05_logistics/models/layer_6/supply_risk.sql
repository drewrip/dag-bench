select sc.category, cs.avg_score, cs.suppliers,
    rh.inventory, rh.reorder_alerts,
    case when cs.avg_score<50 then 'SUPPLIER_RISK'
         when rh.reorder_alerts>10 then 'INVENTORY_RISK' else 'NORMAL' end as risk_status
from {{ ref('category_scores') }} cs
join {{ ref('supplier_scorecard') }} sc using (category)
left join {{ ref('region_health') }} rh on true
group by sc.category, cs.avg_score, cs.suppliers, rh.inventory, rh.reorder_alerts
