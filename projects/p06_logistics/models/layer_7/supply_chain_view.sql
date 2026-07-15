select ts.category, ts.supplier_name, ts.score, ts.delivery_rate,
    ts.avg_fill_rate, sr.risk_status, sr.reorder_alerts, sr.inventory, ts.rank_in_cat
from {{ ref('top_suppliers') }} ts
join {{ ref('supply_risk') }} sr using (category)
where ts.rank_in_cat<=3
