select ts.category, ts.supplier_name, ts.composite_score,
    ts.delivery_rate_pct, ts.avg_fill_rate,
    rm.risk_status, rm.reorder_alerts,
    rm.total_inventory
from {{ ref('top_suppliers_per_category') }} ts
join {{ ref('supply_risk_matrix') }} rm using (category)
where ts.rank_in_cat <= 3
