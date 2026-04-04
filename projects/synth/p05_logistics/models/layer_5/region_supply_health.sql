select region as region,
    sum(total_on_hand)      as total_inventory,
    sum(total_available)    as available_inventory,
    sum(skus_needing_reorder) as reorder_alerts,
    round(avg(avg_capacity_util),2) as avg_warehouse_utilization
from {{ ref('warehouse_inventory_status') }}
group by region
