select region, sum(total_on_hand) as inventory,
    sum(total_available) as available,
    sum(skus_needing_reorder) as reorder_alerts,
    round(avg(avg_capacity_util),2) as avg_utilization
from {{ ref('wh_inventory_status') }}
group by region
