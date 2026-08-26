select region, sum(total_on_hand) as inventory,
    sum(total_available) as available,
    sum(skus_needing_reorder) as reorder_alerts,
    round(cast(avg(avg_capacity_util) as numeric(38,10)), 2) as avg_utilization
from {{ ref('wh_inventory_status') }}
group by region
