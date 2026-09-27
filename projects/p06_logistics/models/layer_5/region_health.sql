select region, cast(sum(total_on_hand) as bigint) as inventory,
    cast(sum(total_available) as bigint) as available,
    cast(sum(skus_needing_reorder) as bigint) as reorder_alerts,
    cast(round(avg(avg_capacity_util), 2) as numeric(10,2)) as avg_utilization
from {{ ref('wh_inventory_status') }}
group by region
