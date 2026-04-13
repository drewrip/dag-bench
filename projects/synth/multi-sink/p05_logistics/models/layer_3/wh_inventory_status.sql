select wh_id, wh_name, region,
    count(distinct sku) as sku_count, sum(qty_on_hand) as total_on_hand,
    sum(qty_available) as total_available,
    count(*) filter (where needs_reorder) as skus_needing_reorder,
    round(avg(capacity_pct),2) as avg_capacity_util
from {{ ref('inventory_enriched') }}
group by wh_id, wh_name, region
