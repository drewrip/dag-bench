select cs.customer_id, cs.country, cs.value_segment, cs.frequency_segment,
    cs.total_revenue, cs.total_gp, cs.avg_order_value, cs.total_orders,
    mg.ytd_revenue is not null as has_ytd_data
from {{ ref('customer_segments') }} cs
cross join (
    select max(order_year) as latest_year from {{ ref('monthly_growth') }}
) ly
left join (
    select 1 as _join_key, max(ytd_revenue) as ytd_revenue
    from {{ ref('monthly_growth') }}
) mg on 1 = 1
where cs.value_segment in ('VIP','High')
