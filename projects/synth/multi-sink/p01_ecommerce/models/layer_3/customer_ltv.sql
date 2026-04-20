select c.customer_id, c.country, c.days_since_signup, c.is_active,
    coalesce(oc.total_orders,0) as total_orders,
    coalesce(oc.fulfilled_orders,0) as fulfilled_orders,
    round(CAST(sum(lf.disc_revenue) AS NUMERIC),2) as total_revenue,
    round(CAST(sum(lf.line_gross_profit) AS NUMERIC),2) as total_gp,
    round(CAST(avg(lf.disc_revenue) AS NUMERIC),2) as avg_order_value,
    {{ datediff("oc.first_order_date", "oc.last_order_date", "day") }} as lifespan_days
from {{ ref('stg_customers') }} c
left join {{ ref('customer_order_counts') }} oc using (customer_id)
left join {{ ref('order_line_facts') }} lf using (customer_id)
group by c.customer_id,c.country,c.days_since_signup,c.is_active,
         oc.total_orders,oc.fulfilled_orders,oc.first_order_date,oc.last_order_date
