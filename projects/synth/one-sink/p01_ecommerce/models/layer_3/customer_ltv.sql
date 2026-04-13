select c.customer_id, c.country, c.days_since_signup, c.is_active,
    coalesce(oc.total_orders, 0)         as total_orders,
    coalesce(oc.fulfilled_orders, 0)     as fulfilled_orders,
    coalesce(oc.total_shipping, 0)       as total_shipping,
    round(sum(lf.disc_revenue),2)        as total_revenue,
    round(sum(lf.line_gross_profit),2)   as total_gp,
    round(avg(lf.disc_revenue),2)        as avg_order_value,
    date_diff('day',oc.first_order_date,
              oc.last_order_date)        as customer_lifespan_days
from {{ ref('stg_customers') }} c
left join {{ ref('customer_order_counts') }} oc using (customer_id)
left join {{ ref('order_line_facts') }} lf   using (customer_id)
group by c.customer_id, c.country, c.days_since_signup, c.is_active,
         oc.total_orders, oc.fulfilled_orders, oc.total_shipping,
         oc.first_order_date, oc.last_order_date
