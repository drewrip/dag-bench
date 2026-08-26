with internal_channels as (
    select order_year, order_month, channel,
        count(distinct order_id) as orders,
        count(distinct customer_id) as unique_customers,
        round(sum(disc_revenue), 2) as revenue,
        round(sum(line_gross_profit), 2) as gross_profit
    from {{ ref('order_line_facts') }}
    where is_fulfilled
    group by order_year, order_month, channel
),

-- Marketplace orders arrive via a partner feed at order-header grain only (no line-item
-- detail is available), so they can't join into order_line_facts. Aggregate separately at
-- the same (order_year, order_month, channel) grain and union the two channel families.
-- gross_profit = revenue here because COGS isn't tracked for consignment/dropship
-- inventory sold through marketplace partners.
marketplace_channel as (
    select order_year, order_month, channel,
        count(distinct order_id) as orders,
        count(distinct customer_id) as unique_customers,
        round(sum(net_revenue), 2) as revenue,
        round(sum(net_revenue), 2) as gross_profit
    from {{ ref('stg_marketplace_orders') }}
    where is_fulfilled
    group by order_year, order_month, channel
)

select * from internal_channels
union all
select * from marketplace_channel
