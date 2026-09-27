select external_order_id as order_id, customer_id, order_date,
    lower(marketplace_name) as marketplace_name,
    split_part(external_order_id, '-', 2) as marketplace_order_code,
    case
        when partner_status in ('Shipped','Delivered') then 'completed'
        when partner_status = 'Awaiting Payment' then 'pending'
        when partner_status = 'Returned' then 'refunded'
        when partner_status = 'Cancelled' then 'cancelled'
        else 'pending'
    end as status,
    'marketplace' as channel,
    cast(coalesce(gross_amount,0) as numeric(10,2)) as gross_amount,
    cast(coalesce(commission_fee,0) as numeric(8,2)) as commission_fee,
    cast(coalesce(gross_amount,0) - coalesce(commission_fee,0) as numeric(11,2)) as net_revenue,
    cast(extract('year' from order_date) as bigint) as order_year,
    cast(extract('month' from order_date) as bigint) as order_month,
    cast(date_trunc('month',order_date) as date) as order_month_start,
    partner_status in ('Shipped','Delivered') as is_fulfilled,
    'partner_feed' as source_system
from {{ source('raw','marketplace_orders') }}
