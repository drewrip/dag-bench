with shipments as (

    select * from {{ ref('stg_shipments') }}

),

suppliers as (

    select * from {{ ref('stg_suppliers') }}

),

warehouses as (

    select * from {{ ref('stg_warehouses') }}

),

final as (

    select s.*, sup.supplier_name, sup.country as supplier_country,
        sup.reliability_score, sup.lead_time_days, sup.category, sup.is_preferred,
        w.wh_name, w.region as wh_region,
        s.transit_days>sup.lead_time_days*1.5 as is_late,
        lag(s.received_date) over (
            partition by s.supplier_id, s.sku
            order by s.shipped_date
        ) as prev_received_date
    from shipments s
    join suppliers sup using (supplier_id)
    join warehouses w using (wh_id)

)

select * from final
