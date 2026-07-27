with stg_outages as (

    select * from {{ ref('stg_outages') }}

),

stg_substations as (

    select * from {{ ref('stg_substations') }}

),

final as (

    select o.*, s.region, s.capacity_mw,
        o.cml/60.0 as customer_hours_lost, o.duration_min/60.0 as duration_hrs
    from stg_outages o
    join stg_substations s using (sub_id)

)

select * from final
