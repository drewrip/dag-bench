{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'MARD') }}
),

deduped as (
    select
        inventory_id,
        variant_id,
        location_type,
        warehouse_id,
        store_id,
        location_id,
        quantity_on_hand,
        quantity_available,
        quantity_reserved,
        quantity_incoming,
        quantity_on_hold,
        unit_cost,
        inventory_status,
        last_counted_at,
        last_received_at,
        last_picked_at,
        created_at,
        updated_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by inventory_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where inventory_id is not null
    )
    where _rn = 1
)

select * from deduped
