{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'T001L') }}
),

deduped as (
    select
        location_id,
        warehouse_id,
        zone_id,
        location_code,
        location_barcode,
        aisle,
        rack,
        shelf,
        location_type,
        is_pickable,
        is_receivable,
        max_weight,
        pick_sequence,
        is_active,
        created_at,
        updated_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by location_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where location_id is not null
    )
    where _rn = 1
)

select * from deduped
