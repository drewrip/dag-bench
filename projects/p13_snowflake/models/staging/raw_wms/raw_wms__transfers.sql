{{
    config(
        materialized='view',
        tags=['staging', 'wms']
    )
}}

with source as (
    select * from {{ source('wms', 'TRANSFERS') }}
),

deduped as (
    select
        transfer_id,
        transfer_number,
        transfer_type,
        source_type,
        source_warehouse_id,
        source_store_id,
        dest_type,
        dest_warehouse_id,
        dest_store_id,
        status,
        priority,
        total_lines,
        total_quantity,
        total_value,
        created_by,
        created_at,
        updated_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by transfer_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where transfer_id is not null
    )
    where _rn = 1
)

select * from deduped
