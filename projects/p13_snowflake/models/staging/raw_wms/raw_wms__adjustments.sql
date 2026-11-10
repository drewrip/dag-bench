{{
    config(
        materialized='view',
        tags=['staging', 'wms']
    )
}}

with source as (
    select * from {{ source('wms', 'ADJUSTMENTS') }}
),

deduped as (
    select
        adjustment_id,
        adjustment_number,
        warehouse_id,
        adjustment_type,
        status,
        total_lines,
        total_quantity,
        total_value,
        reason_code,
        notes,
        requested_by,
        requested_at,
        approved_by,
        created_at,
        updated_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by adjustment_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where adjustment_id is not null
    )
    where _rn = 1
)

select * from deduped
