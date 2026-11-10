{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'T006') }}
),

deduped as (
    select
        uom_code,
        uom_name,
        uom_type,
        base_uom_code,
        conversion_factor,
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
            row_number() over (partition by _batch_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where _batch_id is not null
    )
    where _rn = 1
)

select * from deduped
