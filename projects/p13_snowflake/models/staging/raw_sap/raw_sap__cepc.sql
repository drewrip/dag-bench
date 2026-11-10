{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'CEPC') }}
),

deduped as (
    select
        profit_center_id,
        profit_center_code,
        profit_center_name,
        is_active,
        created_at,
        _loaded_at,
        _source_system,
        _batch_id,
        _row_number,
        _row_hash
    from (
        select *,
            row_number() over (partition by profit_center_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where profit_center_id is not null
    )
    where _rn = 1
)

select * from deduped
