{{
    config(
        materialized='view',
        tags=['staging', 'legacy']
    )
}}

with source as (
    select * from {{ source('legacy', 'STSCOD') }}
),

deduped as (
    select
        status_code_id,
        entity_type,
        status_code,
        status_name,
        status_description,
        display_order,
        is_terminal,
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
            row_number() over (partition by status_code_id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where status_code_id is not null
    )
    where _rn = 1
)

select * from deduped
